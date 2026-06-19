"use strict";

const AthenaService = require("../../services/athena.service");

const OPERATOR_MAP = {
  EQ: "=", eq: "=",
  GT: ">", gt: ">",
  LT: "<", lt: "<",
  GTE: ">=", gte: ">=",
  LTE: "<=", lte: "<=",
  NE: "!=", ne: "!=",
};

// Numeric type IDs for attribute mapping types — avoids case-sensitive label match in Athena
const ATTR_TYPE_IDS = { LOB: 6, Branch: 3 };

// Fields filtered post-fetch using formScoreRows (not via Athena subquery)
const SCORE_FIELDS = new Set(["avg_percentage_score", "form_percentage", "form_score"]);

function sqlNum(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) throw new Error(`Non-numeric value: ${v}`);
  return String(n);
}

function sqlStr(v) {
  return `'${String(v).replace(/'/g, "''")}'`;
}

function dateOnly(v) {
  if (v instanceof Date && !isNaN(v)) {
    return v.toISOString().slice(0, 10);
  }
  const s = String(v || "").trim();
  const m = s.match(/^(\d{4}-\d{2}-\d{2})/);
  if (!m) throw new Error(`Invalid date: ${v}`);
  return m[1];
}

class TrendExportService {
  constructor(athena) {
    this.athena = athena || new AthenaService();
  }

  #idListClause(ids) {
    if (!ids || !ids.length) return "(NULL)";
    return `(${ids.map((v) => parseInt(v, 10)).filter((v) => !isNaN(v)).join(",")})`;
  }

  async buildWhere(rules, combinator = "and") {
    const where = [];
    const scoreRules = []; // applied post-fetch using formScoreRows
    if (!Array.isArray(rules) || !rules.length) return { mainWhere: "", scoreRules: [] };

    const comb = String(combinator).toUpperCase() === "OR" ? "OR" : "AND";

    // Operator inverse for decline fields (magnitude > N means pct_change < -N)
    const DECLINE_OP = { "=": "=", "!=": "!=", ">": "<", ">=": "<=", "<": ">", "<=": ">=" };

    const afcsSubquery = (col, op, val) =>
      `kvl.kbase_version_usage_log_id IN (
         SELECT afcs.kbase_version_usage_log_id
         FROM iceberg_db.assessment_form_consolidated_score afcs
         WHERE afcs.${col} ${op} ${val}
       )`;

    // Uses numeric type_id instead of label string to avoid Athena case-sensitive match issues.
    const attrSubquery = (typeLabel, op, vals) => {
      const clause = vals.length > 1
        ? `IN (${vals.map(sqlNum).join(",")})`
        : `${op} ${sqlNum(vals[0])}`;
      const typeId = ATTR_TYPE_IDS[typeLabel];
      if (typeId != null) {
        return `kvl.kbase_version_usage_log_id IN (
          SELECT kvt_f.kbase_version_usage_log_id
          FROM iceberg_db.kbase_version_usage_log_trx_attribute_mapping_con kvt_f
          JOIN iceberg_db.trx_attribute_mappings_inst tam_f
            ON tam_f.trx_attribute_mappings_inst_id = kvt_f.trx_attribute_mappings_inst_id
          WHERE tam_f.trx_attribute_mapping_type_id = ${typeId}
            AND tam_f.trx_attribute_mappings_inst_id ${clause}
        )`;
      }
      return `kvl.kbase_version_usage_log_id IN (
        SELECT kvt_f.kbase_version_usage_log_id
        FROM iceberg_db.kbase_version_usage_log_trx_attribute_mapping_con kvt_f
        JOIN iceberg_db.trx_attribute_mappings_inst tam_f
          ON tam_f.trx_attribute_mappings_inst_id = kvt_f.trx_attribute_mappings_inst_id
        JOIN iceberg_db.trx_attribute_mapping_types tmt_f
          ON tmt_f.trx_attribute_mapping_type_id = tam_f.trx_attribute_mapping_type_id
        WHERE tmt_f.trx_attribute_mapping_type_label = ${sqlStr(typeLabel)}
          AND tam_f.trx_attribute_mappings_inst_id ${clause}
      )`;
    };

    // Rubric/attribute filters via subquery so they select kvls without
    // narrowing the CSV's rubric columns.
    const rubricSubquery = (col, op, vals) => {
      const clause = vals.length > 1
        ? `IN (${vals.map(sqlNum).join(",")})`
        : `${op} ${sqlNum(vals[0])}`;
      return `kvl.kbase_version_usage_log_id IN (
        SELECT arsm_f.kbase_version_usage_log_id
        FROM iceberg_db.assessment_rubrics_scoring_master arsm_f
        JOIN iceberg_db.assessment_rubrics_attribute_score aras_f
          ON aras_f.rubrics_parameter_id = arsm_f.assessment_rubrics_attribute_score_id
        WHERE aras_f.${col} ${clause}
      )`;
    };

    // sum_score = aggregate rubric percentage per kvl (matches Benchmark HAVING).
    const sumScoreSubquery = (op, val) =>
      `kvl.kbase_version_usage_log_id IN (
        SELECT arsm_f.kbase_version_usage_log_id
        FROM iceberg_db.assessment_rubrics_scoring_master arsm_f
        GROUP BY arsm_f.kbase_version_usage_log_id
        HAVING CASE WHEN SUM(arsm_f.max_score) > 0
                    THEN ROUND(SUM(arsm_f.score) / SUM(arsm_f.max_score) * 100, 2)
                    ELSE NULL END ${op} ${val}
      )`;

    for (const r of rules) {
      const { field, operator, value } = r;
      const op = OPERATOR_MAP[String(operator).toUpperCase()] || OPERATOR_MAP[operator];
      if (!op || value === undefined || value === null || value === "") continue;
      if (Array.isArray(value) && value.length === 0) continue;

      const vals = Array.isArray(value) ? value : [value];

      if (field === "agent_id") {
        where.push(
          vals.length > 1
            ? `kvl.agent_master_id IN (${vals.map(sqlNum).join(",")})`
            : `kvl.agent_master_id ${op} ${sqlNum(vals[0])}`,
        );
      } else if (field === "rubrics_parameter_id") {
        where.push(rubricSubquery("rubrics_parameter_id", op, vals));
      } else if (field === "form_schema_trx_attribute_inst_id") {
        where.push(rubricSubquery("form_schema_trx_attribute_inst_id", op, vals));
      } else if (field === "sum_score") {
        where.push(sumScoreSubquery(op, sqlNum(vals[0])));
      } else if (SCORE_FIELDS.has(field)) {
        if (comb === "AND") {
          // Post-fetch via formScoreRows — avoids AFCS sync gaps returning 0 rows
          scoreRules.push({ op, val: Number(vals[0]) });
        } else {
          // OR combinator: keep in mainWhere so Athena handles OR semantics correctly
          where.push(afcsSubquery("consolidated_score", op, sqlNum(vals[0])));
        }
      } else if (field === "formId") {
        // Support single value (=) and arrays (IN) — arrays come from form_schema resolution.
        const formClause = vals.length > 1
          ? `IN (${vals.map(sqlNum).join(",")})`
          : `${op} ${sqlNum(vals[0])}`;
        where.push(afcsSubquery("form_master_id", "", formClause));
      } else if (field === "LOB" || field === "Branch") {
        where.push(attrSubquery(field, op, vals));
      } else if (field === "pct_change_gain") {
        where.push(afcsSubquery("baseline_pct_change", op, sqlNum(vals[0])));
      } else if (field === "pct_change_decline") {
        where.push(afcsSubquery("baseline_pct_change", DECLINE_OP[op] || op, `-${sqlNum(vals[0])}`));
      } else if (field === "pct_cts_change_gain") {
        where.push(afcsSubquery("continuous_pct_change", op, sqlNum(vals[0])));
      } else if (field === "pct_cts_change_decline") {
        where.push(afcsSubquery("continuous_pct_change", DECLINE_OP[op] || op, `-${sqlNum(vals[0])}`));
      }
    }

    return {
      mainWhere: where.length ? `AND (${where.join(` ${comb} `)})` : "",
      scoreRules,
    };
  }

  // Pure-Athena universe — drives off the same iceberg_db.kbase_version_usage_log
  // table (and its cai/ctt/tm join chain) the benchmark service uses, so trend
  // and benchmark agree on "what's in scope". Date filter is on
  // kvl.audit_create_time because Iceberg's call_assessment_inst_master has no
  // timestamp column (see benchmark.handler.js for the parity story).
  // Returns the same shape as the MySQL fetchAssessments — drop-in replacement.
  async fetchAssessmentUniverseFromAthena(iid, startDate, endDate) {
    const start = dateOnly(startDate);
    const end = dateOnly(endDate);
    // Athena picks a wildly different plan when we add `cai.id` to the
    // SELECT of a kvl-driven universe query — observed 666ms vs 361s for
    // the SAME joins/filter, just adding one column. So we split into two
    // fast queries instead of one slow one:
    //   1) kvl-only universe (matches BenchmarkExportService — known fast)
    //   2) tiny assessment_id lookup over the resulting kvls
    const t1 = Date.now();
    const universeRows = await this.athena.executeQuery(`
      SELECT DISTINCT kvl.kbase_version_usage_log_id AS kvl_id
      FROM iceberg_db.kbase_version_usage_log kvl
      JOIN iceberg_db.call_assessment_inst_master cai
        ON cai.kbase_version_usage_log_id = kvl.kbase_version_usage_log_id
      JOIN iceberg_db.con_transaction_telephony ctt
        ON ctt.con_transaction_telephony_id = cai.con_transaction_telephony_id
      JOIN iceberg_db.transaction_masters tm
        ON tm.transaction_master_id = ctt.transaction_master_id
      WHERE tm.institution_id = ${sqlNum(iid)}
        AND kvl.audit_create_time >= TIMESTAMP '${start} 00:00:00'
        AND kvl.audit_create_time <= TIMESTAMP '${end} 23:59:59'
    `);
    const kvlIds = universeRows
      .map((r) => Number(r.kvl_id))
      .filter((v) => Number.isFinite(v));
    console.log(
      `[TrendExport] universe-kvls=${kvlIds.length} in ${Date.now() - t1}ms`,
    );

    if (!kvlIds.length) {
      return { assessmentIds: [], kvlIds: [], assessmentToKvlId: new Map() };
    }

    const t2 = Date.now();
    const idClause = this.#idListClause(kvlIds);
    const aidRows = await this.athena.executeQuery(`
      SELECT
        cai.call_assessment_inst_master_id  AS assessment_id,
        cai.kbase_version_usage_log_id      AS kvl_id
      FROM iceberg_db.call_assessment_inst_master cai
      WHERE cai.kbase_version_usage_log_id IN ${idClause}
    `);
    console.log(
      `[TrendExport] universe-aids=${aidRows.length} in ${Date.now() - t2}ms`,
    );

    const assessmentToKvlId = new Map();
    const assessmentIds = [];
    for (const r of aidRows) {
      const aid = Number(r.assessment_id);
      const kvl = Number(r.kvl_id);
      if (!Number.isFinite(aid) || !Number.isFinite(kvl)) continue;
      if (!assessmentToKvlId.has(aid)) {
        assessmentToKvlId.set(aid, kvl);
        assessmentIds.push(aid);
      }
    }
    return { assessmentIds, kvlIds, assessmentToKvlId };
  }

  // Returns: assessmentIds (ordered), kvlIds (unique, for Athena), assessmentToKvlId (Map aid → primary kvlId)
  // queryFn: async (sql, params) => rows[]  — caller provides the DB adapter (MySQL, PG, etc.)
  async fetchAssessments(iid, startDate, endDate, queryFn) {
    const start = dateOnly(startDate);
    const end = dateOnly(endDate);

    const assessmentRows = await queryFn(
      `SELECT csim.CallAssessmentInstMasterId,
              csim.KBaseVersionUsageLogId AS LegacyKvlId
       FROM CallAssessmentInstMaster csim
       JOIN ConTransactionTelephony ctt ON csim.ConTransactionTelephonyId = ctt.ConTransactionTelephonyId
       JOIN TransactionMasters tm ON ctt.TransactionMasterId = tm.TransactionMasterId
       WHERE csim.AuditCreateTime BETWEEN ? AND ?
         AND tm.InstitutionId = ?`,
      [`${start} 00:00:00`, `${end} 23:59:59`, Number(iid)],
    );

    if (!assessmentRows.length) {
      return { assessmentIds: [], kvlIds: [], assessmentToKvlId: new Map() };
    }

    const assessmentIds = assessmentRows.map((r) => r.CallAssessmentInstMasterId);

    const junctionRows = await queryFn(
      `SELECT CallAssessmentInstMasterId, KbaseVersionUsageLogId
       FROM CallAssessmentKbaseLog
       WHERE CallAssessmentInstMasterId IN (?)`,
      [assessmentIds],
    );

    // Build assessment → [kvlId, ...] from junction table
    const assessmentToKvlIds = new Map();
    for (const r of junctionRows) {
      const aid = r.CallAssessmentInstMasterId;
      if (!assessmentToKvlIds.has(aid)) assessmentToKvlIds.set(aid, []);
      assessmentToKvlIds.get(aid).push(Number(r.KbaseVersionUsageLogId));
    }

    // Merge legacy csim.KBaseVersionUsageLogId — same logic as Assessmenthandler.js
    for (const r of assessmentRows) {
      const aid = r.CallAssessmentInstMasterId;
      const legacyId = r.LegacyKvlId ? Number(r.LegacyKvlId) : null;
      if (legacyId && legacyId > 0) {
        const existing = assessmentToKvlIds.get(aid);
        if (!existing) {
          assessmentToKvlIds.set(aid, [legacyId]);
        } else if (!existing.includes(legacyId)) {
          existing.push(legacyId);
        }
      }
    }

    // Primary KVL ID per assessment (first in list, same as resolveScoringLogId fallback)
    const assessmentToKvlId = new Map();
    for (const [aid, ids] of assessmentToKvlIds) {
      assessmentToKvlId.set(aid, ids[0]);
    }

    const kvlIds = [
      ...new Set(
        [].concat(...assessmentToKvlIds.values()).filter((v) => v > 0),
      ),
    ];

    return { assessmentIds, kvlIds, assessmentToKvlId };
  }

  // Mirrors BenchmarkExportService.fetchMain — drives from kvl with a
  // GROUP BY kvl_id so we get exactly one row per kvl. Rubric scores and
  // attributes used to be joined in here, producing a kvl × rubric ×
  // attribute cartesian (~15× row blow-up, 74k rows for 5k kvls).
  // They're now in dedicated parallel queries: fetchRubricScores +
  // fetchAttributeMappings.
  async fetchMain({ iid, validKvlIds, mainWhere }) {
    const idClause = this.#idListClause(validKvlIds);
    // Same join chain BenchmarkExportService uses: kvl → cai → ctt → tm → um,
    // and ctt → acsa → agent_master. Resolving the agent through acsa.agent_id
    // (instead of kvl.agent_master_id) matches the UI tile's
    // "Agent Attributed" definition (~2953) and benchmark's Agent_Name count.
    // The Agent_Id column is exposed so filtering "Agent_Id not blank" hits
    // exactly the 2953 figure even when agent_master has unsynced rows.
    const sql = `
      SELECT
        kvl.kbase_version_usage_log_id                                              AS KBaseVersionUsageLogId,
        MAX(tm.transaction_master_id)                                               AS TransactionMasterId,
        MAX(um.user_name)                                                           AS UserName,
        MAX(CONCAT('''', um.user_mobile_number))                                    AS Phone,
        date_format(MAX(CAST(kvl.audit_create_time AS TIMESTAMP)),
                    '%d %b %Y %H:%i:%s')                                            AS Created_Date,
        MAX(acsa.agent_id)                                                          AS Agent_Id,
        MAX(COALESCE(am.agent_name, ''))                                            AS Agent_Name
      FROM iceberg_db.kbase_version_usage_log kvl
      JOIN iceberg_db.call_assessment_inst_master cai
        ON cai.kbase_version_usage_log_id = kvl.kbase_version_usage_log_id
      JOIN iceberg_db.con_transaction_telephony ctt
        ON ctt.con_transaction_telephony_id = cai.con_transaction_telephony_id
      JOIN iceberg_db.transaction_masters tm
        ON tm.transaction_master_id = ctt.transaction_master_id
      LEFT JOIN iceberg_db.user_master um
        ON um.user_master_id = tm.user_master_id
      LEFT JOIN iceberg_db.ask_conv_session_agent_inst_con acsa
        ON acsa.ask_conv_session_agent_inst_con_id = ctt.ask_conv_session_agent_inst_con_id
      LEFT JOIN iceberg_db.agent_master am
        ON am.agent_master_id = acsa.agent_id
      WHERE tm.institution_id = ${sqlNum(iid)}
        AND kvl.kbase_version_usage_log_id IN ${idClause}
        ${mainWhere || ""}
      GROUP BY kvl.kbase_version_usage_log_id
    `;
    return this.athena.executeQuery(sql);
  }

  // Rubric-level scores per kvl — split out of fetchMain so that query can
  // return one row per kvl instead of one row per (kvl × rubric).
  // transformToCsv merges these into the per-kvl record via scores{}.
  async fetchRubricScores(kvlIds) {
    if (!kvlIds.length) return [];
    const sql = `
      SELECT
        arsm.kbase_version_usage_log_id                                             AS KBaseVersionUsageLogId,
        aras.rubrics_parameter_label                                                AS RubricsParameterLabel,
        arsm.score                                                                  AS Score,
        arsm.max_score                                                              AS MaxScore,
        ROUND(
          CAST(arsm.score AS DOUBLE) / NULLIF(CAST(arsm.max_score AS DOUBLE), 0) * 100,
          2
        )                                                                           AS ScorePercentage
      FROM iceberg_db.assessment_rubrics_scoring_master arsm
      LEFT JOIN iceberg_db.assessment_rubrics_attribute_score aras
        ON aras.rubrics_parameter_id = arsm.assessment_rubrics_attribute_score_id
      WHERE arsm.kbase_version_usage_log_id IN ${this.#idListClause(kvlIds)}
    `;
    return this.athena.executeQuery(sql);
  }

  // Returns one Athena row per (kvl × attribute mapping) — replaces the
  // cartesian fan-out that fetchMain used to do via its kvt/tam/tmt joins.
  // Merged into per-kvl record in transformToCsv via the attributes map.
  async fetchAttributeMappings(kvlIds) {
    if (!kvlIds.length) return [];
    const sql = `
      SELECT
        kvt.kbase_version_usage_log_id                  AS KBaseVersionUsageLogId,
        tmt.trx_attribute_mapping_type_label            AS TrxAttributeMappingTypeLabel,
        tam.trx_attribute_mappings_inst_label           AS TrxAttributeMappingsInstLabel
      FROM iceberg_db.kbase_version_usage_log_trx_attribute_mapping_con kvt
      INNER JOIN iceberg_db.trx_attribute_mappings_inst tam
        ON tam.trx_attribute_mappings_inst_id = kvt.trx_attribute_mappings_inst_id
      INNER JOIN iceberg_db.trx_attribute_mapping_types tmt
        ON tmt.trx_attribute_mapping_type_id = tam.trx_attribute_mapping_type_id
      WHERE kvt.kbase_version_usage_log_id IN ${this.#idListClause(kvlIds)}
    `;
    return this.athena.executeQuery(sql);
  }

  async fetchFlags(kvlIds) {
    if (!kvlIds.length) return [];
    const sql = `
      SELECT
        cai.kbase_version_usage_log_id  AS KBaseVersionUsageLogId,
        fci.flag_code_inst_label        AS FlagCodeInstLabel,
        fc.flag_code_label              AS FlagCodeLabel
      FROM iceberg_db.assessement_rubric_flagging_master afm
      INNER JOIN iceberg_db.call_assessment_inst_master cai
        ON cai.call_assessment_inst_master_id = afm.call_assessment_inst_master_id
      INNER JOIN iceberg_db.assessment_rubrics_score_flag_code_connect afc
        ON afc.assessment_rubrics_score_flag_code_connect_id = afm.assessment_rubrics_score_flag_code_connect_id
      INNER JOIN iceberg_db.flag_code_inst fci
        ON fci.flag_code_inst_id = afc.flag_code_inst_id
      INNER JOIN iceberg_db.flag_code fc
        ON fc.flag_code_id = fci.flag_code_id
      WHERE cai.kbase_version_usage_log_id IN ${this.#idListClause(kvlIds)}
    `;
    return this.athena.executeQuery(sql);
  }

  async fetchSentiment(kvlIds) {
    if (!kvlIds.length) return [];
    const sql = `
      SELECT
        cai.kbase_version_usage_log_id   AS KBaseVersionUsageLogId,
        ara.rubrics_parameter_label      AS RubricsParameterLabel,
        smi.sentiment_master_inst_label  AS SentimentMasterInstLabel
      FROM iceberg_db.assessment_rubrics_sentiment_connect arsc
      INNER JOIN iceberg_db.call_assessment_inst_master cai
        ON cai.call_assessment_inst_master_id = arsc.call_assessment_inst_master_id
      INNER JOIN iceberg_db.sentiment_master_inst smi
        ON smi.sentiment_master_inst_id = arsc.sentiment_master_inst_id
      INNER JOIN iceberg_db.assessment_rubrics_attribute_score ara
        ON ara.rubrics_parameter_id = arsc.assessment_rubrics_attribute_score_id
      WHERE cai.kbase_version_usage_log_id IN ${this.#idListClause(kvlIds)}
    `;
    return this.athena.executeQuery(sql);
  }

  async fetchFormScores(kvlIds) {
    if (!kvlIds.length) return [];
    const sql = `
      SELECT
        afcs.kbase_version_usage_log_id  AS KBaseVersionUsageLogId,
        ROUND(MAX(afcs.consolidated_score), 2) AS FormScore
      FROM iceberg_db.assessment_form_consolidated_score afcs
      WHERE afcs.kbase_version_usage_log_id IN ${this.#idListClause(kvlIds)}
      GROUP BY afcs.kbase_version_usage_log_id
    `;
    return this.athena.executeQuery(sql);
  }

  async fetchExternalAgent(kvlIds) {
    if (!kvlIds.length) return [];
    const sql = `
      SELECT
        kvl.kbase_version_usage_log_id  AS KBaseVersionUsageLogId,
        amt.external_id                 AS ExternalId,
        tpim.internal_label             AS InternalLabel
      FROM iceberg_db.agent_master_tpim_connect amt
      INNER JOIN iceberg_db.third_party_integration_master tpim
        ON tpim.tpim_id = amt.tpim_id
      INNER JOIN iceberg_db.kbase_version_usage_log kvl
        ON kvl.agent_master_id = amt.agent_id
      WHERE kvl.kbase_version_usage_log_id IN ${this.#idListClause(kvlIds)}
    `;
    return this.athena.executeQuery(sql);
  }

  // Produces one CSV row per assessment (matching Assessmenthandler.js's one-record-per-assessment).
  // assessmentIds is the ordered list from MySQL; assessmentToKvlId maps each to its primary KVL ID.
  // Athena data is indexed by KVL ID and looked up for each assessment.
  // When hasFilters=true, assessments whose KVL ID has no Athena data are excluded (filter applied).
  // scoreRules are evaluated against formScoreRows post-fetch (bypasses AFCS sync gaps in Athena).
  transformToCsv({
    assessmentIds,
    assessmentToKvlId,
    mainRows,
    rubricRows = [],
    mappingRows = [],
    flagRows = [],
    sentimentRows = [],
    externalRows = [],
    formScoreRows = [],
    hasFilters = false,
    scoreRules = [],
  }) {
    if (!assessmentIds || !assessmentIds.length) return [];

    // fetchMain now returns one row per kvl — base info only.
    const mainByKvl = {};
    for (const r of mainRows) {
      const k = r.KBaseVersionUsageLogId;
      mainByKvl[k] = {
        TransactionMasterId: r.TransactionMasterId,
        UserName: r.UserName,
        Phone: r.Phone,
        Created_Date: r.Created_Date,
        Agent_Id: r.Agent_Id,
        Agent_Name: r.Agent_Name,
        scores: {},
        attributes: {},
        _totalScore: 0,
        _totalMaxScore: 0,
        _seenParams: new Set(),
      };
    }

    // Merge rubric scores (now from a dedicated parallel query)
    for (const r of rubricRows) {
      const k = r.KBaseVersionUsageLogId;
      if (!r.RubricsParameterLabel) continue;
      const bucket = mainByKvl[k];
      if (!bucket) continue; // rubric with no main record — skip
      if (bucket._seenParams.has(r.RubricsParameterLabel)) continue;
      bucket._seenParams.add(r.RubricsParameterLabel);
      bucket.scores[`${r.RubricsParameterLabel}_Score`] = r.Score;
      bucket.scores[`${r.RubricsParameterLabel}_Percentage`] = r.ScorePercentage;
      bucket._totalScore += Number(r.Score) || 0;
      bucket._totalMaxScore += Number(r.MaxScore) || 0;
    }

    // Merge attribute mappings (now from a dedicated parallel query)
    for (const r of mappingRows) {
      const k = r.KBaseVersionUsageLogId;
      if (!r.TrxAttributeMappingTypeLabel) continue;
      const bucket = mainByKvl[k];
      if (!bucket) continue; // attrs with no main record — skip
      bucket.attributes[r.TrxAttributeMappingTypeLabel] =
        r.TrxAttributeMappingsInstLabel;
    }

    const flagMap = {};
    for (const r of flagRows) {
      const k = r.KBaseVersionUsageLogId;
      if (!r.FlagCodeInstLabel) continue;
      (flagMap[k] ||= {})[r.FlagCodeInstLabel] = r.FlagCodeLabel;
    }

    const sentimentMap = {};
    for (const r of sentimentRows) {
      const k = r.KBaseVersionUsageLogId;
      if (!r.RubricsParameterLabel) continue;
      (sentimentMap[k] ||= {})[r.RubricsParameterLabel] = r.SentimentMasterInstLabel;
    }

    const externalMap = {};
    for (const r of externalRows) {
      const k = r.KBaseVersionUsageLogId;
      if (!r.InternalLabel) continue;
      (externalMap[k] ||= {})[r.InternalLabel] = r.ExternalId;
    }

    const formScoreMap = {};
    for (const r of formScoreRows) {
      formScoreMap[r.KBaseVersionUsageLogId] = r.FormScore;
    }

    return assessmentIds
      .filter((aid) => {
        const kvlId = assessmentToKvlId.get(aid);
        if (kvlId == null) return false;
        if (hasFilters) {
          if (mainByKvl[kvlId] == null) return false;
        }
        if (scoreRules.length > 0) {
          // A score-filter rule is meaningless for an unscored assessment —
          // drop it. (When no scoreRules are present we keep unscored kvls
          // so the trend universe matches the assessment view / benchmark
          // count, with Form_Score left blank.)
          const raw = formScoreMap[kvlId];
          const score = parseFloat(raw);
          if (isNaN(score)) return false;
          for (const { op, val } of scoreRules) {
            if (op === "="  && !(score === val)) return false;
            if (op === "!=" && !(score !== val)) return false;
            if (op === "<"  && !(score < val))   return false;
            if (op === "<=" && !(score <= val))   return false;
            if (op === ">"  && !(score > val))    return false;
            if (op === ">=" && !(score >= val))   return false;
          }
        }
        return true;
      })
      .map((aid) => {
        const kvlId = assessmentToKvlId.get(aid) ?? null;
        const athena = kvlId ? (mainByKvl[kvlId] ?? null) : null;
        const totalMax = athena?._totalMaxScore ?? 0;
        const overallPct = totalMax > 0
          ? Math.round((athena._totalScore / totalMax) * 100 * 100) / 100
          : null;
        return {
          KBaseVersionUsageLogId: kvlId,
          TransactionId: athena?.TransactionMasterId ?? null,
          UserName: athena?.UserName ?? null,
          Phone: athena?.Phone ?? null,
          Created_Date: athena?.Created_Date ?? null,
          Agent_Id: athena?.Agent_Id ?? null,
          Agent_Name: athena?.Agent_Name ?? null,
          Overall_Score_Percentage: kvlId != null ? (formScoreMap[kvlId] ?? null) : null,
          ...(kvlId ? (externalMap[kvlId] ?? {}) : {}),
          ...(athena?.scores ?? {}),
          ...(athena?.attributes ?? {}),
          ...(kvlId ? (flagMap[kvlId] ?? {}) : {}),
          ...(kvlId ? (sentimentMap[kvlId] ?? {}) : {}),
        };
      });
  }
}

module.exports = TrendExportService;

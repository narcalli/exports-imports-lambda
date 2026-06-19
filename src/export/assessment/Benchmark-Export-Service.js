const AthenaService = require("../../services/athena.service");

const OPERATOR_MAP = {
  eq: "=",
  EQ: "=",
  gt: ">",
  GT: ">",
  lt: "<",
  LT: "<",
  gte: ">=",
  GTE: ">=",
  lte: "<=",
  LTE: "<=",
  ne: "!=",
  NE: "!=",
};

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
    const y = v.getFullYear();
    const m = String(v.getMonth() + 1).padStart(2, "0");
    const d = String(v.getDate()).padStart(2, "0");
    return `${y}-${m}-${d}`;
  }
  const s = String(v || "").trim();
  const m = s.match(/^(\d{4}-\d{2}-\d{2})/);
  if (!m) throw new Error(`Invalid date: ${v}`);
  return m[1];
}

class BenchmarkExportService {
  constructor(athena) {
    this.athena = athena || new AthenaService();
  }

  // Converts form_schema_trx_attribute_inst_id rules → form_id rules by resolving
  // the attribute inst ID to its associated FormMasterId values via MySQL.
  async resolveRules(conn, rules) {
    const resolved = [];
    for (const rule of rules) {
      if (rule.field === "form_schema_trx_attribute_inst_id") {
        const ids = Array.isArray(rule.value) ? rule.value : [rule.value];
        const rows = await conn.queryAsync(
          `SELECT DISTINCT fsfmc.FormMasterId
           FROM FormSchemaTrxAttributeInst fsai
           JOIN FormSchemaFormMasterConnect fsfmc ON fsfmc.FormSchemaInstMasterId = fsai.FormSchemaInstMasterId
           WHERE fsai.FormSchemaTrxAttributeInstId IN (${ids.map(() => "?").join(",")})`,
          ids,
        );
        if (rows.length > 0) {
          resolved.push({ ...rule, field: "form_id", value: rows.map((r) => r.FormMasterId) });
        }
      } else {
        resolved.push(rule);
      }
    }
    return resolved;
  }

  #formSchemaIdSubquery(op, vals) {
    const clause = vals.length > 1
      ? `IN (${vals.map(sqlNum).join(",")})`
      : `${op} ${sqlNum(vals[0])}`;
    return `kvl.kbase_version_usage_log_id IN (
      SELECT afsc_f.kbase_version_usage_log_id
      FROM iceberg_db.assessment_form_schema_consolidated_score afsc_f
      JOIN iceberg_db.form_schema_wise_weightage fsww_f
        ON fsww_f.form_schema_wise_weightage_id = afsc_f.form_schema_wise_weightage_id
      JOIN iceberg_db.form_schema_form_master_connect fsfc_f
        ON fsfc_f.form_schema_form_master_connect_id = fsww_f.form_schema_form_master_connect_id
      WHERE fsfc_f.form_schema_inst_master_id ${clause}
    )`;
  }

  #rubricsParamIdSubquery(op, vals) {
    const clause = vals.length > 1
      ? `IN (${vals.map(sqlNum).join(",")})`
      : `${op} ${sqlNum(vals[0])}`;
    return `kvl.kbase_version_usage_log_id IN (
      SELECT arsm_f.kbase_version_usage_log_id
      FROM iceberg_db.assessment_rubrics_scoring_master arsm_f
      JOIN iceberg_db.assessment_rubrics_attribute_score aras_f
        ON aras_f.rubrics_parameter_id = arsm_f.assessment_rubrics_attribute_score_id
      WHERE aras_f.rubrics_parameter_id ${clause}
    )`;
  }

  async buildWhere(rules, combinator = "and") {
    const main = [];
    const having = [];
    const kvlSubqueryParts = [];

    if (!Array.isArray(rules) || rules.length === 0) {
      return { mainWhere: "", having: "", kvlFilter: "" };
    }

    // form_id uses cai.form_master_id so it matches csim.FormMasterId (assessment view),
    // not afcs.form_master_id which can be NULL for unscored kvls.
    // agent_id uses ONLY acsa.agent_id — matches Assessmenthandler.js's list filter
    // exactly (acic.AgentId). kvl.agent_master_id is a different path and can resolve
    // to a different agent for the same call.
    const directWhere = {
      form_percentage: "afcs.consolidated_score",
      avg_percentage_score: "afcs.consolidated_score",
      form_id: "cai.form_master_id",
      agent_id: "acsa.agent_id",
    };

    const havingFields = {
      sum_score:
        "CASE WHEN SUM(arsm.max_score) > 0 " +
        "THEN ROUND(SUM(arsm.score)/SUM(arsm.max_score)*100, 2) " +
        "ELSE NULL END",
    };

    for (const r of rules) {
      const { field, operator, value } = r;
      const op = OPERATOR_MAP[operator];
      if (!op) continue;
      if (value === undefined || value === null || value === "") continue;
      const vals = Array.isArray(value) ? value : [value];
      if (!vals.length) continue;

      if (havingFields[field]) {
        having.push(`${havingFields[field]} ${op} ${sqlNum(vals[0])}`);
      } else if (field === "form_schema_id") {
        kvlSubqueryParts.push(this.#formSchemaIdSubquery(op, vals));
      } else if (field === "rubrics_parameter_id") {
        kvlSubqueryParts.push(this.#rubricsParamIdSubquery(op, vals));
      } else if (directWhere[field]) {
        if (vals.length > 1) {
          main.push(`${directWhere[field]} IN (${vals.map(sqlNum).join(",")})`);
        } else {
          main.push(`${directWhere[field]} ${op} ${sqlNum(vals[0])}`);
        }
      } else {
        // Dynamic attribute (Branch, LOB, ...) — apply via kvl IN (sub-query)
        const fieldLit = sqlStr(field);
        const valLit = sqlNum(vals[0]);
        kvlSubqueryParts.push(
          `kvl.kbase_version_usage_log_id IN (
             SELECT kvt.kbase_version_usage_log_id
             FROM iceberg_db.kbase_version_usage_log_trx_attribute_mapping_con kvt
             JOIN iceberg_db.trx_attribute_mappings_inst tam
               ON tam.trx_attribute_mappings_inst_id = kvt.trx_attribute_mappings_inst_id
             JOIN iceberg_db.trx_attribute_mapping_types tmt
               ON tmt.trx_attribute_mapping_type_id = tam.trx_attribute_mapping_type_id
             WHERE tmt.trx_attribute_mapping_type_label = ${fieldLit}
               AND tam.trx_attribute_mappings_inst_id ${op} ${valLit}
           )`,
        );
      }
    }

    const comb = String(combinator).toUpperCase() === "OR" ? "OR" : "AND";
    const allMain = [...main, ...kvlSubqueryParts];
    return {
      mainWhere: allMain.length ? `AND (${allMain.join(` ${comb} `)})` : "",
      having: having.length ? `HAVING ${having.join(` ${comb} `)}` : "",
    };
  }

  // When `kvlIds` are passed, caller has already validated date+institution in
  // MySQL — re-checking against Athena tm/kvl re-introduces the sync drops we
  // just fixed, so we trust the input.
  // Apply BOTH the date filter and (when available) the IN-list:
  //   - Date filter enables partition pruning on kvl.audit_create_time —
  //     without it Athena scans every partition, then post-filters by IN-list.
  //   - IN-list narrows to the exact universe set after pruning.
  // The benchmark handler now passes startDate/endDate alongside kvlIds for
  // this reason. Caller can still pass kvlIds-only (legacy) or dates-only.
  #rangeFilter({ kvlIds, startDate, endDate }) {
    const parts = [];
    if (startDate && endDate) {
      const start = dateOnly(startDate);
      const end = dateOnly(endDate);
      parts.push(
        `AND kvl.audit_create_time >= TIMESTAMP '${start} 00:00:00'`,
      );
      parts.push(
        `AND kvl.audit_create_time <= TIMESTAMP '${end} 23:59:59'`,
      );
    }
    if (kvlIds && kvlIds.length) {
      parts.push(
        `AND kvl.kbase_version_usage_log_id IN ${this.#idListClause(kvlIds)}`,
      );
    }
    return parts.join("\n        ");
  }

  #institutionGuard({ iid, kvlIds }) {
    if (kvlIds && kvlIds.length) return "";
    return `AND tm.institution_id = ${sqlNum(iid)}`;
  }

  // Scope a side query to the same assessed-call universe Assessmenthandler.js
  // builds — without ever shipping an IN-list (which hits Athena's 256KB limit
  // around ~25k kvls). Each side query just appends:
  //   AND <local_col> IN (${this.#universeSubquery(iid, startDate, endDate)})
  // kvl.audit_create_time is timestamp(6) in Iceberg, so the right side must
  // be a TIMESTAMP literal (not a varchar). Left side stays uncast so Athena
  // can push the predicate down to partition pruning.
  #universeSubquery(iid, startDate, endDate) {
    const start = dateOnly(startDate);
    const end = dateOnly(endDate);
    return `
      SELECT kvl.kbase_version_usage_log_id
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
    `;
  }

  // Returns the WHERE-clause fragment a side query uses to scope to the right
  // kvls. Accepts either an explicit kvlIds list (small / one-off callers) or
  // iid + startDate + endDate (the date-range path used by exports at scale).
  // `idColumn` is the local table's kvl-id column, e.g. "kvt.kbase_version_usage_log_id".
  #kvlScopeClause({ idColumn, iid, startDate, endDate, kvlIds }) {
    if (kvlIds && kvlIds.length) {
      return `${idColumn} IN ${this.#idListClause(kvlIds)}`;
    }
    return `${idColumn} IN (${this.#universeSubquery(iid, startDate, endDate)})`;
  }

  // Additional WHERE predicate that filters a side table on its OWN
  // audit_create_time (with a 1-day buffer on each side so we don't miss
  // rows whose creation timestamp drifted slightly from the kvl's). This is
  // the partition-pruning lever for side tables — without it Athena has to
  // scan the entire history of flags/sentiment/attribute mappings for
  // every export. Caller passes the side table's date column reference.
  #sideTableDateFilter(dateColumn, startDate, endDate) {
    if (!startDate || !endDate) return "";
    const start = dateOnly(startDate);
    const end = dateOnly(endDate);
    return `AND ${dateColumn} >= TIMESTAMP '${start} 00:00:00' - INTERVAL '1' DAY
            AND ${dateColumn} <= TIMESTAMP '${end} 23:59:59' + INTERVAL '1' DAY`;
  }

  // Pure-Athena replacement for TrendExportService.fetchAssessments.
  // Builds the assessed-kvl universe straight from Iceberg so we never have to
  // ship a multi-MB IN-list to Athena.
  //
  // Iceberg's call_assessment_inst_master has no timestamp column (only the 3
  // FK columns), so we drive the date filter off kvl.audit_create_time —
  // the same column the cross-domain Athena queries use with raw IST dates.
  // The cai/ctt/tm joins still enforce "must have a csim row + correct
  // institution", matching Assessmenthandler.actionList's universe shape.
  async fetchAssessedKvlsFromAthena(iid, startDate, endDate) {
    const start = dateOnly(startDate);
    const end = dateOnly(endDate);
    const sql = `
      SELECT DISTINCT kvl.kbase_version_usage_log_id AS kbase_version_usage_log_id
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
    `;
    const rows = await this.athena.executeQuery(sql);
    return rows
      .map((r) => parseInt(r.kbase_version_usage_log_id, 10))
      .filter((v) => !isNaN(v));
  }

  async getCount({ iid, kvlIds, startDate, endDate, mainWhere }) {
    const sql = `
      SELECT COUNT(DISTINCT kvl.kbase_version_usage_log_id) AS total
      FROM iceberg_db.kbase_version_usage_log kvl
      LEFT JOIN iceberg_db.assessment_form_consolidated_score afcs
        ON afcs.kbase_version_usage_log_id = kvl.kbase_version_usage_log_id
      LEFT JOIN iceberg_db.call_assessment_inst_master cai
        ON cai.kbase_version_usage_log_id = kvl.kbase_version_usage_log_id
      LEFT JOIN iceberg_db.con_transaction_telephony ctt
        ON ctt.con_transaction_telephony_id = cai.con_transaction_telephony_id
      LEFT JOIN iceberg_db.transaction_masters tm
        ON tm.transaction_master_id = ctt.transaction_master_id
      LEFT JOIN iceberg_db.ask_conv_session_agent_inst_con acsa
        ON acsa.ask_conv_session_agent_inst_con_id = ctt.ask_conv_session_agent_inst_con_id
      WHERE 1=1
        ${this.#rangeFilter({ kvlIds, startDate, endDate })}
        ${this.#institutionGuard({ iid, kvlIds })}
        ${mainWhere || ""}
    `;
    const rows = await this.athena.executeQuery(sql);
    return parseInt(rows?.[0]?.total || "0", 10);
  }

  async fetchMain({ iid, kvlIds, startDate, endDate, mainWhere, having }) {
    // Lean fetchMain — mirrors TrendExportService.fetchMain (which finishes
    // in ~20s). All form-related data (afcs.consolidated_score,
    // afcs.form_master_id, fm.form_label) used to live here behind extra
    // LEFT JOINs; that whole chain has been split into fetchFormInfo and
    // runs in parallel with this query.
    //
    // arsm explodes intermediate rows (~10× kvl count), so we only join it
    // when sum_score HAVING is present. The common no-HAVING path skips it.
    const needsArsm = !!(having && having.trim());
    const arsmJoin = needsArsm
      ? `LEFT JOIN iceberg_db.assessment_rubrics_scoring_master arsm
        ON arsm.kbase_version_usage_log_id = kvl.kbase_version_usage_log_id`
      : "";

    const sql = `
      SELECT
        kvl.kbase_version_usage_log_id                                 AS kbase_version_usage_log_id,
        MAX(tm.transaction_master_id)                                  AS transaction_master_id,
        MAX(COALESCE(um.user_name, ''))                                AS user_name,
        MAX(um.user_mobile_number)                                     AS user_mobile_number,
        date_format(MAX(CAST(kvl.audit_create_time AS TIMESTAMP)),
                    '%d %b %Y %H:%i:%s')                               AS created_date,
        MAX(COALESCE(am.agent_name, ''))                               AS agent_name,
        MAX(acsa.agent_id)                                             AS agent_id
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
      ${arsmJoin}
      WHERE 1=1
        ${this.#rangeFilter({ kvlIds, startDate, endDate })}
        ${this.#institutionGuard({ iid, kvlIds })}
        ${mainWhere || ""}
      GROUP BY kvl.kbase_version_usage_log_id
      ${having || ""}
    `;
    return this.athena.executeQuery(sql);
  }

  // Form-level info per kvl — split out of fetchMain so afcs/cai/fm joins
  // don't bottleneck the base-info query. Runs in parallel with fetchMain
  // in the handler. transformToCsv merges results by kvl id.
  async fetchFormInfo(arg) {
    const opts = this.#normaliseScopeArg(arg);
    if (this.#scopeIsEmpty(opts)) return [];
    const scope = this.#kvlScopeClause({
      idColumn: "afcs.kbase_version_usage_log_id",
      ...opts,
    });
    const sideFilter = this.#sideTableDateFilter(
      "afcs.audit_create_time",
      opts.startDate,
      opts.endDate,
    );
    const sql = `
      SELECT
        afcs.kbase_version_usage_log_id                AS kbase_version_usage_log_id,
        MAX(COALESCE(cai.form_master_id, afcs.form_master_id))
                                                       AS form_master_id,
        MAX(fm.form_label)                             AS form_label,
        ROUND(MAX(afcs.consolidated_score), 2)         AS form_percentage
      FROM iceberg_db.assessment_form_consolidated_score afcs
      LEFT JOIN iceberg_db.call_assessment_inst_master cai
        ON cai.kbase_version_usage_log_id = afcs.kbase_version_usage_log_id
      LEFT JOIN iceberg_db.form_master fm
        ON fm.form_master_id = COALESCE(cai.form_master_id, afcs.form_master_id)
      WHERE ${scope}
        ${sideFilter}
      GROUP BY afcs.kbase_version_usage_log_id
    `;
    return this.athena.executeQuery(sql);
  }

  #idListClause(ids) {
    if (!ids || !ids.length) return "(NULL)";
    return `(${ids
      .map((v) => parseInt(v, 10))
      .filter((v) => !isNaN(v))
      .join(",")})`;
  }

  // ─── Side queries ───────────────────────────────────────────────────────
  // All accept either a legacy kvlIds array (small/one-off callers like the
  // diff script and filter-stream controller) or an object with
  // { iid, startDate, endDate } so the universe is scoped via the inline
  // #universeSubquery — no IN-list, no 256KB ceiling.
  #normaliseScopeArg(arg) {
    if (Array.isArray(arg)) return { kvlIds: arg };
    return arg || {};
  }

  #scopeIsEmpty({ kvlIds, startDate, endDate }) {
    if (kvlIds && kvlIds.length) return false;
    if (startDate && endDate) return false;
    return true;
  }

  async fetchSchemaScoreWithWeight(arg) {
    const opts = this.#normaliseScopeArg(arg);
    if (this.#scopeIsEmpty(opts)) return [];
    const scope = this.#kvlScopeClause({
      idColumn: "sch.kbase_version_usage_log_id",
      ...opts,
    });
    // Push the date filter into sch.audit_create_time so Athena can prune
    // partitions before the join — was 54s without this, returning 19813
    // rows out of likely millions in the table.
    const sideFilter = this.#sideTableDateFilter(
      "sch.audit_create_time",
      opts.startDate,
      opts.endDate,
    );
    const sql = `
    select
        sch.kbase_version_usage_log_id,
        fsm.form_schema_inst_master_label,
        sch.consolidated_score                                                               as schema_raw,
        case
            when sch.consolidated_score > 0 then round((fsw.weightage_value / 100) * sch.consolidated_score, 2)
            else null
        end                                                                                  as schema_weighted,
        fsw.weightage_value,
        concat(fsm.form_schema_inst_master_label, '_', cast(fsw.weightage_value as varchar)) as column_label
    from
        iceberg_db.assessment_form_schema_consolidated_score sch
        join iceberg_db.form_schema_wise_weightage fsw
            on fsw.form_schema_wise_weightage_id = sch.form_schema_wise_weightage_id
        join iceberg_db.form_schema_form_master_connect fsfm
            on fsfm.form_schema_form_master_connect_id = fsw.form_schema_form_master_connect_id
        join iceberg_db.form_schema_inst_master fsm on fsm.form_schema_inst_master_id = fsfm.form_schema_inst_master_id
    where ${scope}
      ${sideFilter}
    `;
    return this.athena.executeQuery(sql);
  }

  async fetchAttributeMappings(arg) {
    const opts = this.#normaliseScopeArg(arg);
    if (this.#scopeIsEmpty(opts)) return [];
    const scope = this.#kvlScopeClause({
      idColumn: "kvt.kbase_version_usage_log_id",
      ...opts,
    });
    // kvt (kbase_version_usage_log_trx_attribute_mapping_con) does NOT have
    // audit_create_time in this Iceberg sync. Attempts to add a partition
    // filter here error with COLUMN_NOT_FOUND, so we rely purely on the
    // kvl-id IN-clause from #kvlScopeClause.
    const sql = `
      SELECT
        kvt.kbase_version_usage_log_id                 AS kbase_version_usage_log_id,
        tmt.trx_attribute_mapping_type_label           AS type_label,
        tam.trx_attribute_mappings_inst_label          AS value_label
      FROM iceberg_db.kbase_version_usage_log_trx_attribute_mapping_con kvt
      INNER JOIN iceberg_db.trx_attribute_mappings_inst tam
        ON tam.trx_attribute_mappings_inst_id = kvt.trx_attribute_mappings_inst_id
      INNER JOIN iceberg_db.trx_attribute_mapping_types tmt
        ON tmt.trx_attribute_mapping_type_id = tam.trx_attribute_mapping_type_id
      WHERE ${scope}
    `;
    return this.athena.executeQuery(sql);
  }

  async fetchFlags(arg) {
    const opts = this.#normaliseScopeArg(arg);
    if (this.#scopeIsEmpty(opts)) return [];
    const scope = this.#kvlScopeClause({
      idColumn: "cai.kbase_version_usage_log_id",
      ...opts,
    });
    const sideFilter = this.#sideTableDateFilter(
      "afm.audit_create_time",
      opts.startDate,
      opts.endDate,
    );
    const sql = `
      SELECT
        cai.kbase_version_usage_log_id   AS kbase_version_usage_log_id,
        fci.flag_code_inst_label         AS flag_inst_label,
        fc.flag_code_label               AS flag_code_label
      FROM iceberg_db.assessement_rubric_flagging_master afm
      INNER JOIN iceberg_db.call_assessment_inst_master cai
        ON cai.call_assessment_inst_master_id = afm.call_assessment_inst_master_id
      INNER JOIN iceberg_db.assessment_rubrics_score_flag_code_connect afc
        ON afc.assessment_rubrics_score_flag_code_connect_id = afm.assessment_rubrics_score_flag_code_connect_id
      INNER JOIN iceberg_db.flag_code_inst fci
        ON fci.flag_code_inst_id = afc.flag_code_inst_id
      INNER JOIN iceberg_db.flag_code fc
        ON fc.flag_code_id = fci.flag_code_id
      WHERE ${scope}
        ${sideFilter}
    `;
    return this.athena.executeQuery(sql);
  }

  async fetchSentiment(arg) {
    const opts = this.#normaliseScopeArg(arg);
    if (this.#scopeIsEmpty(opts)) return [];
    const scope = this.#kvlScopeClause({
      idColumn: "cai.kbase_version_usage_log_id",
      ...opts,
    });
    const sideFilter = this.#sideTableDateFilter(
      "arsc.audit_create_time",
      opts.startDate,
      opts.endDate,
    );
    const sql = `
      SELECT
        cai.kbase_version_usage_log_id    AS kbase_version_usage_log_id,
        ara.rubrics_parameter_label       AS rubrics_parameter_label,
        smi.sentiment_master_inst_label   AS sentiment_label
      FROM iceberg_db.assessment_rubrics_sentiment_connect arsc
      INNER JOIN iceberg_db.call_assessment_inst_master cai
        ON cai.call_assessment_inst_master_id = arsc.call_assessment_inst_master_id
      INNER JOIN iceberg_db.sentiment_master_inst smi
        ON smi.sentiment_master_inst_id = arsc.sentiment_master_inst_id
      INNER JOIN iceberg_db.assessment_rubrics_attribute_score ara
        ON ara.rubrics_parameter_id = arsc.assessment_rubrics_attribute_score_id
      WHERE ${scope}
        ${sideFilter}
    `;
    return this.athena.executeQuery(sql);
  }

  async fetchExternalAgent(arg) {
    const opts = this.#normaliseScopeArg(arg);
    if (this.#scopeIsEmpty(opts)) return [];
    const scope = this.#kvlScopeClause({
      idColumn: "cai.kbase_version_usage_log_id",
      ...opts,
    });
    const sql = `
      SELECT
        cai.kbase_version_usage_log_id   AS kbase_version_usage_log_id,
        amt.external_id                  AS external_id,
        tpim.internal_label              AS internal_label
      FROM iceberg_db.call_assessment_inst_master cai
      INNER JOIN iceberg_db.con_transaction_telephony ctt
        ON ctt.con_transaction_telephony_id = cai.con_transaction_telephony_id
      INNER JOIN iceberg_db.ask_conv_session_agent_inst_con acsa
        ON acsa.ask_conv_session_agent_inst_con_id = ctt.ask_conv_session_agent_inst_con_id
      INNER JOIN iceberg_db.agent_master_tpim_connect amt
        ON amt.agent_id = acsa.agent_id
      INNER JOIN iceberg_db.third_party_integration_master tpim
        ON tpim.tpim_id = amt.tpim_id
      WHERE ${scope}
    `;
    return this.athena.executeQuery(sql);
  }

  transformToCsv({
    mainRows,
    formInfoRows = [],
    mappingRows = [],
    flagRows = [],
    sentimentRows = [],
    externalRows = [],
    schemaResult = [],
  }) {
    if (!mainRows || !mainRows.length) return [];

    const mappingMap = {};
    const flagMap = {};
    const sentimentMap = {};
    const externalMap = {};
    const schemaMap = {};
    const formInfoMap = {};

    for (const r of mappingRows) {
      const k = r.kbase_version_usage_log_id;
      if (!r.type_label) continue;
      (mappingMap[k] ||= {})[r.type_label] = r.value_label;
    }
    for (const r of flagRows) {
      const k = r.kbase_version_usage_log_id;
      if (!r.flag_inst_label) continue;
      (flagMap[k] ||= {})[r.flag_inst_label] = r.flag_code_label;
    }
    for (const r of sentimentRows) {
      const k = r.kbase_version_usage_log_id;
      if (!r.rubrics_parameter_label) continue;
      (sentimentMap[k] ||= {})[r.rubrics_parameter_label] = r.sentiment_label;
    }
    for (const r of externalRows) {
      const k = r.kbase_version_usage_log_id;
      if (!r.internal_label) continue;
      (externalMap[k] ||= {})[r.internal_label] = r.external_id;
    }
    for (const r of schemaResult) {
      const k = r.kbase_version_usage_log_id;
      if (!r.form_schema_inst_master_label) continue;
      (schemaMap[k] ||= {})[`${r.form_schema_inst_master_label}_raw`] =
        r.schema_raw;
      (schemaMap[k] ||= {})[r.column_label] = r.schema_weighted;
    }
    // formInfoRows: { kvl_id, form_master_id, form_label, form_percentage }
    // — populated by fetchFormInfo (split out of fetchMain for speed).
    for (const r of formInfoRows) {
      const k = r.kbase_version_usage_log_id;
      formInfoMap[k] = {
        Form_Label: r.form_label || "",
        Form_Percentage: r.form_percentage || "",
      };
    }

    const grouped = {};

    for (const r of mainRows) {
      const k = r.kbase_version_usage_log_id;
      if (!grouped[k]) {
        grouped[k] = {
          KBaseVersionUsageLogId: k,
          TransactionId: r.transaction_master_id || "",
          UserName: r.user_name || "",
          Phone: r.user_mobile_number ? `'${r.user_mobile_number}` : "",
          Created_Date: r.created_date || "",
          Agent_Id: r.agent_id || "",
          Agent_Name: r.agent_name || "",
          Form_Label: "",
          Form_Percentage: "",
        };
      }
    }

    // Merge side-query columns (form info, schema, attributes, flags, sentiment, external agents)
    for (const k of Object.keys(grouped)) {
      Object.assign(
        grouped[k],
        formInfoMap[k] || {},
        schemaMap[k] || {},
        mappingMap[k] || {},
        flagMap[k] || {},
        sentimentMap[k] || {},
        externalMap[k] || {},
      );
    }

    return Object.values(grouped);
  }
}

module.exports = BenchmarkExportService;

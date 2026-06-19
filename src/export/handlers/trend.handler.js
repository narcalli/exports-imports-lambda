const AthenaService = require("../../services/athena.service");
const TrendExportService = require("../assessment/Trend-Export-Service");
const { toCsv } = require("../_shared/csv");
const { buildJobFileName } = require("../_shared/fileName");
const getConnection = require("../../../allservices/getConnection");

function validateParameters(body) {
  const errors = [];
  if (!body) errors.push("missing body");
  if (body && !body.institutionId) errors.push("institutionId required");
  if (body && !body.startDate) errors.push("startDate required");
  if (body && !body.endDate) errors.push("endDate required");
  if (body && body.rules && !Array.isArray(body.rules))
    errors.push("rules must be an array");
  if (errors.length) {
    const e = new Error(errors.join("; "));
    e.statusCode = 400;
    throw e;
  }
}

function normalizeParameters(body) {
  // Accept rules/combinator at top-level OR nested under `parameters` — some
  // callers wrap them, others pass them flat.
  const src = body && body.parameters && typeof body.parameters === "object"
    ? { ...body.parameters, ...body }
    : body || {};
  const rules = Array.isArray(src.rules)
    ? src.rules
    : Array.isArray(body?.parameters?.rules) ? body.parameters.rules : [];
  const rawComb = src.combinator ?? body?.parameters?.combinator ?? "and";
  return {
    rules,
    combinator: String(rawComb).toLowerCase() === "or" ? "or" : "and",
  };
}

// Resolve form_schema_trx_attribute_inst_id → FormMasterId list via MySQL.
// Matches ScoreReportsService.#resolveFormMasterIdsByAttr (granularity=1 approach).
async function resolveFormAttrFilter(rules, queryFn) {
  const out = [];
  for (const r of rules) {
    if (r.field === "form_schema_trx_attribute_inst_id" && r.value != null) {
      const rows = await queryFn(
        `SELECT DISTINCT fsfmc.FormMasterId
         FROM FormSchemaTrxAttributeInst fsai
         JOIN FormSchemaFormMasterConnect fsfmc ON fsfmc.FormSchemaInstMasterId = fsai.FormSchemaInstMasterId
         WHERE fsai.FormSchemaTrxAttributeInstId = ?`,
        [r.value],
      );
      const formIds = rows.map((f) => f.FormMasterId);
      console.log(`[trend handler] form_schema_trx_attribute_inst_id=${r.value} resolved to formIds=${JSON.stringify(formIds)}`);
      // Replace with formId rule; buildWhere handles the AFCS subquery for form_master_id.
      out.push({ ...r, field: "formId", operator: "eq", value: formIds.length ? formIds : [-1] });
    } else {
      out.push(r);
    }
  }
  return out;
}

// Resolve LOB or Branch mapping inst ID → agent IDs, same logic as ScoreReportsService.
// Dashboard always converts LOB/Branch to agent lists; export must do the same.
async function resolveLobBranch(rules, queryFn) {
  const out = [];
  for (const r of rules) {
    if ((r.field === "LOB" || r.field === "Branch") && r.value != null) {
      const rows = await queryFn(
        `SELECT am.AgentMasterId
         FROM TrxAttributeMappingsInst tmi
         JOIN TrxAttributeMappingChildren tamc ON tmi.TrxAttributeMappingsInstId = tamc.TrxAttributeMappingsInstId
         JOIN SchoolMaster sm ON tamc.Payload = sm.SchoolId
         JOIN AgentMaster am ON am.SchoolId = sm.SchoolId
         WHERE tmi.TrxAttributeMappingsInstId = ?`,
        [r.value],
      );
      const agentIds = rows.map((a) => a.AgentMasterId);
      console.log(`[trend handler] ${r.field}=${r.value} resolved to agents=${JSON.stringify(agentIds)}`);
      // Replace with agent_id rule; buildWhere handles arrays with IN automatically.
      // Use [-1] when no agents found so the Athena IN clause matches nothing.
      out.push({ ...r, field: "agent_id", operator: "eq", value: agentIds.length ? agentIds : [-1] });
    } else {
      out.push(r);
    }
  }
  return out;
}

async function run(jobRow, { onProgress }) {
  const params =
    typeof jobRow.Parameters === "string"
      ? JSON.parse(jobRow.Parameters || "{}")
      : jobRow.Parameters || {};

  const iid = jobRow.InstitutionId;
  const startDate = jobRow.StartDate;
  const endDate = jobRow.EndDate;
  const rules = Array.isArray(params.rules) ? params.rules : [];
  const combinator = params.combinator || "and";

  console.log(
    `[trend handler] job=${jobRow.ExportJobId} iid=${iid} ` +
      `start=${startDate} end=${endDate} combinator=${combinator} ` +
      `rules=${JSON.stringify(rules)}`,
  );

  // MySQL is now only needed for LOB/Branch + form_schema_trx_attribute_inst_id
  // rule resolution — small lookups that don't fire on unfiltered exports.
  // Universe + form scores + everything else is pure Athena.
  const needsMysql = rules.some(
    (r) =>
      r &&
      (r.field === "LOB" ||
        r.field === "Branch" ||
        r.field === "form_schema_trx_attribute_inst_id"),
  );
  let resolvedRules = rules;
  if (needsMysql) {
    const conn = await getConnection();
    const queryFn = (sql, p) => conn.queryAsync(sql, p);
    resolvedRules = await resolveFormAttrFilter(
      await resolveLobBranch(rules, queryFn),
      queryFn,
    );
  }

  const service = new TrendExportService(new AthenaService());
  const { mainWhere, scoreRules } = await service.buildWhere(resolvedRules, combinator);
  console.log(`[trend handler] mainWhere=${mainWhere} scoreRules=${JSON.stringify(scoreRules)}`);

  await onProgress(5, "resolving assessed calls by date");
  const t0 = Date.now();
  const { assessmentIds, kvlIds, assessmentToKvlId } =
    await service.fetchAssessmentUniverseFromAthena(iid, startDate, endDate);

  console.log(
    `[trend handler] assessments=${assessmentIds.length} kvlIds=${kvlIds.length} (${Date.now() - t0}ms)`,
  );

  if (!assessmentIds.length) {
    await onProgress(100, "no records");
    return {
      csv: "",
      totalRecords: 0,
      fileName: buildFileName(iid, startDate, endDate, jobRow, rules),
    };
  }

  await onProgress(10, `${assessmentIds.length} assessed calls found`);

  // Kick off all 7 Athena queries in parallel. fetchMain now returns one
  // row per kvl (mirroring the benchmark service); rubric scores and
  // attribute mappings live in their own dedicated queries so we no longer
  // pay for the kvl × rubric × attribute cartesian.
  const tQueriesStart = Date.now();
  let done = 0;
  const totalSteps = 7;
  const tick = async (label, t0, rows) => {
    done += 1;
    const pct = Math.min(95, 10 + Math.round((done / totalSteps) * 85));
    console.log(
      `[trend handler] ${label}: ${rows?.length ?? 0} rows in ${
        Date.now() - t0
      }ms`,
    );
    await onProgress(pct, label);
  };
  const timed = (label, fn) => {
    const t0 = Date.now();
    return fn()
      .catch((e) => {
        console.error(`[trend handler] ${label} err`, e);
        return [];
      })
      .then(async (r) => {
        await tick(label, t0, r);
        return r;
      });
  };

  const [
    mainRows,
    rubricRows,
    mappingRows,
    flagRows,
    sentimentRows,
    externalRows,
    formScoreRows,
  ] = await Promise.all([
    timed("main rows", () =>
      kvlIds.length
        ? service.fetchMain({ iid, validKvlIds: kvlIds, mainWhere })
        : Promise.resolve([]),
    ),
    timed("rubric scores", () => service.fetchRubricScores(kvlIds)),
    timed("attributes", () => service.fetchAttributeMappings(kvlIds)),
    timed("flags", () => service.fetchFlags(kvlIds)),
    timed("sentiment", () => service.fetchSentiment(kvlIds)),
    timed("external agents", () => service.fetchExternalAgent(kvlIds)),
    timed("form scores", () => service.fetchFormScores(kvlIds)),
  ]);

  console.log(
    `[trend handler] all queries done in ${Date.now() - tQueriesStart}ms`,
  );

  await onProgress(97, "building csv");

  const rows = service.transformToCsv({
    assessmentIds,
    assessmentToKvlId,
    mainRows,
    rubricRows,
    mappingRows,
    flagRows,
    sentimentRows,
    externalRows,
    formScoreRows,
    hasFilters: !!mainWhere,
    scoreRules,
  });

  const csv = toCsv(rows);
  console.log(
    `[trend handler] csv rows=${rows.length} totalMs=${Date.now() - t0}ms`,
  );

  return {
    csv,
    totalRecords: rows.length,
    fileName: buildFileName(iid, startDate, endDate, jobRow, rules),
  };
}

function buildFileName(iid, startDate, endDate, jobRow, rules) {
  return buildJobFileName(jobRow, {
    iid,
    startDate,
    endDate,
    hasFilters: Array.isArray(rules) && rules.length > 0,
  });
}

module.exports = { run, validateParameters, normalizeParameters, buildFileName };

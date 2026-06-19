const AthenaService = require("../../services/athena.service");
const BenchmarkExportService = require("../assessment/Benchmark-Export-Service");
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
  return {
    rules: Array.isArray(body.rules) ? body.rules : [],
    combinator:
      String(body.combinator || "and").toLowerCase() === "or" ? "or" : "and",
  };
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
    `[benchmark handler] job=${jobRow.ExportJobId} iid=${iid} ` +
      `start=${startDate} end=${endDate} combinator=${combinator} ` +
      `rules=${JSON.stringify(rules)}`,
  );

  const service = new BenchmarkExportService(new AthenaService());

  await onProgress(5, "resolving rules");

  // Filter rules using form_schema_trx_attribute_inst_id need a small lookup
  // to translate the attribute-inst IDs into form_master IDs. That lookup is
  // a handful of rows and lives in MySQL — we only open a DB connection when
  // a rule actually needs it, so unfiltered exports stay pure-Athena.
  const hasFormSchemaRule = rules.some(
    (r) => r && r.field === "form_schema_trx_attribute_inst_id",
  );
  let resolvedRules = rules;
  if (hasFormSchemaRule) {
    const conn = await getConnection();
    resolvedRules = await service.resolveRules(conn, rules);
  }

  const { mainWhere, having } = await service.buildWhere(
    resolvedRules,
    combinator,
  );
  console.log(`[benchmark handler] mainWhere=${mainWhere} having=${having}`);

  // Compute the assessed-kvl universe ONCE in Athena, then reuse it for all
  // 6 parallel queries. The earlier "embed the universe subquery in every
  // side query" approach made Athena recompute the kvl⋈cai⋈ctt⋈tm join 6×,
  // which dominated wall-clock time. One upfront query + small IN-lists is
  // dramatically faster for typical sizes (Athena's 256KB query-string cap
  // lets us pass ~25k kvl IDs; above that we fall back to the subquery scope).
  await onProgress(8, "resolving assessed kvl universe");
  const tU = Date.now();
  const universeKvls = await service.fetchAssessedKvlsFromAthena(
    iid,
    startDate,
    endDate,
  );
  console.log(
    `[benchmark handler] universe: ${universeKvls.length} kvls in ${
      Date.now() - tU
    }ms`,
  );

  if (!universeKvls.length) {
    await onProgress(100, "no records");
    return {
      csv: "",
      totalRecords: 0,
      fileName: buildFileName(iid, startDate, endDate, jobRow, rules),
    };
  }

  await onProgress(10, "running athena queries in parallel");

  // Exactly mirrors perf-benchmark-export.js's scope (which measured ~30-60s
  // end-to-end). Earlier I included startDate/endDate alongside kvlIds for
  // "partition pruning", but Athena's planner ended up producing a *worse*
  // plan with both predicates than with just the IN-list — turning 30s into
  // 10+ min. So either-or, matching the perf script:
  //   ≤25k kvls → IN-list only (covers the kvl set exactly; fast)
  //   >25k kvls → date-range subquery scope (Athena query-string cap)
  const IN_LIST_LIMIT = 25000;
  const scope =
    universeKvls.length <= IN_LIST_LIMIT
      ? { kvlIds: universeKvls }
      : { iid, startDate, endDate };
  console.log(
    `[benchmark handler] scope mode: ${
      scope.kvlIds ? `IN-list(${scope.kvlIds.length})` : "date-range subquery"
    }`,
  );
  let done = 0;
  const totalSteps = 7;
  const tQueriesStart = Date.now();
  const tick = async (label, t0, rows) => {
    done += 1;
    const pct = Math.min(95, 10 + Math.round((done / totalSteps) * 85));
    console.log(
      `[benchmark handler] ${label}: ${rows?.length ?? 0} rows in ${
        Date.now() - t0
      }ms`,
    );
    await onProgress(pct, label);
  };
  const timed = (label, fn) => {
    const t0 = Date.now();
    return fn()
      .catch((e) => {
        console.error(`[benchmark handler] ${label} err`, e);
        return [];
      })
      .then(async (r) => {
        await tick(label, t0, r);
        return r;
      });
  };

  const [
    mainRows,
    formInfoRows,
    mappingRows,
    flagRows,
    sentimentRows,
    externalRows,
    schemaResult,
  ] = await Promise.all([
    timed("main rows", () => service.fetchMain({ ...scope, mainWhere, having })),
    timed("form info", () => service.fetchFormInfo(scope)),
    timed("attributes", () => service.fetchAttributeMappings(scope)),
    timed("flags", () => service.fetchFlags(scope)),
    timed("sentiment", () => service.fetchSentiment(scope)),
    timed("external agents", () => service.fetchExternalAgent(scope)),
    timed("schema with weight", () => service.fetchSchemaScoreWithWeight(scope)),
  ]);

  console.log(
    `[benchmark handler] all athena queries done in ${Date.now() - tQueriesStart}ms`,
  );

  if (!mainRows.length) {
    await onProgress(100, "no records");
    return {
      csv: "",
      totalRecords: 0,
      fileName: buildFileName(iid, startDate, endDate, jobRow, rules),
    };
  }

  const rows = service.transformToCsv({
    mainRows,
    formInfoRows,
    mappingRows,
    flagRows,
    sentimentRows,
    externalRows,
    schemaResult,
  });

  await onProgress(97, "building csv");
  const csv = toCsv(rows);

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

module.exports = {
  run,
  validateParameters,
  normalizeParameters,
  buildFileName,
};

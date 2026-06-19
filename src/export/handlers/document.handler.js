/**
 * Document Metrics export handler — trend/benchmark style (single
 * typeCode + clean Parameters).
 *
 * One registered typeCode:
 *   document-metrics → honours whatever rules the FE sends. Empty rules
 *                       array = "Export All Records"; populated rules =
 *                       "Export Filtered Records". File name picks up
 *                       a `_FILTERED` suffix automatically when rules
 *                       are present (see buildFileName).
 *
 * Same run/normalize shape as benchmark + trend + whatsapp so the BullMQ
 * worker (src/export/exportWorker.js) picks it up by typeCode with no
 * extra plumbing.
 *
 * Output: per-document rows from `iceberg_db.new_document_master` via
 * Athena (matches the dashboard's StarRocks cohort identically), with
 * DocumentUrl / UserMobile / labels enriched from MySQL.
 *
 * Persisted `Parameters` JSON mirrors the trend handler exactly:
 *   { "rules": [...], "combinator": "and"|"or" }
 *
 * No `_variant`, no `partialId`, no `filters` envelope — those were
 * noise; the typeCode + the rules array are enough.
 *
 * ExportTypes row must exist in MySQL (seed manually):
 *   ExportTypeCode = 'document-metrics' → "Document Metrics"
 *
 * (The previously seeded 'document-metrics-filtered' row is now
 * orphaned — safe to leave in place or delete; the runtime no longer
 * targets it.)
 */

const DocumentExportService = require("../document/Document-Export-Service");
const AthenaService = require("../../services/athena.service");
const getConnection = require("../../../allservices/getConnection");
const getStarrocksConnection = require("../../../allservices/getStarrocksConnection");
const { toCsv } = require("../_shared/csv");
const { buildJobFileName } = require("../_shared/fileName");

const DOCUMENTS_MV = "mv_document_metrics_daily";

// Synchronously refresh the StarRocks MV that backs the Documents
// dashboard. The export itself reads Athena directly, so this hook only
// exists so the dashboard's TOTAL DOCUMENTS card lines up with the CSV
// row count when the user toggles back to it after exporting.
//
// Errors are caught and logged but do NOT fail the export.
//
// Note: REFRESH ... WITH SYNC MODE re-aggregates Iceberg's current
// snapshot into the MV. It does NOT pull new rows from MySQL into
// Iceberg — that's the ncx-task-runner ETL's job.
async function refreshDocumentsMv() {
  const tRefresh = Date.now();
  try {
    const conn = await getStarrocksConnection();
    await conn.queryAsync(`REFRESH MATERIALIZED VIEW ${DOCUMENTS_MV} WITH SYNC MODE`);
    console.log(
      `[document handler] refreshed ${DOCUMENTS_MV} in ${Date.now() - tRefresh}ms`,
    );
  } catch (err) {
    console.warn(
      `[document handler] MV refresh failed (continuing): ${err.message}`,
    );
  }
}

function validateParameters(body) {
  const errors = [];
  if (!body) errors.push("missing body");
  if (body && !body.institutionId) errors.push("institutionId required");
  if (body && !body.startDate) errors.push("startDate required");
  if (body && !body.endDate) errors.push("endDate required");
  if (body && body.rules && !Array.isArray(body.rules)) {
    errors.push("rules must be an array");
  }
  if (errors.length) {
    const e = new Error(errors.join("; "));
    e.statusCode = 400;
    throw e;
  }
}

// Trend-style minimal normalize: { rules, combinator } only. Accepts
// rules/combinator at top-level OR nested under `parameters` to match
// the trend handler's tolerance.
function normalizeParameters(body) {
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
    `[document handler] job=${jobRow.ExportJobId} iid=${iid} ` +
      `start=${startDate} end=${endDate} combinator=${combinator} ` +
      `rules=${JSON.stringify(rules)}`,
  );

  await onProgress(3, "refreshing dashboard MV (for count alignment)");
  await refreshDocumentsMv();

  await onProgress(5, "opening mysql connection");
  const sqlConn = await getConnection();
  const service = new DocumentExportService(new AthenaService(), sqlConn);

  // QueryBuilder envelope the export service expects is { combinator,
  // rules } — same shape DocumentReportService accepts on the live
  // report path. Null when there are no rules so buildWhere can skip
  // the QB branch entirely.
  const queryBuilder = rules.length ? { combinator, rules } : null;

  await onProgress(15, "querying iceberg per-document cohort (athena)");
  const tQuery = Date.now();
  const rawRows = await service.fetchRows({
    institutionId: iid,
    startDate,
    endDate,
    filters: {},
    queryBuilder,
  });
  console.log(
    `[document handler] cohort: ${rawRows.length} rows in ${Date.now() - tQuery}ms`,
  );

  if (!rawRows.length) {
    await onProgress(100, "no records");
    return {
      csv: "",
      totalRecords: 0,
      fileName: buildFileName(iid, startDate, endDate, jobRow, rules),
    };
  }

  await onProgress(90, "shaping rows");
  const rows = service.shape(rawRows);

  await onProgress(95, "building csv");
  const csv = toCsv(rows);

  return {
    csv,
    totalRecords: rows.length,
    fileName: buildFileName(iid, startDate, endDate, jobRow, rules),
  };
}

// File name picks up a `_FILTERED` suffix automatically when rules are
// present (matches the trend handler's convention). Single ExportTypes
// row covers both All and Filtered exports — the suffix is the
// distinguishing token in the downloads-panel file list.
function buildFileName(iid, startDate, endDate, jobRow, rules) {
  return buildJobFileName(jobRow, {
    iid,
    startDate,
    endDate,
    hasFilters: Array.isArray(rules) && rules.length > 0,
  });
}

module.exports = { run, validateParameters, normalizeParameters, buildFileName };

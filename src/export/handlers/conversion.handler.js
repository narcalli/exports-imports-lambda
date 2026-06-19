/**
 * Transaction Conversion export handler — benchmark/document style
 * (single typeCode, BullMQ-queued, S3-backed download).
 *
 * Registered typeCode:
 *   trx-conversion → per-transaction conversion rows for an institution over
 *                    a date range. No filters/rules; the cohort is fully
 *                    determined by institutionId + startDate + endDate.
 *
 * Same run/normalize shape as benchmark + trend + document + whatsapp so the
 * BullMQ worker (src/export/exportWorker.js) picks it up by typeCode with no
 * extra plumbing.
 *
 * Output: one CSV row per Transaction+Order pulled from Athena
 * (iceberg_db), identical in shape to the legacy `/trx-conversion-download`.
 *
 * ExportTypes row must exist in MySQL (seeded):
 *   ExportTypeCode = 'trx-conversion' → "Transaction Conversion"
 */

const AthenaService = require("../../services/athena.service");
const ConversionExportService = require("../conversion/Conversion-Export-Service");
const { toCsv } = require("../_shared/csv");
const { buildJobFileName } = require("../_shared/fileName");

function validateParameters(body) {
  const errors = [];
  if (!body) errors.push("missing body");
  if (body && !body.institutionId) errors.push("institutionId required");
  if (body && !body.startDate) errors.push("startDate required");
  if (body && !body.endDate) errors.push("endDate required");
  if (errors.length) {
    const e = new Error(errors.join("; "));
    e.statusCode = 400;
    throw e;
  }
}

// No rules/filters for this export — the cohort is just iid + date range.
// Return an empty params object so the parameters hash stays stable for dedup.
function normalizeParameters(_body) {
  return {};
}

async function run(jobRow, { onProgress }) {
  const iid = jobRow.InstitutionId;
  const startDate = jobRow.StartDate;
  const endDate = jobRow.EndDate;

  console.log(
    `[conversion handler] job=${jobRow.ExportJobId} iid=${iid} ` +
      `start=${startDate} end=${endDate}`,
  );

  const service = new ConversionExportService(new AthenaService());

  await onProgress(10, "querying transactions (athena)");
  const tQuery = Date.now();
  const rawRows = await service.fetchRows({
    institutionId: iid,
    startDate,
    endDate,
  });
  console.log(
    `[conversion handler] fetched ${rawRows.length} raw rows in ${
      Date.now() - tQuery
    }ms`,
  );

  if (!rawRows.length) {
    await onProgress(100, "no records");
    return {
      csv: "",
      totalRecords: 0,
      fileName: buildFileName(iid, startDate, endDate, jobRow),
    };
  }

  await onProgress(90, "shaping rows");
  const rows = service.transformToCsv(rawRows);

  await onProgress(95, "building csv");
  const csv = toCsv(rows);

  return {
    csv,
    totalRecords: rows.length,
    fileName: buildFileName(iid, startDate, endDate, jobRow),
  };
}

function buildFileName(iid, startDate, endDate, jobRow) {
  return buildJobFileName(jobRow, {
    iid,
    startDate,
    endDate,
    hasFilters: false,
  });
}

module.exports = { run, validateParameters, normalizeParameters, buildFileName };

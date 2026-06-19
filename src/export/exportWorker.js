"use strict";

const { EXPORT_STATUS } = require("./_shared/exportStatus");
const repo = require("./_shared/exportJobRepository");
const { uploadCsv } = require("./_shared/s3");
const { getHandler } = require("./handlers");

const ARCHIVE_DELAY_MS = 24*60 * 60 * 1000

async function processExportJob(job, exportJobQueue) {
  const { exportJobId } = job.data || {};
  const typeCode = job.name;

  if (!exportJobId) {
    throw new Error(`exportJob without exportJobId; payload=${JSON.stringify(job.data)}`);
  }

  if (typeCode === "archiveExport") {
    const conn = await repo.getConnection();
    await conn.queryAsync(
      `UPDATE ExportJob SET EntityStatusId = ? WHERE ExportJobId = ? AND EntityStatusId = ?`,
      [EXPORT_STATUS.ARCHIVED, exportJobId, EXPORT_STATUS.COMPLETED],
    );
    console.log(`[exportJob worker] archived ExportJob ${exportJobId}`);
    return;
  }

  const handler = getHandler(typeCode);
  if (!handler) {
    throw new Error(`No handler registered for export type '${typeCode}'`);
  }

  const conn = await repo.getConnection();
  await repo.markRunning(conn, { exportJobId, bullJobId: job.id });

  try {
    const row = await repo.getJobById(conn, exportJobId);
    if (!row) throw new Error(`ExportJob ${exportJobId} not found after markRunning`);

    const onProgress = async (pct, message) => {
      try {
        await repo.updateProgress(conn, { exportJobId, progress: pct, message });
      } catch (e) {
        console.warn("[exportJob worker] progress update failed:", e.message);
      }
    };

    const { csv, totalRecords, fileName } = await handler.run(row, { onProgress });

    let bucket = null, key = null, fileSize = 0;
    if (csv && csv.length) {
      const up = await uploadCsv({
        typeCode,
        institutionId: row.InstitutionId,
        exportJobId,
        fileName,
        body: csv,
      });
      bucket = up.bucket;
      key = up.key;
      fileSize = up.size;
    }

    await repo.markCompleted(conn, { exportJobId, totalRecords, fileName, bucket, key, fileSize });

    await exportJobQueue.add(
      "archiveExport",
      { exportJobId },
      { delay: ARCHIVE_DELAY_MS, jobId: `archive-${exportJobId}` },
    );
  } catch (err) {
    console.error(`[exportJob worker] job ${exportJobId} failed:`, err);
    await repo.markFailed(conn, { exportJobId, errorMessage: err.message || String(err) });
    throw err;
  }
}

module.exports = { processExportJob, ARCHIVE_DELAY_MS };

const getConnection = require("../../../allservices/getConnection");
const { EXPORT_STATUS, EXPORT_STATUS_NAME } = require("./exportStatus");

const TYPE_CACHE = new Map();

async function getExportTypeIdByCode(conn, code) {
  if (TYPE_CACHE.has(code)) return TYPE_CACHE.get(code);
  const rows = await conn.queryAsync(
    "SELECT ExportTypeId FROM ExportTypes WHERE ExportTypeCode = ? LIMIT 1",
    [code],
  );
  if (!rows.length) return null;
  const id = rows[0].ExportTypeId;
  TYPE_CACHE.set(code, id);
  return id;
}

async function getExportTypeCodeById(conn, id) {
  const rows = await conn.queryAsync(
    "SELECT ExportTypeCode FROM ExportTypes WHERE ExportTypeId = ? LIMIT 1",
    [id],
  );
  return rows[0]?.ExportTypeCode || null;
}

async function findReusableJob(conn, { institutionId, exportTypeId, parametersHash }) {
  const rows = await conn.queryAsync(
    `SELECT ExportJobId, EntityStatusId
       FROM ExportJob
      WHERE InstitutionId   = ?
        AND ExportTypeId    = ?
        AND ParametersHash  = ?
        AND EntityStatusId IN (?, ?)
      ORDER BY ExportJobId DESC
      LIMIT 1`,
    [
      institutionId,
      exportTypeId,
      parametersHash,
      EXPORT_STATUS.QUEUED,
      EXPORT_STATUS.RUNNING,
    ],
  );
  return rows[0] || null;
}

async function insertJob(conn, job) {
  const result = await conn.queryAsync(
    `INSERT INTO ExportJob (
       ExportTypeId, InstitutionId, AgentId, AgentName,
       StartDate, EndDate, Parameters, ParametersHash, EntityStatusId, QueuedAt
     ) VALUES (?, ?, ?, ?, ?, ?, CAST(? AS JSON), ?, ?, NOW())`,
    [
      job.exportTypeId,
      job.institutionId,
      job.agentId ?? null,
      job.agentName ?? null,
      job.startDate,
      job.endDate,
      JSON.stringify(job.parameters || {}),
      job.parametersHash,
      EXPORT_STATUS.QUEUED,
    ],
  );
  return result.insertId;
}

const SELECT_JOB_COLS = `
  j.ExportJobId, j.ExportTypeId, t.ExportTypeCode, t.ExportTypeName,
  j.InstitutionId, j.AgentId, j.AgentName,
  DATE_FORMAT(j.StartDate, '%Y-%m-%d') AS StartDate,
  DATE_FORMAT(j.EndDate,   '%Y-%m-%d') AS EndDate,
  j.Parameters,
  j.EntityStatusId, j.Progress, j.ProgressMessage,
  j.TotalRecords, j.FileName, j.S3Bucket, j.S3Key, j.FileSizeBytes,
  j.ErrorMessage, j.BullJobId, j.AttemptCount,
  CONVERT_TZ(j.QueuedAt,    '+00:00', '+05:30') AS QueuedAt,
  CONVERT_TZ(j.StartedAt,   '+00:00', '+05:30') AS StartedAt,
  CONVERT_TZ(j.CompletedAt, '+00:00', '+05:30') AS CompletedAt,
  CONVERT_TZ(j.AuditCreateTime, '+00:00', '+05:30') AS AuditCreateTime,
  CONVERT_TZ(j.AuditLastModifyTime, '+00:00', '+05:30') AS AuditLastModifyTime
`;

async function getJobById(conn, id) {
  const rows = await conn.queryAsync(
    `SELECT ${SELECT_JOB_COLS}
       FROM ExportJob j
       JOIN ExportTypes t ON t.ExportTypeId = j.ExportTypeId
      WHERE j.ExportJobId = ?`,
    [id],
  );
  return rows[0] || null;
}

async function listJobs(conn, { institutionId, exportTypeId, limit = 50, offset = 0 }) {
  const cappedLimit = Math.min(Math.max(parseInt(limit, 10) || 50, 1), 100);
  const safeOffset = Math.max(parseInt(offset, 10) || 0, 0);
  const rows = await conn.queryAsync(
    `SELECT ${SELECT_JOB_COLS}
       FROM ExportJob j
       JOIN ExportTypes t ON t.ExportTypeId = j.ExportTypeId
      WHERE j.InstitutionId = ?
        AND j.ExportTypeId  = ?
      ORDER BY j.AuditCreateTime DESC
      LIMIT ? OFFSET ?`,
    [institutionId, exportTypeId, cappedLimit, safeOffset],
  );
  return { rows, limit: cappedLimit, offset: safeOffset };
}

async function listAllJobs(conn, { institutionId, limit = 50, offset = 0 }) {
  const cappedLimit = Math.min(Math.max(parseInt(limit, 10) || 50, 1), 100);
  const safeOffset = Math.max(parseInt(offset, 10) || 0, 0);
  const rows = await conn.queryAsync(
    `SELECT ${SELECT_JOB_COLS}
       FROM ExportJob j
       JOIN ExportTypes t ON t.ExportTypeId = j.ExportTypeId
      WHERE j.InstitutionId = ?
      ORDER BY j.AuditCreateTime DESC
      LIMIT ? OFFSET ?`,
    [institutionId, cappedLimit, safeOffset],
  );
  return { rows, limit: cappedLimit, offset: safeOffset };
}

async function markRunning(conn, { exportJobId, bullJobId }) {
  const result = await conn.queryAsync(
    `UPDATE ExportJob
        SET EntityStatusId = ?, StartedAt = NOW(), BullJobId = ?,
            Progress = 0, ProgressMessage = 'started',
            AttemptCount = AttemptCount + 1
      WHERE ExportJobId = ?
        AND EntityStatusId = ?`,
    [
      EXPORT_STATUS.RUNNING,
      bullJobId || null,
      exportJobId,
      EXPORT_STATUS.QUEUED,
    ],
  );
  return result.affectedRows || 0;
}

async function updateProgress(conn, { exportJobId, progress, message }) {
  await conn.queryAsync(
    `UPDATE ExportJob
        SET Progress = ?, ProgressMessage = ?
      WHERE ExportJobId = ?
        AND EntityStatusId = ?`,
    [
      Math.max(0, Math.min(100, parseInt(progress, 10) || 0)),
      (message || "").slice(0, 255),
      exportJobId,
      EXPORT_STATUS.RUNNING,
    ],
  );
}

async function markCompleted(conn, { exportJobId, totalRecords, fileName, bucket, key, fileSize }) {
  await conn.queryAsync(
    `UPDATE ExportJob
        SET EntityStatusId = ?, Progress = 100,
            ProgressMessage = 'completed',
            CompletedAt = NOW(),
            TotalRecords = ?, FileName = ?,
            S3Bucket = ?, S3Key = ?, FileSizeBytes = ?
      WHERE ExportJobId = ?`,
    [
      EXPORT_STATUS.COMPLETED,
      totalRecords ?? null,
      fileName ?? null,
      bucket ?? null,
      key ?? null,
      fileSize ?? null,
      exportJobId,
    ],
  );
}

async function markFailed(conn, { exportJobId, errorMessage }) {
  await conn.queryAsync(
    `UPDATE ExportJob
        SET EntityStatusId = ?, CompletedAt = NOW(),
            ErrorMessage = ?
      WHERE ExportJobId = ?`,
    [EXPORT_STATUS.FAILED, String(errorMessage || "").slice(0, 65535), exportJobId],
  );
}

async function resetStaleRunning(conn, staleMinutes = 15) {
  const result = await conn.queryAsync(
    `UPDATE ExportJob
        SET EntityStatusId = ?,
            ProgressMessage = 'Reset after worker restart'
      WHERE EntityStatusId = ?
        AND StartedAt < NOW() - INTERVAL ? MINUTE`,
    [EXPORT_STATUS.QUEUED, EXPORT_STATUS.RUNNING, parseInt(staleMinutes, 10) || 15],
  );
  return result.affectedRows || 0;
}

function toDto(row) {
  if (!row) return null;
  let parameters = row.Parameters;
  if (typeof parameters === "string") {
    try { parameters = JSON.parse(parameters); } catch (_) { /* keep as string */ }
  }
  return {
    exportJobId: row.ExportJobId,
    exportTypeId: row.ExportTypeId,
    exportTypeCode: row.ExportTypeCode,
    exportTypeName: row.ExportTypeName,
    institutionId: row.InstitutionId,
    agentId: row.AgentId,
    agentName: row.AgentName,
    startDate: row.StartDate,
    endDate: row.EndDate,
    parameters,
    status: {
      id: row.EntityStatusId,
      name: EXPORT_STATUS_NAME[row.EntityStatusId] || null,
    },
    progress: row.Progress,
    progressMessage: row.ProgressMessage,
    totalRecords: row.TotalRecords,
    fileName: row.FileName,
    fileSizeBytes: row.FileSizeBytes,
    s3Bucket: row.S3Bucket,
    s3Key: row.S3Key,
    errorMessage: row.ErrorMessage,
    attemptCount: row.AttemptCount,
    queuedAt: row.QueuedAt,
    startedAt: row.StartedAt,
    completedAt: row.CompletedAt,
    createdAt: row.AuditCreateTime,
    updatedAt: row.AuditLastModifyTime,
  };
}

module.exports = {
  getConnection,
  getExportTypeIdByCode,
  getExportTypeCodeById,
  findReusableJob,
  insertJob,
  getJobById,
  listJobs,
  listAllJobs,
  markRunning,
  updateProgress,
  markCompleted,
  markFailed,
  resetStaleRunning,
  toDto,
};

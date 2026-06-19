"use strict";

const { processExportJob } = require("../../src/export/exportWorker");
const { makeArchiveShim } = require("../archiveShim");

/**
 * Export domain: turn one SQS record into the BullMQ-shaped `job` that the
 * ported worker expects, then run it.
 *
 * Accepted message bodies (both supported so the producer can migrate
 * gradually from BullMQ to SQS without a lockstep deploy):
 *   - SQS-native:  { "typeCode": "benchmark", "exportJobId": 123 }
 *   - BullMQ-ish:  { "name": "benchmark", "data": { "exportJobId": 123 } }
 */
async function processExportRecord(record) {
  const body = JSON.parse(record.body || "{}");

  const typeCode = body.typeCode || body.name;
  const exportJobId =
    body.exportJobId != null
      ? body.exportJobId
      : body.data && body.data.exportJobId;

  if (!typeCode) throw new Error(`export record missing typeCode; body=${record.body}`);
  if (exportJobId == null) throw new Error(`export record missing exportJobId; body=${record.body}`);

  const job = {
    name: typeCode,
    data: { exportJobId },
    id: record.messageId, // recorded as BullJobId in MySQL
  };

  await processExportJob(job, makeArchiveShim());
}

module.exports = processExportRecord;

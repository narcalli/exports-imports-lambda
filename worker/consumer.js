"use strict";

/**
 * Generic SQS -> Lambda consumer factory.
 *
 * Every domain (export now, import later) gets its own Lambda handler built
 * from this single factory. The factory owns the cross-cutting concerns that
 * are identical for every queue:
 *
 *   - iterate event.Records
 *   - per-record try/catch so one poison message can't fail the whole batch
 *   - SQS partial-batch response (ReportBatchItemFailures): only the records
 *     that actually threw are returned for redelivery; everything that
 *     succeeded is deleted. The event source mapping must be created with
 *     FunctionResponseTypes=ReportBatchItemFailures for this to take effect.
 *
 * @param {(record: object) => Promise<void>} processRecord  domain processor
 * @param {{ domain: string }} opts
 * @returns {(event: object) => Promise<{ batchItemFailures: {itemIdentifier:string}[] }>}
 */
function makeSqsConsumer(processRecord, { domain }) {
  return async function handler(event) {
    const records = (event && event.Records) || [];
    console.log(`[${domain}] received ${records.length} record(s)`);

    const batchItemFailures = [];

    for (const record of records) {
      const id = record.messageId;
      try {
        await processRecord(record);
        console.log(`[${domain}] record ${id} ok`);
      } catch (err) {
        console.error(`[${domain}] record ${id} failed:`, err && err.stack ? err.stack : err);
        batchItemFailures.push({ itemIdentifier: id });
      }
    }

    // SQS reads this to decide which messages to redeliver. Empty array = all ok.
    return { batchItemFailures };
  };
}

module.exports = { makeSqsConsumer };

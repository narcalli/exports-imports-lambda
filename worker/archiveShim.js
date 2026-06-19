"use strict";

const { SQSClient, SendMessageCommand } = require("@aws-sdk/client-sqs");

/**
 * BullMQ-compatible queue shim.
 *
 * `processExportJob(job, exportJobQueue)` (src/export/exportWorker.js) was
 * written against a BullMQ Queue and only ever calls `.add(name, data, opts)`
 * on it — to schedule the 24h `archiveExport` follow-up after a job completes.
 * This shim lets the ported worker run unchanged inside Lambda.
 *
 * Behaviour:
 *   - archiveExport: BullMQ supported a 24h `delay`. SQS DelaySeconds maxes out
 *     at 900s (15 min), so a 24h delay CANNOT be expressed as an SQS message.
 *     For now we log and skip — leaving the job COMPLETED (still downloadable)
 *     rather than archiving it. The real archive sweep moves to EventBridge
 *     Scheduler / a daily cron Lambda (see README "Archive" section). This is
 *     intentionally a no-op, not a silent drop.
 *   - anything else: re-enqueued onto the same SQS queue (DelaySeconds capped
 *     at 900s) so future fan-out style follow-ups keep working.
 */

const REGION = process.env.AWS_REGION || "ap-south-1";
const QUEUE_URL = process.env.EXPORT_QUEUE_URL;
const MAX_SQS_DELAY_SECONDS = 900;

const sqs = new SQSClient({ region: REGION });

function makeArchiveShim() {
  return {
    async add(name, data, opts = {}) {
      if (name === "archiveExport") {
        console.log(
          `[archiveShim] archiveExport for job ${data && data.exportJobId} deferred ` +
            `(SQS cannot delay 24h; handled by EventBridge/cron sweep — see README)`,
        );
        return { id: opts.jobId || null, skipped: true };
      }

      if (!QUEUE_URL) {
        console.warn(`[archiveShim] EXPORT_QUEUE_URL not set; dropping re-enqueue of '${name}'`);
        return { id: null, skipped: true };
      }

      const delaySeconds = Math.min(
        Math.floor((opts.delay || 0) / 1000),
        MAX_SQS_DELAY_SECONDS,
      );

      await sqs.send(
        new SendMessageCommand({
          QueueUrl: QUEUE_URL,
          MessageBody: JSON.stringify({ typeCode: name, ...data }),
          DelaySeconds: delaySeconds,
        }),
      );
      return { id: opts.jobId || null, skipped: false };
    },
  };
}

module.exports = { makeArchiveShim };

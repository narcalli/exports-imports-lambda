# exports-imports-lambda

Standalone **SQS → Lambda** worker for async exports (and, later, imports),
migrated out of `dashboard-backend`'s BullMQ `exportJob` worker.

Notion plan: [Standalone Gateway + Lambda for Exports & Imports](https://app.notion.com/p/384bb83239398067b773c1be5ca8215c)

## Flow

```
dashboard-backend  POST /exports/:typeCode
  ├─ insert ExportJob row (MySQL)        ◄── MySQL stays system of record
  └─ SQS.sendMessage({typeCode, exportJobId})   (replaces exportJobQueue.add)
            │
            ▼ (event source mapping, auto-invoke)
   exports-queue-mumbai ──► Lambda exports-worker-mumbai (index.exportHandler)
            │                 └─ worker/domains/export.js → src/export/exportWorker.processExportJob
            │                       markRunning → Athena → S3 (ncx-prod-exports) → markCompleted (MySQL)
            └─ on repeated failure ──► exports-dlq-mumbai
```

The heavy code under `src/export/**`, `src/services/athena.service.js` and
`allservices/**` is **copied verbatim** from `dashboard-backend` (same relative
paths, so its `require()`s resolve unchanged). Only two files were adapted for
Lambda: `allservices/mysqlconnect.js` (dropped the incident-log dep + the 30s
keep-alive timer, smaller pool) and the env wiring. Re-syncing from upstream is
a straight file copy.

## Layout

```
index.js               Lambda entrypoints (one handler per domain)
worker/
  consumer.js          generic SQS consumer factory (batch-item-failures)
  registry.js          domain -> record-processor map  ← add imports here
  domains/export.js    SQS record -> processExportJob
  archiveShim.js       BullMQ-compatible .add() shim (24h archive deferred)
src/export/**          ported worker + handlers + Export-Service files (verbatim)
src/services/          athena.service.js (verbatim)
allservices/           getConnection, mysqlconnect (adapted), getStarrocksConnection, redisclient
dashboard/             conversationWithTransaction.js (conversion export, verbatim)
deploy.sh              create/update queue+DLQ+role+function+mapping (ap-south-1)
scripts/               build-env-json, send-test-message, invoke-local
```

## Deploy

```bash
cp .env.example .env     # fill MySQL / Athena / Redis values
npm install
npm run deploy           # idempotent; creates everything in ap-south-1 (Mumbai)
npm run send benchmark 123   # send a test message; watch CloudWatch logs
```

Resources created (account auto-detected via STS):
- SQS `exports-queue-mumbai` (visibility 960s, redrive → DLQ after 3 receives)
- SQS `exports-dlq-mumbai`
- IAM role `exports-imports-lambda-role-mumbai` (logs + SQS consume + S3 RW on `ncx-prod-exports`)
- Lambda `exports-worker-mumbai` (nodejs18.x, 1024 MB, 900s, handler `index.exportHandler`)
- Event source mapping (batchSize 1, ReportBatchItemFailures)

### Credentials note
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_REGION` are **reserved**
Lambda env names and are not set. S3 upload therefore uses the **execution
role** (granted in `deploy.sh`). Athena uses its own `AWS_ACCESS_KEY_ID_ATHENA`
/ `AWS_SECRET_ACCESS_KEY_ATHENA` (custom names, set from `.env`).

## Producer change in dashboard-backend (Phase 1 cutover)

`exports.controller.js` `#enqueue()` swaps the BullMQ call for SQS — the REST
API, dedup and `202` response are otherwise unchanged:

```js
const { SQSClient, SendMessageCommand } = require("@aws-sdk/client-sqs");
const sqs = new SQSClient({ region: process.env.AWS_REGION || "ap-south-1" });
await sqs.send(new SendMessageCommand({
  QueueUrl: process.env.EXPORT_QUEUE_URL,
  MessageBody: JSON.stringify({ typeCode, exportJobId }),
}));
```

## Adding the import queue later

1. `worker/domains/import.js` → `module.exports = async (record) => {...}`
2. `worker/registry.js` → add `import: require("./domains/import")`
3. `index.js` → `exports.importHandler = makeSqsConsumer(registry.import, { domain: "import" })`
4. `deploy.sh` → add an `imports-queue-mumbai` + a second function/mapping (or
   parameterise the existing block over a domain list)

Consumer, IAM, logging and batch-failure handling are all shared.

## Known follow-ups (out of scope for this migration step)

- **24h archive** (`COMPLETED → ARCHIVED`): BullMQ used a 24h delayed job. SQS
  max delay is 900s, so `archiveShim` currently logs-and-skips. Move to an
  EventBridge Scheduler one-time schedule or a daily cron Lambda + S3 lifecycle.
- **Stale-running reset**: handled by SQS visibility timeout + DLQ redrive; the
  old `resetStaleRunning` boot sweep is not needed.
- **MySQL connectivity**: assumes the DB is reachable over TLS from Lambda (as
  dashboard-backend reaches it). If it is VPC-private, add VPC config + consider
  RDS Proxy.
- **Large exports**: for results that risk the 15-min wall, switch handlers to
  Athena `UNLOAD ... TO 's3://...'` so Athena writes the CSV directly.
```
# exports-imports-lambda

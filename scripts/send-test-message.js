"use strict";

// Sends one test message to the export queue, which triggers the Lambda.
// Usage:
//   node scripts/send-test-message.js <typeCode> <exportJobId>
//   node scripts/send-test-message.js benchmark 123
//
// Reads the queue URL from EXPORT_QUEUE_URL (env) or derives it from the
// account + ap-south-1 + exports-queue-mumbai.

const { SQSClient, SendMessageCommand } = require("@aws-sdk/client-sqs");

const REGION = process.env.AWS_REGION || "ap-south-1";
const typeCode = process.argv[2] || "benchmark";
const exportJobId = parseInt(process.argv[3] || "1", 10);

(async () => {
  let queueUrl = process.env.EXPORT_QUEUE_URL;
  if (!queueUrl) {
    const { STSClient, GetCallerIdentityCommand } = require("@aws-sdk/client-sts");
    let acct;
    try {
      const sts = new STSClient({ region: REGION });
      acct = (await sts.send(new GetCallerIdentityCommand({}))).Account;
    } catch (e) {
      console.error("Set EXPORT_QUEUE_URL or install @aws-sdk/client-sts:", e.message);
      process.exit(1);
    }
    queueUrl = `https://sqs.${REGION}.amazonaws.com/${acct}/exports-queue-mumbai`;
  }

  const sqs = new SQSClient({ region: REGION });
  const body = JSON.stringify({ typeCode, exportJobId });
  const res = await sqs.send(
    new SendMessageCommand({ QueueUrl: queueUrl, MessageBody: body }),
  );
  console.log(`sent -> ${queueUrl}`);
  console.log(`body: ${body}`);
  console.log(`messageId: ${res.MessageId}`);
})();

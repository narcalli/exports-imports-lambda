"use strict";

// Run the export handler locally against a fake SQS event (uses your local
// .env). Useful to smoke-test the full pipeline without deploying.
//   node scripts/invoke-local.js <typeCode> <exportJobId>

try {
  require("dotenv").config();
} catch (_) {
  /* dotenv optional; rely on real env if absent */
}

const { exportHandler } = require("../index");

const typeCode = process.argv[2] || "benchmark";
const exportJobId = parseInt(process.argv[3] || "1", 10);

const event = {
  Records: [
    {
      messageId: "local-test-1",
      body: JSON.stringify({ typeCode, exportJobId }),
    },
  ],
};

(async () => {
  const res = await exportHandler(event, {});
  console.log("handler result:", JSON.stringify(res));
  process.exit(0); // redis/mysql keep the loop alive otherwise
})().catch((e) => {
  console.error(e);
  process.exit(1);
});

const {
  S3Client,
  PutObjectCommand,
  GetObjectCommand,
} = require("@aws-sdk/client-s3");

const REGION = process.env.AWS_REGION || "ap-south-1";
const BUCKET = "ncx-prod-exports";

function buildClient() {
  // The export bucket (ncx-prod-exports) lives in a different AWS account
  // (742019417498) from this Lambda (003339248139). AWS_ACCESS_KEY_ID is a
  // reserved Lambda env name (it holds the execution-role creds), so the
  // cross-account S3 creds are supplied under EXPORT_S3_* and preferred here.
  // Falls back to AWS_ACCESS_KEY_ID for dashboard-backend / local use, where
  // behaviour is unchanged.
  const accessKeyId =
    process.env.EXPORT_S3_ACCESS_KEY_ID || process.env.AWS_ACCESS_KEY_ID;
  const secretAccessKey =
    process.env.EXPORT_S3_SECRET_ACCESS_KEY || process.env.AWS_SECRET_ACCESS_KEY;

  const config = { region: REGION };
  if (accessKeyId && secretAccessKey) {
    config.credentials = { accessKeyId, secretAccessKey };
  }
  return new S3Client(config);
}

const s3 = buildClient();

function buildKey({ typeCode, institutionId, exportJobId, fileName }) {
  const now = new Date();
  const yyyy = now.getUTCFullYear();
  const mm = String(now.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(now.getUTCDate()).padStart(2, "0");
  return `${typeCode}/iid=${institutionId}/${yyyy}/${mm}/${dd}/${exportJobId}_${fileName}`;
}

async function uploadCsv({ typeCode, institutionId, exportJobId, fileName, body }) {
  const Key = buildKey({ typeCode, institutionId, exportJobId, fileName });
  await s3.send(
    new PutObjectCommand({
      Bucket: BUCKET,
      Key,
      Body: body,
      ContentType: "text/csv; charset=utf-8",
      ContentDisposition: `attachment; filename="${fileName}"`,
    }),
  );
  return { bucket: BUCKET, key: Key, size: Buffer.byteLength(body) };
}

async function streamDownload({ bucket, key, res, fileName }) {
  const result = await s3.send(new GetObjectCommand({ Bucket: bucket, Key: key }));
  res.setHeader("Content-Type", "text/csv; charset=utf-8");
  res.setHeader(
    "Content-Disposition",
    `attachment; filename="${fileName || key.split("/").pop()}"`,
  );
  result.Body.pipe(res);
}

module.exports = { uploadCsv, streamDownload, BUCKET };

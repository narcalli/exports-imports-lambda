"use strict";

// Reads .env and emits Lambda's --environment JSON: { "Variables": {...} }.
// Excludes names Lambda reserves (it rejects them on update). EXPORT_QUEUE_URL
// is injected from the deploy shell, not from .env.

const fs = require("fs");
const path = require("path");

const RESERVED = new Set([
  "AWS_ACCESS_KEY_ID",
  "AWS_SECRET_ACCESS_KEY",
  "AWS_SESSION_TOKEN",
  "AWS_REGION",
  "AWS_DEFAULT_REGION",
]);

const envPath = path.join(__dirname, "..", ".env");
const text = fs.readFileSync(envPath, "utf8");

const vars = {};
for (const raw of text.split("\n")) {
  const line = raw.trim();
  if (!line || line.startsWith("#")) continue;
  const eq = line.indexOf("=");
  if (eq === -1) continue;
  const key = line.slice(0, eq).trim();
  let val = line.slice(eq + 1).trim();
  if (
    (val.startsWith('"') && val.endsWith('"')) ||
    (val.startsWith("'") && val.endsWith("'"))
  ) {
    val = val.slice(1, -1);
  }
  if (!key || RESERVED.has(key)) continue;
  vars[key] = val;
}

if (process.env.EXPORT_QUEUE_URL) {
  vars.EXPORT_QUEUE_URL = process.env.EXPORT_QUEUE_URL;
}

process.stdout.write(JSON.stringify({ Variables: vars }));

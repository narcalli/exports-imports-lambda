const crypto = require("crypto");

function canonicalize(value) {
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map(canonicalize);
  const out = {};
  for (const k of Object.keys(value).sort()) out[k] = canonicalize(value[k]);
  return out;
}

function parametersHash({ exportTypeId, institutionId, startDate, endDate, parameters }) {
  const payload = JSON.stringify({
    t: exportTypeId,
    i: institutionId,
    s: startDate,
    e: endDate,
    p: canonicalize(parameters || {}),
  });
  return crypto.createHash("sha256").update(payload).digest("hex");
}

module.exports = { canonicalize, parametersHash };

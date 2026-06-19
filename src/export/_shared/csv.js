function escapeCell(value) {
  if (value === null || value === undefined) return "";
  const s = String(value);
  if (/[",\r\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function toCsv(rows) {
  if (!rows || rows.length === 0) return "";
  const seen = new Set();
  const columns = [];
  for (const r of rows) {
    for (const k of Object.keys(r)) {
      if (!seen.has(k)) {
        seen.add(k);
        columns.push(k);
      }
    }
  }
  const lines = [columns.map(escapeCell).join(",")];
  for (const r of rows) {
    lines.push(columns.map((c) => escapeCell(r[c])).join(","));
  }
  return lines.join("\n") + "\n";
}

module.exports = { toCsv };

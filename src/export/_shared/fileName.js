"use strict";

// Sanitize an ExportType display name into a filesystem-safe token:
// "Whatsapp Metrics (Filtered)" → "Whatsapp_Metrics_Filtered"
function sanitizeName(name) {
  if (!name) return "";
  return String(name)
    .replace(/[^A-Za-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

// Build the export filename from the job row's ExportTypeName so the file
// matches the "Report Name" column the user sees in the downloads panel.
// Falls back to ExportTypeCode if the display name is missing.
//
// Extra tokens (e.g. WhatsApp's grain tag) are joined after the base name
// in the order supplied. hasFilters appends `_FILTERED` but only when the
// resolved base name doesn't already carry a "filtered" marker (the
// whatsapp-metrics-filtered type already encodes it in ExportTypeName).
function buildJobFileName(jobRow, { iid, startDate, endDate, hasFilters = false, extras = [] } = {}) {
  const base =
    sanitizeName(jobRow && jobRow.ExportTypeName) ||
    sanitizeName(jobRow && jobRow.ExportTypeCode) ||
    "EXPORT";

  const alreadyFiltered = /filtered/i.test(base);
  const suffix = hasFilters && !alreadyFiltered ? "_FILTERED" : "";

  const tail = (extras || []).filter(Boolean).join("_");
  const tailPart = tail ? `_${tail}` : "";

  const s = String(startDate).slice(0, 10);
  const e = String(endDate).slice(0, 10);

  return `${base}${suffix}${tailPart}_${iid}_${s}_${e}.csv`;
}

module.exports = { sanitizeName, buildJobFileName };

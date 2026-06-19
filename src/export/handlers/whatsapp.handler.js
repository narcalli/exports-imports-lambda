/**
 * WhatsApp Metrics export handlers — assessment-benchmark style.
 *
 * Two registered typeCodes:
 *   whatsapp-metrics            → "Export All Records" (rules stripped at normalize)
 *   whatsapp-metrics-filtered   → "Export Filtered Records" (rules kept)
 *
 * Same run/normalize shape as benchmark + trend so the BullMQ worker can pick
 * it up with no extra plumbing.
 *
 * Output: aggregated funnel rows — exactly what the live /whatsapp-reports/table
 * endpoint returns for the same filters. The totals therefore match the
 * on-screen table column-for-column.
 *
 * ExportTypes rows must exist in MySQL (already seeded):
 *   ExportTypeCode = 'whatsapp-metrics'           → "Whatsapp Metrics"
 *   ExportTypeCode = 'whatsapp-metrics-filtered'  → "Whatsapp Metrics (Filtered)"
 */

const WhatsappExportService = require("../whatsapp/Whatsapp-Export-Service");
const AthenaService = require("../../services/athena.service");
const getConnection = require("../../../allservices/getConnection");
const { toCsv } = require("../_shared/csv");
const { buildJobFileName } = require("../_shared/fileName");

function validate(body) {
  const errors = [];
  if (!body) errors.push("missing body");
  if (body && !body.institutionId) errors.push("institutionId required");
  if (body && !body.startDate) errors.push("startDate required");
  if (body && !body.endDate) errors.push("endDate required");
  if (body && body.rules && !Array.isArray(body.rules)) {
    errors.push("rules must be an array");
  }
  if (errors.length) {
    const e = new Error(errors.join("; "));
    e.statusCode = 400;
    throw e;
  }
}

// `keepRules=false` strips the QueryBuilder rules so "Export All Records"
// can't accidentally be filtered. partialId always flows through.
//
// granularity is now honoured: granularity=10 (Campaign) switches the
// fetch path so the CSV reflects per-(campaign_run_log_id, user) rows
// instead of the default per-(template, user). drillDown is still
// dropped — the export emits all funnel volumes on every row, the chart's
// funnel-state unpivot doesn't change what's exported.
//
// Coerce numerics + sort rule keys so the parameters-hash is stable across
// clicks. A FE sending granularity:"8" one click and granularity:8 the next
// would otherwise hash differently and bypass dedup.
function normalize(body, { keepRules }) {
  const rules = keepRules && Array.isArray(body.rules)
    ? body.rules.map(canonicalRule)
    : [];
  const combinator =
    String(body.combinator || "and").toLowerCase() === "or" ? "or" : "and";
  const partialId = body.partialId != null && body.partialId !== "" ? Number(body.partialId) : null;
  const granularityNum = body.granularity != null && body.granularity !== ""
    ? Number(body.granularity)
    : null;
  // mobileSearch is a top-of-console wildcard filter, not part of the
  // QueryBuilder rules. Both export variants honour it — even "Export
  // All Records" narrows by mobile when the search box is active, so the
  // CSV matches the on-screen cohort. Trim once here so the parameters-
  // hash stays stable across cosmetic whitespace differences.
  const mobileSearch = body.mobileSearch != null
    ? String(body.mobileSearch).trim()
    : "";
  return {
    rules,
    combinator,
    partialId: Number.isFinite(partialId) ? partialId : null,
    granularity: Number.isFinite(granularityNum) ? granularityNum : null,
    mobileSearch: mobileSearch || null,
    _variant: keepRules ? "filtered" : "all",
  };
}

// Sort keys + coerce numeric-looking values so two identical rule objects
// hash the same way regardless of how the FE happened to serialise them.
function canonicalRule(r) {
  if (!r || typeof r !== "object") return r;
  const out = {};
  for (const k of Object.keys(r).sort()) {
    let v = r[k];
    if (typeof v === "string" && v !== "" && !isNaN(Number(v))) v = Number(v);
    out[k] = v;
  }
  return out;
}

async function run(jobRow, { onProgress }) {
  const params =
    typeof jobRow.Parameters === "string"
      ? JSON.parse(jobRow.Parameters || "{}")
      : jobRow.Parameters || {};

  const iid = jobRow.InstitutionId;
  const startDate = jobRow.StartDate;
  const endDate = jobRow.EndDate;

  console.log(
    `[whatsapp handler] job=${jobRow.ExportJobId} variant=${params._variant || "?"} ` +
      `iid=${iid} start=${startDate} end=${endDate} ` +
      `granularity=${params.granularity} drillDown=${params.drillDown} ` +
      `combinator=${params.combinator} mobileSearch=${params.mobileSearch || ""} ` +
      `rules=${JSON.stringify(params.rules)}`,
  );

  await onProgress(5, "opening connections");
  // Athena (Iceberg) for the per-message anchor + per-user funnel, MySQL
  // for the small dimension lookups (UserMaster, TransactionMasters,
  // session-template ids). Mirrors Benchmark-Export-Service's wiring.
  const sqlConn = await getConnection();
  const service = new WhatsappExportService(new AthenaService(), sqlConn);

  // 1. Pull the template_category pseudo-rule out of the QueryBuilder rules
  //    so the session gate runs at the same 3-mode semantics as the on-screen
  //    report (Template / Session / All).
  // 2. Re-inject mobileSearch as a synthetic user_mobile_number IN [...] rule
  //    so buildWhere's prefix-LIKE path catches it. Same machinery the live
  //    report endpoint uses — keeps the CSV scope identical to the cohort
  //    the user was looking at when they clicked Export.
  const { templateCategory, rules: cleanRules } =
    service.extractTemplateCategory(params.rules || []);
  const rules = service.injectMobileSearch(cleanRules, params.mobileSearch);

  // Granularity dispatch:
  //   7  → per-(template_type, user, phone)   — Template Message Type
  //   8  → per-(template, user, phone)        — default Template path
  //   10 → per-(campaign_run_log_id, user, phone) — raw-fact Campaign path
  //   11 → per-(template, user, phone)        — Consolidated falls through
  //                                             to template grain so the
  //                                             CSV keeps row-level detail
  // drillDown is intentionally ignored — every CSV row carries all 7
  // funnel volumes, so the chart's funnel-state unpivot doesn't change
  // what's exported.
  const granularity = Number(params.granularity);
  const grain =
    granularity === 10 ? "campaign" :
    granularity === 7  ? "template_type" :
                         "template";

  await onProgress(15, `querying iceberg per-user funnel — grain=${grain} (athena)`);
  const tQuery = Date.now();
  const fetchArgs = {
    institutionId: iid,
    startDate,
    endDate,
    rules,
    templateCategory,
    combinator: params.combinator,
  };
  let rawRows;
  try {
    if (grain === "campaign") {
      rawRows = await service.fetchPerCampaignUserRows(fetchArgs);
    } else if (grain === "template_type") {
      rawRows = await service.fetchPerTemplateTypeUserRows(fetchArgs);
    } else {
      rawRows = await service.fetchPerUserRows(fetchArgs);
    }
  } catch (err) {
    // Date-range guard / invalid params surface as 400 so the BullMQ
    // worker marks the job failed with a usable message.
    if (err.statusCode === 400) throw err;
    throw err;
  }
  console.log(`[whatsapp handler] funnel (${grain}): ${rawRows.length} rows in ${Date.now() - tQuery}ms`);

  if (!rawRows.length) {
    await onProgress(100, "no records");
    return {
      csv: "",
      totalRecords: 0,
      fileName: buildFileName(iid, startDate, endDate, jobRow, grain),
    };
  }

  const userIds = [...new Set(rawRows.map((r) => r.user_id).filter((id) => id != null))];

  let rows;
  if (grain === "campaign") {
    // Campaign mode: per-message anchor often leaves user_id NULL on
    // outbound bulk sends (events log phone, not user master id). Run a
    // phone → user_master_id backfill BEFORE the user-keyed lookups so
    // any anchor-missed user_id gets resolved by phone — then the
    // downstream UserName + TransactionId lookups have a full user_id
    // set to work against.
    const phones      = [...new Set(rawRows.map((r) => r.phone_number).filter((p) => p && String(p).trim()))];
    const runIds      = [...new Set(rawRows.map((r) => r.campaign_run_log_id).filter((v) => v != null))];
    const templateIds = [...new Set(rawRows.map((r) => r.message_template_id).filter((v) => v != null))];

    await onProgress(60, `resolving ${phones.length} phones → users`);
    const tPhones = Date.now();
    const phoneMap = await service.fetchUsersByPhone(phones);
    console.log(`[whatsapp handler] phone→user backfill: ${phoneMap.size} of ${phones.length} phones in ${Date.now() - tPhones}ms`);

    // Mutate rawRows in-place: fill user_id when anchor missed it so the
    // downstream lookups + shape see a consistent dataset.
    for (const r of rawRows) {
      if ((r.user_id == null || Number(r.user_id) <= 0) && r.phone_number) {
        const hit = phoneMap.get(String(r.phone_number));
        if (hit && hit.userId != null) r.user_id = hit.userId;
      }
    }
    const resolvedUserIds = [...new Set(rawRows.map((r) => r.user_id).filter((id) => id != null && Number(id) > 0))];

    await onProgress(70, `resolving ${resolvedUserIds.length} users + ${runIds.length} campaigns`);
    const tLookup = Date.now();
    const [userNameMap, txnMap, campaignMap, templateNameMap] = await Promise.all([
      service.fetchUserNames(resolvedUserIds),
      service.fetchTransactionIds(resolvedUserIds),
      service.fetchCampaignLabels(runIds),
      service.fetchTemplateNames(templateIds),
    ]);

    // Latest-run-per-master gate — same default the chart uses
    // (#keepLatestRunPerMaster). For each CbotCampaignMasterId we keep
    // only rows whose campaign_run_log_id is the highest. Without this
    // a multi-run campaign (e.g. Bcom Runs 6863/6864/6865/6866) shows up
    // in the CSV as 4× its on-screen count — the chart hides the older
    // tabs but the CSV would still include them and look wrong against
    // the chart's totals.
    const masterByRun = new Map();
    for (const [runId, info] of campaignMap.entries()) {
      if (info && info.masterId != null) masterByRun.set(runId, Number(info.masterId));
    }
    const maxRunByMaster = new Map();
    for (const r of rawRows) {
      const runId    = r.campaign_run_log_id != null ? Number(r.campaign_run_log_id) : null;
      const masterId = runId != null ? masterByRun.get(runId) : null;
      if (masterId == null || runId == null) continue;
      const cur = maxRunByMaster.get(masterId);
      if (cur == null || runId > cur) maxRunByMaster.set(masterId, runId);
    }
    const beforeCount = rawRows.length;
    rawRows = rawRows.filter((r) => {
      const runId    = r.campaign_run_log_id != null ? Number(r.campaign_run_log_id) : null;
      const masterId = runId != null ? masterByRun.get(runId) : null;
      // Untagged rows (no master id resolved) pass through — same safety
      // valve the chart's #keepLatestRunPerMaster uses.
      if (masterId == null || runId == null) return true;
      return runId === maxRunByMaster.get(masterId);
    });
    console.log(`[whatsapp handler] latest-run-per-master gate: ${rawRows.length} of ${beforeCount} rows kept`);
    // Merge the phone-backfilled names into the UserName map for any
    // user_id that came from the phone lookup but didn't appear in
    // UserMaster's id-keyed result (cheap belt-and-suspenders).
    for (const [, hit] of phoneMap) {
      if (hit.userId != null && hit.userName && !userNameMap.has(hit.userId)) {
        userNameMap.set(hit.userId, hit.userName);
      }
    }
    console.log(`[whatsapp handler] lookups: names=${userNameMap.size} txns=${txnMap.size} campaigns=${campaignMap.size} templates=${templateNameMap.size} in ${Date.now() - tLookup}ms`);

    await onProgress(90, "shaping rows (campaign)");
    rows = service.shapeCampaign(rawRows, userNameMap, txnMap, campaignMap, templateNameMap);
  } else if (grain === "template_type") {
    // Three parallel MySQL trips for Template-Type mode: user names,
    // transaction ids, type labels (Text / Image / Video / …).
    const typeIds = [...new Set(rawRows.map((r) => r.message_template_type_id).filter((v) => v != null))];
    await onProgress(70, `resolving ${userIds.length} users + ${typeIds.length} template types`);
    const tLookup = Date.now();
    const [userNameMap, txnMap, typeNameMap] = await Promise.all([
      service.fetchUserNames(userIds),
      service.fetchTransactionIds(userIds),
      service.fetchTemplateTypeNames(typeIds),
    ]);
    console.log(`[whatsapp handler] lookups: names=${userNameMap.size} txns=${txnMap.size} types=${typeNameMap.size} in ${Date.now() - tLookup}ms`);

    await onProgress(90, "shaping rows (template type)");
    rows = service.shapeTemplateType(rawRows, userNameMap, txnMap, typeNameMap);
  } else {
    // Two parallel MySQL trips for the default template-grain path.
    await onProgress(70, `resolving ${userIds.length} user names + transactions`);
    const tLookup = Date.now();
    const [userNameMap, txnMap] = await Promise.all([
      service.fetchUserNames(userIds),
      service.fetchTransactionIds(userIds),
    ]);
    console.log(`[whatsapp handler] lookups: names=${userNameMap.size} txns=${txnMap.size} of ${userIds.length} users in ${Date.now() - tLookup}ms`);

    await onProgress(90, "shaping rows");
    rows = service.shape(rawRows, userNameMap, txnMap);
  }

  await onProgress(95, "building csv");
  const csv = toCsv(rows);

  return {
    csv,
    totalRecords: rows.length,
    fileName: buildFileName(iid, startDate, endDate, jobRow, grain),
  };
}

// File name uses the ExportTypeName ("Whatsapp Metrics" / "Whatsapp Metrics
// (Filtered)") shown in the downloads panel, plus the granularity tag so
// the three grains are still distinguishable without opening the CSV.
// `grain` accepts the string form ("campaign" / "template_type" /
// "template") or a legacy boolean (true = campaign).
function buildFileName(iid, startDate, endDate, jobRow, grain) {
  let grainTag = "TEMPLATE";
  if (grain === "campaign" || grain === true)        grainTag = "CAMPAIGN";
  else if (grain === "template_type")                 grainTag = "TEMPLATE_TYPE";
  return buildJobFileName(jobRow, {
    iid,
    startDate,
    endDate,
    extras: [grainTag],
  });
}

const whatsappMetricsAll = {
  validateParameters: validate,
  normalizeParameters: (body) => normalize(body, { keepRules: false }),
  run,
  // The downloads-panel preview of the file name doesn't know granularity
  // until the job runs, so we default to TEMPLATE here — the actual file
  // produced by `run` carries the correct CAMPAIGN / TEMPLATE_TYPE tag
  // when granularity is 10 / 7.
  buildFileName: (iid, s, e) =>
    buildFileName(iid, s, e, { ExportTypeName: "Whatsapp Metrics" }, "template"),
};

const whatsappMetricsFiltered = {
  validateParameters: validate,
  normalizeParameters: (body) => normalize(body, { keepRules: true }),
  run,
  buildFileName: (iid, s, e) =>
    buildFileName(iid, s, e, { ExportTypeName: "Whatsapp Metrics (Filtered)" }, "template"),
};

module.exports = { whatsappMetricsAll, whatsappMetricsFiltered };

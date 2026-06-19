/**
 * Athena-backed export service for the WhatsApp Metrics view.
 *
 * Mirrors the structure of Benchmark-Export-Service:
 *   - constructor accepts an AthenaService (or builds the default one)
 *   - buildWhere() splits QueryBuilder rules into { mainWhere, having }
 *   - the main fetch composes a per-message anchor CTE, an optional
 *     qualifying-templates CTE (HAVING gate at TEMPLATE level so the
 *     export's row scope matches what the chart shows), and a final
 *     per-(template, user, phone) projection.
 *
 * Why Athena (not StarRocks/MV):
 *   The on-screen report reads `mv_whatsapp_metrics_daily` via StarRocks
 *   for ~ms latency. The export runs out-of-band in BullMQ and can
 *   tolerate the 5-30s polling Athena needs — and matching the existing
 *   assessment-benchmark export keeps the export-infra surface uniform.
 *   Tables referenced are the same Iceberg tables the MV is built from;
 *   numbers reconcile to the report at refresh boundaries.
 *
 * Output: one CSV row per (MessageTemplateId, UserId, PhoneNumber):
 *   MessageTemplateId, MessageTemplateName,
 *   UserId, UserName, PhoneNumber, TransactionId,
 *   Triggered, Sent, Delivered, Read, Clicked, Failed, Skipped,
 *   FirstEventAt, LastEventAt
 *
 * Fill policy: numeric cells default to 0; missing UserName / PhoneNumber /
 * TransactionId fall back to "-" so the reader can still trace the row.
 */

const AthenaService = require("../../services/athena.service");

const OPERATOR_MAP = {
  eq: "=",   EQ: "=",
  gt: ">",   GT: ">",
  lt: "<",   LT: "<",
  gte: ">=", GTE: ">=",
  lte: "<=", LTE: "<=",
  ne: "!=",  NE: "!=",
};

// user_activity_id buckets — same definitions the MV uses. Kept literal
// (not imported from the service) so the export's SQL is self-contained
// and a UAT remap doesn't silently change exported totals.
const FUNNEL_BUCKETS = {
  triggered: [13],
  sent:      [6],
  delivered: [5],
  read:      [3],
  clicked:   [4, 11, 12, 24],
  failed:    [10, 22],
  skipped:   [15, 16, 19, 21, 23],
};

// HAVING evaluates against the per-template subquery's aliases
// (c_triggered, c_sent, ... = SUM of c_* events for the message). Volume
// rules collapse to SUM(<bucket>); rate rules compose volumes the same
// way #measureExpr does in the live service so a `delivery_rate > 80`
// filter matches the chart.
const HAVING_EXPRS_ANCHORED = {
  triggered:        "SUM(c_triggered)",
  sent:             "SUM(c_sent)",
  delivered:        "SUM(c_delivered)",
  read:             'SUM(c_read)',
  clicked:          "SUM(c_clicked)",
  failed:           "SUM(c_failed)",
  skipped:          "SUM(c_skipped)",
  // FE field "Trigger Failed" — maps to the failed bucket (UAT 10 + 22)
  // for parity with the live service's #measureExpr.
  triggered_failed: "SUM(c_failed)",
  // Distinct user phones at the qualifying-dim grain (template / campaign).
  // The anchored CTE already deduplicates events into one row per message
  // with user_mobile_number resolved, so COUNT(DISTINCT) here yields the
  // distinct-users count the chart's `unique_users` measure reports.
  unique_users:     "COUNT(DISTINCT user_mobile_number)",
  delivery_rate:    "(ROUND(SUM(c_delivered) / NULLIF(SUM(c_sent),       0) * 100, 2))",
  read_rate:        "(ROUND(SUM(c_read)      / NULLIF(SUM(c_delivered),  0) * 100, 2))",
  ctr:              "(ROUND(SUM(c_clicked)   / NULLIF(SUM(c_delivered),  0) * 100, 2))",
  failure_rate:     "(ROUND(SUM(c_failed)    / NULLIF(SUM(c_triggered),  0) * 100, 2))",
};

// Campaign mode reads raw fact directly (no MV anchor) — same path the live
// service's #buildRawFactSql uses, so per-event totals reconcile to the
// legacy /askengage/notification-report endpoint. HAVING expressions here
// inline the per-UAT SUMs (no c_* aliases available).
const sumBucket = (ids) => `SUM(CASE WHEN whl.user_activity_id IN (${ids.join(",")}) THEN 1 ELSE 0 END)`;
const HAVING_EXPRS_RAW = {
  triggered:        sumBucket(FUNNEL_BUCKETS.triggered),
  sent:             sumBucket(FUNNEL_BUCKETS.sent),
  delivered:        sumBucket(FUNNEL_BUCKETS.delivered),
  read:             sumBucket(FUNNEL_BUCKETS.read),
  clicked:          sumBucket(FUNNEL_BUCKETS.clicked),
  failed:           sumBucket(FUNNEL_BUCKETS.failed),
  skipped:          sumBucket(FUNNEL_BUCKETS.skipped),
  triggered_failed: sumBucket(FUNNEL_BUCKETS.failed),
  // Distinct-phone count over raw events for the qualifying-dim grain.
  // Matches the live service's COUNT(DISTINCT whl.user_mobile_number).
  unique_users:     "COUNT(DISTINCT whl.user_mobile_number)",
  delivery_rate:    `(ROUND(${sumBucket(FUNNEL_BUCKETS.delivered)} / NULLIF(${sumBucket(FUNNEL_BUCKETS.sent)},      0) * 100, 2))`,
  read_rate:        `(ROUND(${sumBucket(FUNNEL_BUCKETS.read)}      / NULLIF(${sumBucket(FUNNEL_BUCKETS.delivered)}, 0) * 100, 2))`,
  ctr:              `(ROUND(${sumBucket(FUNNEL_BUCKETS.clicked)}   / NULLIF(${sumBucket(FUNNEL_BUCKETS.delivered)}, 0) * 100, 2))`,
  failure_rate:     `(ROUND(${sumBucket(FUNNEL_BUCKETS.failed)}    / NULLIF(${sumBucket(FUNNEL_BUCKETS.triggered)}, 0) * 100, 2))`,
};

// ROC pseudo-fields require week buckets + window functions to compute
// baseline / continuous % change. The export grain is per-(user, …) with
// no week split, so the predicates can't be evaluated. Listed here so
// they're dropped EXPLICITLY (with a log warning) rather than silently —
// the user gets a hint that the filter didn't apply.
const ROC_FIELDS = new Set([
  "pct_change_gain",
  "pct_change_decline",
  "pct_cts_change_gain",
  "pct_cts_change_decline",
]);

// Hard cap on date range. Wide windows on high-volume institutions can
// blow past Athena's per-query scan cost / 30-min timeout and surface as
// a failed BullMQ job with a cryptic error. Reject early with a clear
// message so the user knows to narrow the date range.
const MAX_EXPORT_DAYS = 90;

const sqlStr = (v) => `'${String(v).replace(/'/g, "''")}'`;
const sqlNumStrict = (v) => {
  const n = Number(v);
  if (!Number.isFinite(n)) throw new Error(`Non-numeric value: ${v}`);
  return String(n);
};
// Tolerates ",10" / "10," shapes the FE sometimes ships from a min/max
// widget where only one box was filled. Returns the last non-empty number
// or 0 on totally-unparseable input — invariant: never NaN into SQL.
const sqlNumLenient = (v) => {
  if (v === null || v === undefined) return "0";
  const parts = String(v).split(",").map((x) => x.trim()).filter(Boolean);
  const pick  = parts.length ? parts[parts.length - 1] : "";
  const n = Number(pick);
  return Number.isFinite(n) ? String(n) : "0";
};

const fmtListNumbers = (value) => {
  const arr = Array.isArray(value) ? value : String(value).split(",");
  return arr
    .map((v) => Number(String(v).trim()))
    .filter((n) => Number.isFinite(n))
    .join(",");
};

const dateOnly = (v) => {
  if (v instanceof Date && !isNaN(v)) {
    const y = v.getFullYear();
    const m = String(v.getMonth() + 1).padStart(2, "0");
    const d = String(v.getDate()).padStart(2, "0");
    return `${y}-${m}-${d}`;
  }
  const s = String(v || "").trim();
  const m = s.match(/^(\d{4}-\d{2}-\d{2})/);
  if (!m) throw new Error(`Invalid date: ${v}`);
  return m[1];
};

class WhatsappExportService {
  /**
   * @param {AthenaService} athena   Iceberg query runner (defaults to a fresh AthenaService)
   * @param {object}        sqlConn  MySQL connection (UserMaster / TransactionMasters / session-template lookups)
   */
  constructor(athena, sqlConn) {
    this.athena  = athena || new AthenaService();
    this.sqlConn = sqlConn;
  }

  /* ------------------------------------------------------------------ */
  /*                          MASTER LOOKUPS                            */
  /* ------------------------------------------------------------------ */
  // Session templates by MessageTemplateMaster.IsSessionTemplates — same
  // source of truth the live report uses (legacy getTemplateInteractionReport.js
  // also keys off this column). Using the column instead of name-pattern
  // matching keeps the export reconciled with the on-screen WhatsApp
  // Message Metrics table.
  async #sessionTemplateIds() {
    try {
      const rows = await this.sqlConn.queryAsync(
        `SELECT MessageTemplateId FROM MessageTemplateMaster
         WHERE IsSessionTemplates IS NOT NULL;`,
      );
      return rows.map((r) => Number(r.MessageTemplateId)).filter((n) => !isNaN(n));
    } catch (err) {
      console.error("[whatsapp export] session-template lookup failed:", err.message);
      return [];
    }
  }

  /* ------------------------------------------------------------------ */
  /*                        PSEUDO-RULE EXTRACTION                       */
  /* ------------------------------------------------------------------ */
  // Mirror of WhatsappReportsService.#extractTemplateCategory. Pulled
  // out before SQL build so it never reaches buildWhere as a column
  // predicate. Default mode is "template" (outbound funnel only).
  extractTemplateCategory(rules) {
    let templateCategory = "template";
    if (!Array.isArray(rules)) return { templateCategory, rules: [] };
    const filtered = rules.filter((r) => {
      if (r && r.field === "template_category") {
        const v = String(r.value ?? "").toLowerCase();
        if (v === "session" || v === "all" || v === "template") templateCategory = v;
        return false;
      }
      return true;
    });
    return { templateCategory, rules: filtered };
  }

  // Wildcard mobile search → synthetic user_mobile_number IN [...] rule.
  // Reuses buildWhere's mobile path so the partial-prefix LIKE semantics
  // match the live service.
  injectMobileSearch(rules, mobileSearch) {
    if (mobileSearch == null) return Array.isArray(rules) ? rules : [];
    const raw = Array.isArray(mobileSearch)
      ? mobileSearch
      : String(mobileSearch).split(",");
    const tokens = raw.map((v) => String(v || "").trim()).filter((v) => v.length > 0);
    if (!tokens.length) return Array.isArray(rules) ? rules : [];
    return [
      ...(Array.isArray(rules) ? rules : []),
      { field: "user_mobile_number", operator: "IN", value: tokens },
    ];
  }

  /* ------------------------------------------------------------------ */
  /*                          RULE → WHERE / HAVING                      */
  /* ------------------------------------------------------------------ */
  // Returns { mainWhere, having } — same shape Benchmark-Export-Service
  // returns. mainWhere filters at the raw-fact row level. having filters
  // at the qualifying-dim level (template / template_type / campaign_run)
  // so the export's row scope matches the chart's HAVING gate exactly.
  //
  // Group-aware: QueryBuilder rule trees of the form
  //   { combinator: "or", rules: [
  //       { field, operator, value },
  //       { combinator: "and", rules: [...] },
  //   ]}
  // are walked recursively. Each nested group produces a parenthesized
  // SQL fragment with its own combinator. WHERE and HAVING fragments are
  // collected separately and re-joined under the top-level combinator —
  // mixed dim/aggregate groups inside the same OR are not perfectly
  // expressible across the two layers and degrade to "AND of WHERE-clauses
  // AND HAVING-clauses", which is the safest semantics (a row passes only
  // if both groups pass).
  //
  // mode: "anchored" (default; reads c_* aliases of the anchored CTE) or
  // "raw" (Campaign mode; raw fact, HAVING inlines per-UAT SUMs).
  buildWhere(rules, combinator = "and", mode = "anchored") {
    const havingMap = mode === "raw" ? HAVING_EXPRS_RAW : HAVING_EXPRS_ANCHORED;
    const { mainParts, havingParts } = this.#walkRuleGroup(
      { combinator, rules },
      havingMap,
      /*topLevel=*/true,
    );
    const topComb = String(combinator).toUpperCase() === "OR" ? "OR" : "AND";
    return {
      mainWhere: mainParts.length   ? `AND (${mainParts.join(` ${topComb} `)})`   : "",
      having:    havingParts.length ? `HAVING ${havingParts.join(` ${topComb} `)}` : "",
    };
  }

  // Recursive walker. Returns { mainParts, havingParts } — both arrays of
  // SQL fragments. When called on a nested group, it parenthesises each
  // group's joined fragments so the inner combinator is preserved.
  #walkRuleGroup(group, havingMap, topLevel = false) {
    const mainParts   = [];
    const havingParts = [];
    if (!group || !Array.isArray(group.rules) || group.rules.length === 0) {
      return { mainParts, havingParts };
    }
    const comb = String(group.combinator || "and").toUpperCase() === "OR" ? "OR" : "AND";

    for (const r of group.rules) {
      if (!r) continue;
      // Nested group
      if (Array.isArray(r.rules)) {
        const child = this.#walkRuleGroup(r, havingMap, /*topLevel=*/false);
        if (child.mainParts.length) {
          const joined = child.mainParts.join(` ${String(r.combinator || "and").toUpperCase() === "OR" ? "OR" : "AND"} `);
          mainParts.push(`(${joined})`);
        }
        if (child.havingParts.length) {
          const joinedH = child.havingParts.join(` ${String(r.combinator || "and").toUpperCase() === "OR" ? "OR" : "AND"} `);
          havingParts.push(`(${joinedH})`);
        }
        continue;
      }

      // Leaf rule
      const fragment = this.#compileLeafRule(r, havingMap);
      if (!fragment) continue;
      if (fragment.kind === "main")   mainParts.push(fragment.sql);
      if (fragment.kind === "having") havingParts.push(fragment.sql);
    }

    // Nested groups don't need the top-level "AND (...)" wrapper — the
    // caller handles that. Returning the raw parts lets the parent decide
    // how to combine.
    if (topLevel) return { mainParts, havingParts };
    // For nested groups we still return both halves unjoined; parent
    // wraps them with its own combinator above.
    return { mainParts, havingParts };
  }

  // One leaf rule → either { kind: "main", sql } or { kind: "having", sql }
  // or null (silently dropped). Reused by both top-level and nested walks.
  #compileLeafRule(r, havingMap) {
    const field = r && r.field;
    const op    = OPERATOR_MAP[String((r && r.operator) || "").toLowerCase()] || null;
    const rawOp = String((r && r.operator) || "").toUpperCase();
    const value = r && r.value;
    if (!field) return null;
    if ((value === undefined || value === null || value === "") &&
        rawOp !== "IS_NULL" && rawOp !== "IS_NOT_NULL") {
      return null;
    }

    // ROC pseudo-fields can't be expressed in per-user export grain —
    // log once so the user knows their filter didn't apply.
    if (ROC_FIELDS.has(field)) {
      console.warn(`[whatsapp export] ROC field '${field}' is not exportable (no week buckets at this grain); dropping rule.`);
      return null;
    }

    if (havingMap[field]) {
      if (!op) return null;
      return { kind: "having", sql: `${havingMap[field]} ${op} ${sqlNumLenient(value)}` };
    }

    if (field === "message_template_id") {
      return { kind: "main", sql: this.#dim("whl.message_template_id", rawOp, value) };
    }
    if (field === "whatsapp_message_type_id") {
      // Despite the field name, the FE dropdown sources MessageTemplateTypeMaster
      // (Text / Image / Video / …). The value is a MessageTemplateTypeId, so
      // route to the template-type subquery — matches the live service's
      // #buildMessageTypeWhere.
      return { kind: "main", sql: this.#templateTypeSubquery(rawOp, value) };
    }
    if (field === "message_template_type_id") {
      return { kind: "main", sql: this.#templateTypeSubquery(rawOp, value) };
    }
    if (field === "user_activity_id") {
      return { kind: "main", sql: this.#dim("whl.user_activity_id", rawOp, value) };
    }
    if (field === "inst_event_journey_master_id") {
      // Mirrors the live service's #drillMap(7) JOIN chain, collapsed to a
      // template-id subquery so the predicate plugs into the same
      // mainWhere shape (no extra JOIN at the outer query level).
      //   journey → wa_inst_event_journey_master → event_journey_template_artefact
      //          → connect_bot_master_message_template_master → message_template_id
      return { kind: "main", sql: this.#journeyTemplateSubquery(rawOp, value) };
    }
    if (field === "user_mobile_number") {
      const arr = Array.isArray(value) ? value : String(value).split(",");
      const escaped = arr.map((v) => String(v).replace(/'/g, "''"));
      if (rawOp === "IN" || rawOp === "EQ") {
        const parts = escaped.map((v) => `whl.user_mobile_number LIKE '%${v}%'`);
        return { kind: "main", sql: parts.length === 1 ? parts[0] : `(${parts.join(" OR ")})` };
      }
      if (rawOp === "NOT_IN" || rawOp === "NE") {
        return { kind: "main", sql: escaped.map((v) => `whl.user_mobile_number NOT LIKE '%${v}%'`).join(" AND ") };
      }
    }
    // Unknown field → silently dropped, same as live service.
    return null;
  }

  #dim(col, op, value) {
    if (op === "IS_NULL")     return `${col} IS NULL`;
    if (op === "IS_NOT_NULL") return `${col} IS NOT NULL`;
    if (op === "IN")          return `${col} IN (${fmtListNumbers(value)})`;
    if (op === "NOT_IN")      return `${col} NOT IN (${fmtListNumbers(value)})`;
    if (op === "BETWEEN") {
      const [a, b] = Array.isArray(value) ? value : String(value).split(",");
      return `${col} BETWEEN ${sqlNumStrict(a)} AND ${sqlNumStrict(b)}`;
    }
    const sqlOp = OPERATOR_MAP[op.toLowerCase()] || OPERATOR_MAP[op];
    if (!sqlOp) return "1=1";
    const n = Number(value);
    return Number.isFinite(n)
      ? `${col} ${sqlOp} ${n}`
      : `${col} ${sqlOp} ${sqlStr(value)}`;
  }

  // Template type lives on message_template_master, not the fact. Subquery
  // form lets the anchor CTE still partition-prune on date / institution.
  #templateTypeSubquery(op, value) {
    if (op === "IS_NULL" || op === "IS_NOT_NULL") {
      return this.#dim("whl.message_template_id", op, value);
    }
    const typePredicate = this.#dim("message_template_type_id", op, value);
    return `whl.message_template_id IN (
      SELECT message_template_id
      FROM iceberg_db.message_template_master
      WHERE ${typePredicate}
        AND message_template_id IS NOT NULL
    )`;
  }

  // Journey filter — walks the same chain the live service's #drillMap(7)
  // does, collapsed to "templates that belong to one of these journeys".
  // Letting the predicate be a template-id subquery means the outer fact
  // query keeps its partition pruning on date/institution and doesn't need
  // a 4-way LEFT JOIN per row.
  //
  //   journey → wa_inst_event_journey_master (joins by journey_inst_master_id)
  //           → event_journey_template_artefact (joins by inst_event_journey_master_id)
  //           → connect_bot_master_message_template_master (joins by cbmt_id)
  //           → message_template_id
  #journeyTemplateSubquery(op, value) {
    if (op === "IS_NULL" || op === "IS_NOT_NULL") {
      return this.#dim("whl.message_template_id", op, value);
    }
    const journeyPredicate = this.#dim("jim.journey_inst_master_id", op, value);
    return `whl.message_template_id IN (
      SELECT cbmt.message_template_id
      FROM iceberg_db.connect_bot_master_message_template_master cbmt
      JOIN iceberg_db.event_journey_template_artefact ejta
        ON ejta.connect_bot_master_message_template_master_id = cbmt.cbmt_id
      JOIN iceberg_db.wa_inst_event_journey_master iejm
        ON iejm.inst_event_journey_master_id = ejta.inst_event_journey_master_id
      JOIN iceberg_db.wa_journey_inst_master jim
        ON jim.journey_inst_master_id = iejm.journey_inst_master_id
      WHERE ${journeyPredicate}
        AND cbmt.message_template_id IS NOT NULL
    )`;
  }

  /* ------------------------------------------------------------------ */
  /*                       TEMPLATE-CATEGORY GATE                        */
  /* ------------------------------------------------------------------ */
  // Mirrors WhatsappReportsService.#buildBase's session gate so the
  // export scope == the chart's cohort:
  //   template (default) → exclude IsSessionTemplates IS NOT NULL templates
  //   session            → INCLUDE ONLY IsSessionTemplates IS NOT NULL
  //   all                → no session filter
  #templateCategoryGate(templateCategory, sessionTemplateIds, alias = "a") {
    const list = (sessionTemplateIds || []).map(Number).filter((n) => !isNaN(n));
    if (templateCategory === "session") {
      return list.length
        ? `AND ${alias}.message_template_id IN (${list.join(",")})`
        : `AND 1 = 0`;
    }
    if (templateCategory !== "all") {
      return list.length
        ? `AND (${alias}.message_template_id IS NULL OR ${alias}.message_template_id NOT IN (${list.join(",")}))`
        : "";
    }
    return "";
  }

  /* ------------------------------------------------------------------ */
  /*             ATHENA: PER-MESSAGE ANCHOR + PER-USER FUNNEL            */
  /* ------------------------------------------------------------------ */
  // Athena query shape — three logical stages, single round-trip:
  //
  //   anchored             : per-message anchor (one row per
  //                          whatsapp_message_id). Resolves real dim
  //                          values via MAX(CASE WHEN col > 0 ...),
  //                          normalises mobile (REPLACE '+'), computes
  //                          per-event c_* sums for each funnel bucket.
  //                          Mirrors mv_whatsapp_metrics_daily inline.
  //
  //   qualifying_templates : (only when HAVING rules are present)
  //                          template ids that pass the per-template
  //                          HAVING gate — the chart's filter, applied
  //                          at the same grain.
  //
  //   final SELECT         : per-(template, user, phone) projection,
  //                          joined to message_template_master for the
  //                          template name, gated to the qualifying
  //                          template set if HAVING is present.
  async fetchPerUserRows({ institutionId, startDate, endDate, rules, templateCategory = "template", combinator = "and" }) {
    const iid   = sqlNumStrict(institutionId);
    const start = dateOnly(startDate);
    const end   = dateOnly(endDate);
    this.#assertWindowOk(start, end);

    const sessionTemplateIds = await this.#sessionTemplateIds();
    const { mainWhere, having } = this.buildWhere(rules, combinator);

    const tcGateAnchored = this.#templateCategoryGate(templateCategory, sessionTemplateIds, "anchored");
    const tcGateA        = this.#templateCategoryGate(templateCategory, sessionTemplateIds, "a");

    const cohortIds = [...new Set([
      ...FUNNEL_BUCKETS.triggered,
      ...FUNNEL_BUCKETS.skipped,
      ...FUNNEL_BUCKETS.failed.filter((id) => id === 22),
    ])].join(",");

    const anchorCte = `
      anchored AS (
        SELECT
          whl.institution_id,
          whl.whatsapp_message_id,
          MAX(CASE WHEN whl.message_template_id      > 0 THEN whl.message_template_id      END) AS message_template_id,
          MAX(CASE WHEN whl.whatsapp_message_type_id > 0 THEN whl.whatsapp_message_type_id END) AS whatsapp_message_type_id,
          MAX(whl.user_id)                                                                     AS user_id,
          REPLACE(MAX(CASE WHEN whl.user_mobile_number IS NOT NULL AND whl.user_mobile_number <> ''
                           THEN whl.user_mobile_number END), '+', '')                          AS user_mobile_number,
          MIN(whl.audit_created_time)                                                          AS min_event_time,
          MAX(whl.audit_created_time)                                                          AS max_event_time,
          COALESCE(
            MIN(CASE WHEN whl.user_activity_id = 13 THEN whl.audit_created_time END),
            MIN(whl.audit_created_time)
          )                                                                                    AS bucket_time,
          MAX(CASE WHEN whl.user_activity_id IN (${cohortIds}) THEN 1 ELSE 0 END)              AS in_cohort,
          SUM(CASE WHEN whl.user_activity_id IN (${FUNNEL_BUCKETS.triggered.join(",")}) THEN 1 ELSE 0 END) AS c_triggered,
          SUM(CASE WHEN whl.user_activity_id IN (${FUNNEL_BUCKETS.sent.join(",")})      THEN 1 ELSE 0 END) AS c_sent,
          SUM(CASE WHEN whl.user_activity_id IN (${FUNNEL_BUCKETS.delivered.join(",")}) THEN 1 ELSE 0 END) AS c_delivered,
          SUM(CASE WHEN whl.user_activity_id IN (${FUNNEL_BUCKETS.read.join(",")})      THEN 1 ELSE 0 END) AS c_read,
          SUM(CASE WHEN whl.user_activity_id IN (${FUNNEL_BUCKETS.clicked.join(",")})   THEN 1 ELSE 0 END) AS c_clicked,
          SUM(CASE WHEN whl.user_activity_id IN (${FUNNEL_BUCKETS.failed.join(",")})    THEN 1 ELSE 0 END) AS c_failed,
          SUM(CASE WHEN whl.user_activity_id IN (${FUNNEL_BUCKETS.skipped.join(",")})   THEN 1 ELSE 0 END) AS c_skipped
        FROM iceberg_db.whatsapp_hook_log_master whl
        WHERE whl.institution_id          = ${iid}
          AND whl.whatsapp_message_id IS NOT NULL
          AND whl.whatsapp_message_id    <> ''
          AND whl.message_template_id     > 0
          AND whl.audit_created_time >= TIMESTAMP '${start} 00:00:00'
          AND whl.audit_created_time <= TIMESTAMP '${end} 23:59:59'
          ${mainWhere}
        GROUP BY whl.institution_id, whl.whatsapp_message_id
      )
    `;

    const qualifyingCte = having
      ? `,
      qualifying_templates AS (
        SELECT message_template_id
        FROM anchored
        WHERE in_cohort = 1
          AND message_template_id IS NOT NULL
          AND DATE(bucket_time) BETWEEN DATE '${start}' AND DATE '${end}'
          ${this.#templateCategoryGate(templateCategory, sessionTemplateIds, "anchored").replace(/anchored\./g, "")}
        GROUP BY message_template_id
        ${having}
      )`
      : "";

    const qualifyingFilter = having
      ? `AND a.message_template_id IN (SELECT message_template_id FROM qualifying_templates)`
      : "";

    const sql = `
      WITH ${anchorCte}${qualifyingCte}
      SELECT
        a.message_template_id                  AS message_template_id,
        mtm.message_template_name              AS message_template_name,
        a.user_id                              AS user_id,
        a.user_mobile_number                   AS phone_number,
        SUM(a.c_triggered)                     AS triggered,
        SUM(a.c_sent)                          AS sent,
        SUM(a.c_delivered)                     AS delivered,
        SUM(a.c_read)                          AS "read",
        SUM(a.c_clicked)                       AS clicked,
        SUM(a.c_failed)                        AS failed,
        SUM(a.c_skipped)                       AS skipped,
        MIN(a.min_event_time)                  AS first_event_at,
        MAX(a.max_event_time)                  AS last_event_at
      FROM anchored a
      LEFT JOIN (
        SELECT message_template_id, MAX(message_template_name) AS message_template_name
        FROM iceberg_db.message_template_master
        GROUP BY message_template_id
      ) mtm ON mtm.message_template_id = a.message_template_id
      WHERE a.in_cohort               = 1
        AND a.message_template_id IS NOT NULL
        AND DATE(a.bucket_time) BETWEEN DATE '${start}' AND DATE '${end}'
        ${tcGateA}
        ${qualifyingFilter}
      GROUP BY a.message_template_id, mtm.message_template_name, a.user_id, a.user_mobile_number
      ORDER BY a.message_template_id, triggered DESC
    `;

    console.log(`[whatsapp export] athena SQL prepared (mainWhere=${!!mainWhere}, having=${!!having}, tc=${templateCategory})`);
    return this.athena.executeQuery(sql);
  }

  /* ------------------------------------------------------------------ */
  /*                        DATE-RANGE GUARD                             */
  /* ------------------------------------------------------------------ */
  // Hard-fail wide windows up front so the user gets an actionable error
  // ("narrow your date range") instead of an Athena timeout 25 minutes
  // into a BullMQ job. Threshold mirrors the same shape the on-screen
  // report uses for Message Instance granularity (14 days) — picked
  // 90 days here because export workloads can tolerate more scan.
  #assertWindowOk(startDate, endDate) {
    const ms = new Date(endDate) - new Date(startDate);
    if (!Number.isFinite(ms) || ms < 0) {
      const err = new Error("Invalid date range (endDate before startDate)");
      err.statusCode = 400;
      throw err;
    }
    const days = ms / (24 * 60 * 60 * 1000);
    if (days > MAX_EXPORT_DAYS) {
      const err = new Error(`Export window exceeds ${MAX_EXPORT_DAYS}-day cap (got ${Math.round(days)} days). Please narrow the date range.`);
      err.statusCode = 400;
      throw err;
    }
  }

  /* ------------------------------------------------------------------ */
  /*           ATHENA: PER-CAMPAIGN-RUN x USER (Campaign granularity)    */
  /* ------------------------------------------------------------------ */
  // Uses the same per-message anchor pattern as the Template path
  // (NOT the live chart's raw-fact #buildRawFactSql).
  //
  // Why anchored instead of raw fact:
  //   - On many campaigns whl.user_id is NULL/0 on the events themselves
  //     (the writer logs the phone number, not the user master id, for
  //     outbound bulk sends). Grouping raw events by whl.user_id collapses
  //     every user into a single NULL-user row per run — the CSV ends up
  //     showing 1 row per run with UserId="-".
  //   - The anchor `MAX(CASE WHEN user_id > 0 THEN user_id END)` lifts a
  //     real user_id from ANY event of the message (the Sent webhook
  //     often carries it even when Triggered doesn't). Same trick the
  //     MV uses and the Template export path uses.
  //   - HAVING gate uses HAVING_EXPRS_ANCHORED — same SUM(c_*) shape
  //     as Template, so a `delivered > 100` rule means the same thing
  //     in both grains.
  //
  // Output grain: per-(campaign_run_log_id, user_id, phone_number). The
  // export INTENTIONALLY does NOT apply #keepLatestRunPerMaster — the
  // chart hides older runs by default, but the CSV is the source of
  // truth and includes every run that had messages in scope.
  async fetchPerCampaignUserRows({ institutionId, startDate, endDate, rules, templateCategory = "template", combinator = "and" }) {
    const iid   = sqlNumStrict(institutionId);
    const start = dateOnly(startDate);
    const end   = dateOnly(endDate);
    this.#assertWindowOk(start, end);

    const sessionTemplateIds = await this.#sessionTemplateIds();
    const { mainWhere, having } = this.buildWhere(rules, combinator, "anchored");

    const tcGateAnchored = this.#templateCategoryGate(templateCategory, sessionTemplateIds, "anchored");
    const tcGateA        = this.#templateCategoryGate(templateCategory, sessionTemplateIds, "a");

    const cohortIds = [...new Set([
      ...FUNNEL_BUCKETS.triggered,
      ...FUNNEL_BUCKETS.skipped,
      ...FUNNEL_BUCKETS.failed.filter((id) => id === 22),
    ])].join(",");

    // Per-message anchor — same shape as the Template path's CTE, plus the
    // campaign_run_log_id is resolved via MAX(CASE WHEN > 0). One message
    // belongs to exactly one run, so MAX is safe.
    const anchorCte = `
      anchored AS (
        SELECT
          whl.institution_id,
          whl.whatsapp_message_id,
          MAX(CASE WHEN whl.campaign_run_log_id > 0 THEN whl.campaign_run_log_id END) AS campaign_run_log_id,
          MAX(CASE WHEN whl.message_template_id > 0 THEN whl.message_template_id END) AS message_template_id,
          MAX(CASE WHEN whl.user_id             > 0 THEN whl.user_id             END) AS user_id,
          REPLACE(MAX(CASE WHEN whl.user_mobile_number IS NOT NULL AND whl.user_mobile_number <> ''
                           THEN whl.user_mobile_number END), '+', '')                 AS user_mobile_number,
          MIN(whl.audit_created_time)                                                 AS min_event_time,
          MAX(whl.audit_created_time)                                                 AS max_event_time,
          COALESCE(MIN(CASE WHEN whl.user_activity_id = 13 THEN whl.audit_created_time END),
                   MIN(whl.audit_created_time))                                       AS bucket_time,
          MAX(CASE WHEN whl.user_activity_id IN (${cohortIds}) THEN 1 ELSE 0 END)     AS in_cohort,
          SUM(CASE WHEN whl.user_activity_id IN (${FUNNEL_BUCKETS.triggered.join(",")}) THEN 1 ELSE 0 END) AS c_triggered,
          SUM(CASE WHEN whl.user_activity_id IN (${FUNNEL_BUCKETS.sent.join(",")})      THEN 1 ELSE 0 END) AS c_sent,
          SUM(CASE WHEN whl.user_activity_id IN (${FUNNEL_BUCKETS.delivered.join(",")}) THEN 1 ELSE 0 END) AS c_delivered,
          SUM(CASE WHEN whl.user_activity_id IN (${FUNNEL_BUCKETS.read.join(",")})      THEN 1 ELSE 0 END) AS c_read,
          SUM(CASE WHEN whl.user_activity_id IN (${FUNNEL_BUCKETS.clicked.join(",")})   THEN 1 ELSE 0 END) AS c_clicked,
          SUM(CASE WHEN whl.user_activity_id IN (${FUNNEL_BUCKETS.failed.join(",")})    THEN 1 ELSE 0 END) AS c_failed,
          SUM(CASE WHEN whl.user_activity_id IN (${FUNNEL_BUCKETS.skipped.join(",")})   THEN 1 ELSE 0 END) AS c_skipped
        FROM iceberg_db.whatsapp_hook_log_master whl
        WHERE whl.institution_id          = ${iid}
          AND whl.whatsapp_message_id IS NOT NULL
          AND whl.whatsapp_message_id    <> ''
          AND whl.message_template_id     > 0
          AND whl.audit_created_time >= TIMESTAMP '${start} 00:00:00'
          AND whl.audit_created_time <= TIMESTAMP '${end} 23:59:59'
          ${mainWhere}
        GROUP BY whl.institution_id, whl.whatsapp_message_id
      )
    `;

    const qualifyingCte = having
      ? `,
      qualifying_runs AS (
        SELECT campaign_run_log_id
        FROM anchored
        WHERE in_cohort = 1
          AND campaign_run_log_id IS NOT NULL
          AND DATE(bucket_time) BETWEEN DATE '${start}' AND DATE '${end}'
          ${tcGateAnchored}
        GROUP BY campaign_run_log_id
        ${having}
      )`
      : "";

    const qualifyingFilter = having
      ? `AND a.campaign_run_log_id IN (SELECT campaign_run_log_id FROM qualifying_runs)`
      : "";

    const sql = `
      WITH ${anchorCte}${qualifyingCte}
      SELECT
        a.campaign_run_log_id                  AS campaign_run_log_id,
        a.message_template_id                  AS message_template_id,
        a.user_id                              AS user_id,
        a.user_mobile_number                   AS phone_number,
        SUM(a.c_triggered)                     AS triggered,
        SUM(a.c_sent)                          AS sent,
        SUM(a.c_delivered)                     AS delivered,
        SUM(a.c_read)                          AS "read",
        SUM(a.c_clicked)                       AS clicked,
        SUM(a.c_failed)                        AS failed,
        SUM(a.c_skipped)                       AS skipped,
        MIN(a.min_event_time)                  AS first_event_at,
        MAX(a.max_event_time)                  AS last_event_at
      FROM anchored a
      WHERE a.in_cohort               = 1
        AND a.campaign_run_log_id IS NOT NULL
        AND DATE(a.bucket_time) BETWEEN DATE '${start}' AND DATE '${end}'
        ${tcGateA}
        ${qualifyingFilter}
      GROUP BY a.campaign_run_log_id, a.message_template_id, a.user_id, a.user_mobile_number
      ORDER BY a.campaign_run_log_id, triggered DESC
    `;

    console.log(`[whatsapp export] athena CAMPAIGN SQL prepared (mainWhere=${!!mainWhere}, having=${!!having}, tc=${templateCategory})`);
    return this.athena.executeQuery(sql);
  }

  /* ------------------------------------------------------------------ */
  /*    ATHENA: PER-TEMPLATE-TYPE x USER (Template Message Type granul.) */
  /* ------------------------------------------------------------------ */
  // Granularity = 7 (Template Message Type) — chart rolls templates up
  // by Text / Image / Video / Document / Interactive_* etc.
  //
  // Same anchored MV-shape SQL the template path uses, but the final
  // SELECT groups by `message_template_type_id` instead of
  // `message_template_id`. The type lives on message_template_master, so
  // a LEFT JOIN to a deduped subquery resolves it without fan-out.
  //
  // HAVING gate, when present, qualifies at the message_template_type_id
  // grain — same grain the chart filters at when this granularity is
  // selected.
  async fetchPerTemplateTypeUserRows({ institutionId, startDate, endDate, rules, templateCategory = "template", combinator = "and" }) {
    const iid   = sqlNumStrict(institutionId);
    const start = dateOnly(startDate);
    const end   = dateOnly(endDate);
    this.#assertWindowOk(start, end);

    const sessionTemplateIds = await this.#sessionTemplateIds();
    const { mainWhere, having } = this.buildWhere(rules, combinator, "anchored");

    const tcGateAnchored = this.#templateCategoryGate(templateCategory, sessionTemplateIds, "anchored");
    const tcGateA        = this.#templateCategoryGate(templateCategory, sessionTemplateIds, "a");

    const cohortIds = [...new Set([
      ...FUNNEL_BUCKETS.triggered,
      ...FUNNEL_BUCKETS.skipped,
      ...FUNNEL_BUCKETS.failed.filter((id) => id === 22),
    ])].join(",");

    const anchorCte = `
      anchored AS (
        SELECT
          whl.institution_id,
          whl.whatsapp_message_id,
          MAX(CASE WHEN whl.message_template_id > 0 THEN whl.message_template_id END) AS message_template_id,
          MAX(whl.user_id)                                                            AS user_id,
          REPLACE(MAX(CASE WHEN whl.user_mobile_number IS NOT NULL AND whl.user_mobile_number <> ''
                           THEN whl.user_mobile_number END), '+', '')                 AS user_mobile_number,
          MIN(whl.audit_created_time)                                                 AS min_event_time,
          MAX(whl.audit_created_time)                                                 AS max_event_time,
          COALESCE(MIN(CASE WHEN whl.user_activity_id = 13 THEN whl.audit_created_time END),
                   MIN(whl.audit_created_time))                                       AS bucket_time,
          MAX(CASE WHEN whl.user_activity_id IN (${cohortIds}) THEN 1 ELSE 0 END)     AS in_cohort,
          SUM(CASE WHEN whl.user_activity_id IN (${FUNNEL_BUCKETS.triggered.join(",")}) THEN 1 ELSE 0 END) AS c_triggered,
          SUM(CASE WHEN whl.user_activity_id IN (${FUNNEL_BUCKETS.sent.join(",")})      THEN 1 ELSE 0 END) AS c_sent,
          SUM(CASE WHEN whl.user_activity_id IN (${FUNNEL_BUCKETS.delivered.join(",")}) THEN 1 ELSE 0 END) AS c_delivered,
          SUM(CASE WHEN whl.user_activity_id IN (${FUNNEL_BUCKETS.read.join(",")})      THEN 1 ELSE 0 END) AS c_read,
          SUM(CASE WHEN whl.user_activity_id IN (${FUNNEL_BUCKETS.clicked.join(",")})   THEN 1 ELSE 0 END) AS c_clicked,
          SUM(CASE WHEN whl.user_activity_id IN (${FUNNEL_BUCKETS.failed.join(",")})    THEN 1 ELSE 0 END) AS c_failed,
          SUM(CASE WHEN whl.user_activity_id IN (${FUNNEL_BUCKETS.skipped.join(",")})   THEN 1 ELSE 0 END) AS c_skipped
        FROM iceberg_db.whatsapp_hook_log_master whl
        WHERE whl.institution_id          = ${iid}
          AND whl.whatsapp_message_id IS NOT NULL
          AND whl.whatsapp_message_id    <> ''
          AND whl.message_template_id     > 0
          AND whl.audit_created_time >= TIMESTAMP '${start} 00:00:00'
          AND whl.audit_created_time <= TIMESTAMP '${end} 23:59:59'
          ${mainWhere}
        GROUP BY whl.institution_id, whl.whatsapp_message_id
      ),
      templates AS (
        SELECT message_template_id, MAX(message_template_type_id) AS message_template_type_id
        FROM iceberg_db.message_template_master
        GROUP BY message_template_id
      )
    `;

    const qualifyingCte = having
      ? `,
      qualifying_template_types AS (
        SELECT t.message_template_type_id
        FROM anchored anchored
        JOIN templates t ON t.message_template_id = anchored.message_template_id
        WHERE anchored.in_cohort = 1
          AND anchored.message_template_id IS NOT NULL
          AND DATE(anchored.bucket_time) BETWEEN DATE '${start}' AND DATE '${end}'
          ${tcGateAnchored}
        GROUP BY t.message_template_type_id
        ${having}
      )`
      : "";

    const qualifyingFilter = having
      ? `AND t.message_template_type_id IN (SELECT message_template_type_id FROM qualifying_template_types)`
      : "";

    const sql = `
      WITH ${anchorCte}${qualifyingCte}
      SELECT
        t.message_template_type_id                AS message_template_type_id,
        a.user_id                                 AS user_id,
        a.user_mobile_number                      AS phone_number,
        SUM(a.c_triggered)                        AS triggered,
        SUM(a.c_sent)                             AS sent,
        SUM(a.c_delivered)                        AS delivered,
        SUM(a.c_read)                             AS "read",
        SUM(a.c_clicked)                          AS clicked,
        SUM(a.c_failed)                           AS failed,
        SUM(a.c_skipped)                          AS skipped,
        MIN(a.min_event_time)                     AS first_event_at,
        MAX(a.max_event_time)                     AS last_event_at
      FROM anchored a
      JOIN templates t ON t.message_template_id = a.message_template_id
      WHERE a.in_cohort               = 1
        AND a.message_template_id IS NOT NULL
        AND DATE(a.bucket_time) BETWEEN DATE '${start}' AND DATE '${end}'
        ${tcGateA}
        ${qualifyingFilter}
      GROUP BY t.message_template_type_id, a.user_id, a.user_mobile_number
      ORDER BY t.message_template_type_id, triggered DESC
    `;

    console.log(`[whatsapp export] athena TEMPLATE-TYPE SQL prepared (mainWhere=${!!mainWhere}, having=${!!having}, tc=${templateCategory})`);
    return this.athena.executeQuery(sql);
  }

  /* ------------------------------------------------------------------ */
  /*                  MYSQL: USER NAME / TRANSACTION LOOKUPS             */
  /* ------------------------------------------------------------------ */
  // Kept on MySQL — these are point lookups on small dimension tables
  // and adding Athena round-trips here would dominate runtime. The main
  // funnel query (the expensive one) is what moved to Athena.
  async fetchUserNames(userIds) {
    const real = userIds.filter((id) => id != null && Number(id) > 0).map(Number);
    if (!real.length) return new Map();
    const placeholders = real.map(() => "?").join(",");
    const rows = await this.sqlConn.queryAsync(
      `SELECT UserMasterId, UserName FROM UserMaster WHERE UserMasterId IN (${placeholders})`,
      real,
    );
    const map = new Map();
    for (const r of rows) map.set(Number(r.UserMasterId), r.UserName || "");
    return map;
  }

  // Campaign label resolution for the Campaign-mode export.
  //   CampaignRunLogMaster.CampaignRunlogId
  //     → CampaignRunLogMaster.ConnectBotInstSchoolCampaignId   (master id)
  //     → ConnectCampaignSchoolAudienceMaster.CampaignConfigName (display name)
  //
  // Same join the live service's #campaignLabels walks — keeps the CSV
  // labels aligned with the chart's campaign-name display so a row in
  // the export is traceable to a bar on screen.
  async fetchCampaignLabels(runLogIds) {
    const real = (runLogIds || []).map(Number).filter((n) => !isNaN(n) && n > 0);
    if (!real.length) return new Map();
    const placeholders = real.map(() => "?").join(",");
    try {
      const rows = await this.sqlConn.queryAsync(
        `SELECT crlm.CampaignRunlogId               AS run_log_id,
                crlm.ConnectBotInstSchoolCampaignId AS master_id,
                MAX(csam.CampaignConfigName)        AS campaign_name
           FROM CampaignRunLogMaster crlm
           LEFT JOIN ConnectCampaignSchoolAudienceMaster csam
                  ON csam.CbotCampaignMasterId = crlm.ConnectBotInstSchoolCampaignId
          WHERE crlm.CampaignRunlogId IN (${placeholders})
          GROUP BY crlm.CampaignRunlogId, crlm.ConnectBotInstSchoolCampaignId`,
        real,
      );
      const map = new Map();
      for (const r of rows) {
        map.set(Number(r.run_log_id), {
          masterId: r.master_id != null ? Number(r.master_id) : null,
          name:     r.campaign_name,
        });
      }
      return map;
    } catch (err) {
      console.error("[whatsapp export] campaignLabels lookup failed:", err.message);
      return new Map();
    }
  }

  // Phone → { userId, userName } resolver for rows where the per-message
  // anchor couldn't find a user_id (outbound campaign events frequently
  // carry the phone but not the user master id; the chart doesn't care
  // because it groups by run, but the export's per-user grain does).
  //
  // Returns Map<phoneNumber, { userId, userName }>. The phone keys are
  // normalised to digits-only (REPLACE '+', '') so they match what the
  // Athena anchor's REPLACE produces.
  async fetchUsersByPhone(phoneNumbers) {
    const phones = [...new Set(
      (phoneNumbers || [])
        .map((p) => String(p ?? "").replace(/[+\s]/g, ""))
        .filter((p) => p.length >= 6),
    )];
    if (!phones.length) return new Map();
    const placeholders = phones.map(() => "?").join(",");
    try {
      const rows = await this.sqlConn.queryAsync(
        `SELECT REPLACE(UserMobileNumber, '+', '') AS phone_norm,
                UserMasterId,
                UserName
           FROM UserMaster
          WHERE REPLACE(UserMobileNumber, '+', '') IN (${placeholders})`,
        phones,
      );
      const map = new Map();
      for (const r of rows) {
        const key = String(r.phone_norm || "");
        if (!key) continue;
        // First-write-wins — if a phone maps to multiple users, take the
        // one we hit first. Matches the legacy single-pick semantics.
        if (!map.has(key)) {
          map.set(key, {
            userId:   r.UserMasterId != null ? Number(r.UserMasterId) : null,
            userName: r.UserName || "",
          });
        }
      }
      return map;
    } catch (err) {
      console.error("[whatsapp export] usersByPhone lookup failed:", err.message);
      return new Map();
    }
  }

  // First TransactionMasterId per user — same single-pick behaviour as
  // the legacy /templateinteractionexportreport.
  async fetchTransactionIds(userIds) {
    const real = userIds.filter((id) => id != null && Number(id) > 0).map(Number);
    if (!real.length) return new Map();
    const placeholders = real.map(() => "?").join(",");
    const rows = await this.sqlConn.queryAsync(
      `SELECT UserMasterId, TransactionMasterId
       FROM TransactionMasters
       WHERE UserMasterId IN (${placeholders})`,
      real,
    );
    const map = new Map();
    for (const r of rows) {
      const uid = Number(r.UserMasterId);
      if (!map.has(uid)) map.set(uid, r.TransactionMasterId);
    }
    return map;
  }

  /* ------------------------------------------------------------------ */
  /*                          SHAPE FOR CSV                              */
  /* ------------------------------------------------------------------ */
  shape(rawRows, userNameMap, txnMap = new Map()) {
    return (rawRows || []).map((r) => {
      const userIdRaw = r.user_id != null && Number(r.user_id) > 0 ? Number(r.user_id) : null;
      return {
        MessageTemplateId:   r.message_template_id ?? "-",
        MessageTemplateName: r.message_template_name || "-",
        UserId:              userIdRaw ?? "-",
        UserName:            (userIdRaw && userNameMap.get(userIdRaw)) || "-",
        PhoneNumber:         r.phone_number || "-",
        TransactionId:       (userIdRaw && txnMap.get(userIdRaw)) || "-",
        Triggered:           this.#num(r.triggered),
        Sent:                this.#num(r.sent),
        Delivered:           this.#num(r.delivered),
        Read:                this.#num(r.read),
        Clicked:             this.#num(r.clicked),
        Failed:              this.#num(r.failed),
        Skipped:             this.#num(r.skipped),
        FirstEventAt:        this.#fmtDate(r.first_event_at),
        LastEventAt:         this.#fmtDate(r.last_event_at),
      };
    });
  }

  // Template-Type-mode CSV shape. Replaces the template-id/name columns
  // with TemplateTypeId / TemplateTypeName (Text / Image / …). Per-user
  // funnel volumes are aggregated over every template of that type.
  shapeTemplateType(rawRows, userNameMap, txnMap, templateTypeNameMap) {
    return (rawRows || []).map((r) => {
      const userIdRaw = r.user_id != null && Number(r.user_id) > 0 ? Number(r.user_id) : null;
      const ttId      = r.message_template_type_id != null && Number(r.message_template_type_id) > 0
        ? Number(r.message_template_type_id)
        : null;
      return {
        TemplateTypeId:      ttId ?? "-",
        TemplateTypeName:    (ttId && templateTypeNameMap.get(ttId)) || "-",
        UserId:              userIdRaw ?? "-",
        UserName:            (userIdRaw && userNameMap.get(userIdRaw)) || "-",
        PhoneNumber:         r.phone_number || "-",
        TransactionId:       (userIdRaw && txnMap.get(userIdRaw)) || "-",
        Triggered:           this.#num(r.triggered),
        Sent:                this.#num(r.sent),
        Delivered:           this.#num(r.delivered),
        Read:                this.#num(r.read),
        Clicked:             this.#num(r.clicked),
        Failed:              this.#num(r.failed),
        Skipped:             this.#num(r.skipped),
        FirstEventAt:        this.#fmtDate(r.first_event_at),
        LastEventAt:         this.#fmtDate(r.last_event_at),
      };
    });
  }

  // Campaign-mode CSV shape. Adds CampaignRunLogId, CampaignMasterId,
  // CampaignName up front so each row is traceable back to its
  // /askengage/notification-report URL (which is per CbotCampaignMasterId).
  // Template columns retained — every run targets one template, but it's
  // useful context in the CSV.
  shapeCampaign(rawRows, userNameMap, txnMap, campaignMap, templateNameMap = new Map()) {
    return (rawRows || []).map((r) => {
      const userIdRaw = r.user_id != null && Number(r.user_id) > 0 ? Number(r.user_id) : null;
      const runId     = r.campaign_run_log_id != null && Number(r.campaign_run_log_id) > 0 ? Number(r.campaign_run_log_id) : null;
      const tplId     = r.message_template_id != null && Number(r.message_template_id) > 0 ? Number(r.message_template_id) : null;
      const campaign  = runId ? campaignMap.get(runId) : null;
      return {
        CampaignRunLogId:    runId ?? "-",
        CampaignMasterId:    (campaign && campaign.masterId) ?? "-",
        CampaignName:        (campaign && campaign.name) || "-",
        MessageTemplateId:   tplId ?? "-",
        MessageTemplateName: (tplId && templateNameMap.get(tplId)) || "-",
        UserId:              userIdRaw ?? "-",
        UserName:            (userIdRaw && userNameMap.get(userIdRaw)) || "-",
        PhoneNumber:         r.phone_number || "-",
        TransactionId:       (userIdRaw && txnMap.get(userIdRaw)) || "-",
        Triggered:           this.#num(r.triggered),
        Sent:                this.#num(r.sent),
        Delivered:           this.#num(r.delivered),
        Read:                this.#num(r.read),
        Clicked:             this.#num(r.clicked),
        Failed:              this.#num(r.failed),
        Skipped:             this.#num(r.skipped),
        FirstEventAt:        this.#fmtDate(r.first_event_at),
        LastEventAt:         this.#fmtDate(r.last_event_at),
      };
    });
  }

  // Template Message Type labels (Text / Image / Video / Document / …)
  // for the Template-Type-grain CSV. One round-trip; cached on the
  // instance so repeat exports in the same worker reuse the map.
  async fetchTemplateTypeNames(typeIds) {
    const real = (typeIds || []).map(Number).filter((n) => !isNaN(n) && n > 0);
    if (!real.length) return new Map();
    if (this._templateTypeNamesCache) {
      // Cheap-cache hit — every export's type set is small (~12 types).
      // Return only the requested ids from the cached map.
      const out = new Map();
      for (const id of real) {
        if (this._templateTypeNamesCache.has(id)) out.set(id, this._templateTypeNamesCache.get(id));
      }
      if (out.size === real.length) return out;
    }
    try {
      const rows = await this.sqlConn.queryAsync(
        `SELECT MessageTemplateTypeId, MessageTemplateTypeName FROM MessageTemplateTypeMaster`,
      );
      const map = new Map();
      for (const r of rows) map.set(Number(r.MessageTemplateTypeId), r.MessageTemplateTypeName || "");
      this._templateTypeNamesCache = map;
      const out = new Map();
      for (const id of real) {
        if (map.has(id)) out.set(id, map.get(id));
      }
      return out;
    } catch (err) {
      console.error("[whatsapp export] templateTypeNames lookup failed:", err.message);
      return new Map();
    }
  }

  // Cheap MySQL lookup for the Campaign-mode CSV's MessageTemplateName
  // column. Mirrors the GROUP-BY-deduped subquery the chart uses inline.
  async fetchTemplateNames(templateIds) {
    const real = (templateIds || []).map(Number).filter((n) => !isNaN(n) && n > 0);
    if (!real.length) return new Map();
    const placeholders = real.map(() => "?").join(",");
    try {
      const rows = await this.sqlConn.queryAsync(
        `SELECT MessageTemplateId, MessageTemplateName
           FROM MessageTemplateMaster
          WHERE MessageTemplateId IN (${placeholders})`,
        real,
      );
      const map = new Map();
      for (const r of rows) map.set(Number(r.MessageTemplateId), r.MessageTemplateName || "");
      return map;
    } catch (err) {
      console.error("[whatsapp export] templateNames lookup failed:", err.message);
      return new Map();
    }
  }

  #num(v) {
    if (v == null || v === "") return 0;
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
  }

  #fmtDate(d) {
    if (!d) return "-";
    const dt = d instanceof Date ? d : new Date(d);
    if (isNaN(dt)) return String(d);
    const yyyy = dt.getFullYear();
    const mm = String(dt.getMonth() + 1).padStart(2, "0");
    const dd = String(dt.getDate()).padStart(2, "0");
    const hh = String(dt.getHours()).padStart(2, "0");
    const mi = String(dt.getMinutes()).padStart(2, "0");
    const ss = String(dt.getSeconds()).padStart(2, "0");
    return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
  }
}

module.exports = WhatsappExportService;

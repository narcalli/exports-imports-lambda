/**
 * Athena-backed export service for the Document Metrics view.
 *
 * Mirrors the structure of Trend-Export-Service / Benchmark-Export-Service:
 *   - constructor takes an AthenaService (cohort query) and a MySQL
 *     connection (metadata enrichment).
 *   - buildWhere() composes a parameterised-by-interpolation Iceberg
 *     WHERE clause for `iceberg_db.new_document_master`, mirroring the
 *     report service's hot path so the CSV cohort matches the dashboard
 *     cohort EXACTLY (same rows, same count).
 *   - fetchRows() runs the cohort query against Athena, then enriches
 *     the result IDs with MySQL-only metadata (DocumentUrl from
 *     DocumentMetaDataConnect, UserMobile via DocTrxConnnectMaster).
 *   - shape() projects rows to the CSV column set.
 *
 * Why Athena (matches Trend/Benchmark/WhatsApp export pattern):
 *   The dashboard's TOTAL DOCUMENTS card and breakdowns read from the
 *   StarRocks MV / raw `iceberg.iceberg_db.new_document_master`. Athena
 *   reads the same underlying Iceberg tables (via the iceberg_db
 *   database), so cohort counts reconcile to the dashboard at any given
 *   point in time. Out-of-band BullMQ jobs can tolerate Athena's 2-30s
 *   query lag, and keeping the export-infra surface uniform with the
 *   other three exports avoids one-off connection wiring.
 *
 * Why MySQL for enrichment:
 *   - DocumentUrl (DocumentMetaDataConnect.DataStorePayload) — Iceberg
 *     has it but the MySQL master table is the canonical source and
 *     avoids a second Athena round-trip for a small ID-list lookup.
 *   - UserMobile via DocTrxConnnectMaster → TransactionMasters →
 *     UserMaster — same MySQL join path the dashboard uses for its own
 *     enrichment elsewhere.
 *   - Label master tables (Document type / source / status) only live
 *     in MySQL.
 *
 * Output: one CSV row per new_document_master within the date range:
 *   DocumentId, DocumentType, DocumentSource, DocumentStatus,
 *   UserMobile, DocumentUrl, CreatedAt
 *
 * Fill policy: missing labels / URL / mobile fall back to "-" so the
 * reader can still trace the row by id.
 */

const AthenaService = require("../../services/athena.service");

// Iceberg snake_case columns with `ndm` alias prefix — matches the
// FROM clause in fetchRows. Mirrors DocumentReportService's
// STARROCKS_QB_MAPPING semantically so the CSV cohort tracks the
// dashboard cohort row-for-row.
const TYPE_ALIASES = [
  "DocumentType",
  "DocumentDataType",
  "DocumentDataTypes",
  "document_type",
  "document_data_type",
  "document_data_types",
  "document_datatypes",
  "documentTypeId",
  "Document Type",
  "Document Data Type",
  "Document Data Types",
];
const SOURCE_ALIASES = [
  "DocumentSource",
  "DocumentSources",
  "document_source",
  "document_sources",
  "documentSourceId",
  "Document Source",
  "Document Sources",
  "Sources",
];
const STATUS_ALIASES = [
  "DocumentStatus",
  "document_status",
  "documentStatusId",
  "Document Status",
];

const expand = (aliases, col) =>
  Object.fromEntries(aliases.map((a) => [a, col]));

const QB_MAPPING = {
  ...expand(TYPE_ALIASES, "ndm.document_data_type_master_id"),
  ...expand(SOURCE_ALIASES, "ndm.document_source_id"),
  ...expand(STATUS_ALIASES, "ndm.document_status_id"),
};

// Mobile-number aliases — same list the report service uses. Lifted out
// of QB_MAPPING because the column lives on user_master, not
// new_document_master, and needs an EXISTS sub-query rather than a
// column equality.
const MOBILE_ALIASES = [
  "user_mobile_number",
  "UserMobileNumber",
  "userMobileNumber",
  "user_mobile",
  "userMobile",
  "Mobile",
  "Mobile Number",
  "User Mobile Number",
];
const MOBILE_ALIAS_SET = new Set(MOBILE_ALIASES);

// Athena has no parameter binding — all values are interpolated into
// the SQL string. These helpers (lifted from Trend-Export-Service) are
// the safety boundary: every user-controlled value goes through one of
// them before reaching the query.
function sqlNum(v) {
  const n = Number(v);
  if (!Number.isFinite(n)) throw new Error(`Non-numeric value: ${v}`);
  return String(n);
}

function sqlStr(v) {
  return `'${String(v).replace(/'/g, "''")}'`;
}

// Every column in QB_MAPPING is an integer master-id (document_data_type_master_id,
// document_source_id, document_status_id). FE sends them as Number or numeric
// string; emit as a bare integer literal so Athena/Trino doesn't reject the
// comparison with `Cannot apply operator: integer = varchar(N)`.
function sqlIdVal(v) {
  const s = String(v).trim();
  if (/^-?\d+$/.test(s)) return s;
  return sqlStr(v);
}

// Trino/Athena is strict about types — `audit_create_time` is
// `timestamp(6)` in Iceberg, so comparing against a bare string literal
// errors with `Cannot apply operator: timestamp(6) <= varchar(19)`.
// `TIMESTAMP 'YYYY-MM-DD HH:MM:SS'` is the canonical typed-literal
// syntax Trino accepts.
function sqlTimestamp(v) {
  return `TIMESTAMP ${sqlStr(v)}`;
}

// Escape LIKE-special characters (`_` `%` `\`) so a mobile token typed
// with one of them doesn't act as a SQL wildcard. The trailing `%` for
// prefix-match is appended AFTER escaping.
function escapeLike(s) {
  return String(s).replace(/\\/g, "\\\\").replace(/%/g, "\\%").replace(/_/g, "\\_");
}

// Lifts the mobile-number rule(s) out of a QueryBuilder payload and
// returns { mobileTokens, remaining }. Mirrors
// DocumentReportService.extractMobileSearch so the export's filtered
// cohort matches the dashboard's filtered cohort exactly.
function extractMobileSearch(queryBuilder) {
  if (!queryBuilder || !Array.isArray(queryBuilder.rules) || !queryBuilder.rules.length) {
    return { mobileTokens: [], remaining: queryBuilder };
  }
  const tokens = [];
  const remaining = [];
  for (const rule of queryBuilder.rules) {
    if (!rule || !MOBILE_ALIAS_SET.has(rule.field)) {
      remaining.push(rule);
      continue;
    }
    const raw = rule.value;
    const parts = Array.isArray(raw) ? raw : String(raw ?? "").split(",");
    for (const p of parts) {
      const cleaned = String(p || "").trim();
      if (!cleaned) continue;
      const contains = cleaned.startsWith("*");
      const stripped = cleaned.replace(/^\*+/, "").replace(/\*+$/, "");
      if (!stripped) continue;
      tokens.push(
        contains ? `%${escapeLike(stripped)}%` : `${escapeLike(stripped)}%`,
      );
    }
  }
  return {
    mobileTokens: tokens,
    remaining: { ...queryBuilder, rules: remaining },
  };
}

// Build the QB-rule WHERE fragment as a single interpolated string.
// Drops unsupported fields (matches DocumentReportService) so a stray
// non-documents rule won't sneak through as `WHERE <field> = ...`.
function buildQbWhere(queryBuilder) {
  if (!queryBuilder || !Array.isArray(queryBuilder.rules) || !queryBuilder.rules.length) {
    return "";
  }
  const combinator = String(queryBuilder.combinator || "AND").toUpperCase();
  const comb = combinator === "OR" ? "OR" : "AND";

  const conds = queryBuilder.rules
    .map((rule) => {
      const { field, operator, value } = rule || {};
      if (!field || !operator || value === undefined || value === null || value === "") {
        return null;
      }
      const col = QB_MAPPING[field];
      if (!col) {
        console.warn(
          `[document export] dropping unsupported QB rule field "${field}" — not in documents mapping`,
        );
        return null;
      }
      switch (String(operator).toLowerCase()) {
        case "equals":
        case "eq":
          return `${col} = ${sqlIdVal(value)}`;
        case "not_equals":
        case "neq":
        case "ne":
          return `${col} != ${sqlIdVal(value)}`;
        case "in":
          if (Array.isArray(value) && value.length > 0) {
            return `${col} IN (${value.map(sqlIdVal).join(", ")})`;
          }
          return `${col} = ${sqlIdVal(value)}`;
        default:
          return `${col} = ${sqlIdVal(value)}`;
      }
    })
    .filter(Boolean);

  return conds.length ? conds.join(` ${comb} `) : "";
}

class DocumentExportService {
  /**
   * @param {object} athena   AthenaService (cohort query against Iceberg)
   * @param {object} sqlConn  MySQL connection (metadata enrichment)
   */
  constructor(athena, sqlConn) {
    this.athena = athena || new AthenaService();
    this.sqlConn = sqlConn;
  }

  // Build the Athena WHERE clause for the cohort query against
  // `iceberg_db.new_document_master ndm`. Returns a single SQL string
  // — Athena has no `?` placeholder support, so every value is
  // interpolated via sqlNum / sqlStr / escapeLike. Mirrors
  // DocumentReportService's icebergWhere shape so the CSV cohort tracks
  // the dashboard cohort exactly.
  buildWhere({ institutionId, startDate, endDate, filters = {}, queryBuilder = null }) {
    const { mobileTokens, remaining: qbWithoutMobile } =
      extractMobileSearch(queryBuilder);

    const parts = [
      `ndm.institution_id = ${sqlNum(institutionId)}`,
      `ndm.audit_create_time >= ${sqlTimestamp(`${startDate} 00:00:00`)}`,
      `ndm.audit_create_time <= ${sqlTimestamp(`${endDate} 23:59:59`)}`,
    ];

    if (filters.documentTypeId) {
      parts.push(`ndm.document_data_type_master_id = ${sqlNum(filters.documentTypeId)}`);
    }
    if (filters.documentSourceId) {
      parts.push(`ndm.document_source_id = ${sqlNum(filters.documentSourceId)}`);
    }
    if (filters.documentStatusId) {
      parts.push(`ndm.document_status_id = ${sqlNum(filters.documentStatusId)}`);
    }

    const qb = buildQbWhere(qbWithoutMobile);
    if (qb) parts.push(`(${qb})`);

    // Mobile filter — non-correlated `IN (...)` rather than correlated
    // EXISTS. Same shape DocumentReportService uses on StarRocks (where
    // the catalog grammar rejected EXISTS); kept identical here so the
    // export and the report compute the same cohort.
    //
    // Scoped by institution + date window on `transaction_masters` so
    // we don't fan out across the whole multi-tenant transaction
    // history. Without these the subquery scans years of transactions
    // joined against the entire user_master table.
    //
    // No `ESCAPE '\\'` clause — backslash is the default LIKE escape
    // in Trino/Athena too, and dropping it keeps the SQL byte-identical
    // to the report-side query for diff-ability.
    if (mobileTokens.length > 0) {
      const likeFragments = mobileTokens
        .map((t) => `um.user_mobile_number LIKE ${sqlStr(t)}`)
        .join(" OR ");
      parts.push(`ndm.id IN (
        SELECT dtc.new_document_master_id
        FROM iceberg_db.doc_trx_connect_master dtc
        JOIN iceberg_db.transaction_masters tx
          ON tx.transaction_master_id = dtc.transaction_id
        JOIN iceberg_db.user_master um
          ON um.user_master_id = tx.user_master_id
        WHERE tx.institution_id = ${sqlNum(institutionId)}
          AND tx.audit_create_time >= ${sqlTimestamp(`${startDate} 00:00:00`)}
          AND tx.audit_create_time <= ${sqlTimestamp(`${endDate} 23:59:59`)}
          AND (${likeFragments})
      )`);
    }

    return parts.join(" AND ");
  }

  // Run the cohort query against Athena, then enrich the result IDs
  // with MySQL-only metadata.
  //
  // FOUR-PASS strategy:
  //   Pass 1 — Athena cohort: per-doc ids + timestamp + type/source/status
  //            ids. SAME data source as the dashboard, so CSV row count
  //            equals dashboard total by construction.
  //   Pass 2 — DocumentUrl (DataStorePayload) from MySQL for the cohort IDs.
  //   Pass 3 — MAX(UserMobileNumber) from MySQL for the cohort IDs.
  //   Pass 4 — Three label master tables from MySQL (joined in JS).
  //
  // Passes 2 + 3 + 4 run in parallel after pass 1.
  //
  // CreatedAt is pre-formatted in Athena as YYYY-MM-DD; emitting the raw
  // timestamp would surface a JS Date.toString() in the CSV.
  async fetchRows({ institutionId, startDate, endDate, filters, queryBuilder }) {
    const whereSql = this.buildWhere({
      institutionId,
      startDate,
      endDate,
      filters,
      queryBuilder,
    });

    const cohortSql = `
      SELECT
        ndm.id AS NewDocumentMasterId,
        DATE_FORMAT(ndm.audit_create_time, '%Y-%m-%d') AS CreatedAt,
        ndm.document_data_type_master_id AS DocumentDataTypeMasterId,
        ndm.document_source_id AS DocumentSourceId,
        ndm.document_status_id AS DocumentStatusId
      FROM iceberg_db.new_document_master ndm
      WHERE ${whereSql}
      ORDER BY ndm.audit_create_time DESC
    `;

    // Athena returns every column as a string (VarCharValue). Normalise
    // numeric ids back to Number so downstream Map lookups against
    // MySQL results (which arrive as numbers) hit on equality.
    console.log("[document export] cohort SQL:\n" + cohortSql);
    const rawAthenaRows = await this.athena.executeQuery(cohortSql);
    const mainRows = rawAthenaRows
      .map((r) => ({
        NewDocumentMasterId: r.NewDocumentMasterId ? Number(r.NewDocumentMasterId) : null,
        CreatedAt: r.CreatedAt || null,
        DocumentDataTypeMasterId:
          r.DocumentDataTypeMasterId ? Number(r.DocumentDataTypeMasterId) : null,
        DocumentSourceId: r.DocumentSourceId ? Number(r.DocumentSourceId) : null,
        DocumentStatusId: r.DocumentStatusId ? Number(r.DocumentStatusId) : null,
      }))
      .filter((r) => r.NewDocumentMasterId != null);

    if (!mainRows.length) return [];

    const ids = mainRows.map((r) => r.NewDocumentMasterId);

    // Passes 2 + 3 + 4 in parallel. Label lookups don't depend on the
    // ID set but kicking them off here keeps wall-clock identical to
    // a sequential pipeline.
    const [urlMap, mobileMap, typeLabelMap, sourceLabelMap, statusLabelMap] =
      await Promise.all([
        this.#fetchDocumentUrls(ids),
        this.#fetchUserMobiles(ids),
        this.#fetchLabelMap(
          `SELECT DocumentId AS id, Document AS label FROM DocumentDataTypeMaster`,
        ),
        this.#fetchLabelMap(
          `SELECT DocumentSourceId AS id, DocumentSourceLabel AS label FROM DocumentSource`,
        ),
        this.#fetchLabelMap(
          `SELECT DocumentStatusId AS id, DocumentStatusLabel AS label FROM DocumentStatus`,
        ),
      ]);

    // Stitch all lookups into the main rows. Hash lookups are O(1).
    for (const r of mainRows) {
      const id = r.NewDocumentMasterId;
      r.DocumentUrl = urlMap.get(id) || null;
      r.UserMobile = mobileMap.get(id) || null;
      r.DocumentTypeLabel = typeLabelMap.get(r.DocumentDataTypeMasterId) || null;
      r.DocumentSourceLabel = sourceLabelMap.get(r.DocumentSourceId) || null;
      r.DocumentStatusLabel = statusLabelMap.get(r.DocumentStatusId) || null;
    }
    return mainRows;
  }

  // ROW_NUMBER preference matches DocumentReportService.listSql: prefer
  // DocMetaDataTypeId=1 (canonical), fall back to 4, drop anything else.
  // Scoped to the IDs that made it through the cohort pass.
  async #fetchDocumentUrls(ids) {
    if (!ids.length) return new Map();
    const sql = `
      SELECT NewDocumentMasterId, DataStorePayload
      FROM (
        SELECT
          d.NewDocumentMasterId,
          d.DataStorePayload,
          ROW_NUMBER() OVER (
            PARTITION BY d.NewDocumentMasterId
            ORDER BY
              CASE
                WHEN d.DocMetaDataTypeId = 1 THEN 1
                WHEN d.DocMetaDataTypeId = 4 THEN 2
                ELSE 3
              END
          ) AS rn
        FROM DocumentMetaDataConnect d
        WHERE d.NewDocumentMasterId IN (?)
          AND d.DocMetaDataTypeId IN (1, 4)
      ) x
      WHERE x.rn = 1`;
    const rows = await this.sqlConn.queryAsync(sql, [ids]);
    return new Map(rows.map((r) => [r.NewDocumentMasterId, r.DataStorePayload]));
  }

  // A single document can be attached to multiple transactions/users —
  // MAX(UserMobileNumber) picks one deterministically without multiplying
  // rows. Scoped to the IDs that made it through the cohort pass.
  async #fetchUserMobiles(ids) {
    if (!ids.length) return new Map();
    const sql = `
      SELECT
        dtc.NewDocumentMasterId,
        MAX(um.UserMobileNumber) AS UserMobileNumber
      FROM DocTrxConnnectMaster dtc
      JOIN TransactionMasters tx
        ON dtc.TransactionId = tx.TransactionMasterId
      JOIN UserMaster um
        ON tx.UserMasterId = um.UserMasterId
      WHERE dtc.NewDocumentMasterId IN (?)
      GROUP BY dtc.NewDocumentMasterId`;
    const rows = await this.sqlConn.queryAsync(sql, [ids]);
    return new Map(rows.map((r) => [r.NewDocumentMasterId, r.UserMobileNumber]));
  }

  // Generic MySQL label-map loader. Used for the three small master
  // tables (Document type / source / status). All three have the same
  // `{ id, label }` projection shape so one helper covers all.
  async #fetchLabelMap(sql) {
    const rows = await this.sqlConn.queryAsync(sql);
    return new Map(rows.map((r) => [r.id, r.label]));
  }

  // Project raw rows to the CSV column shape. CreatedAt arrives
  // pre-formatted (YYYY-MM-DD) from Athena. DocumentUrl + UserMobile
  // + labels default to "-" so blanks read as "no link / no user / no
  // label" rather than empty cells.
  shape(rawRows) {
    return rawRows.map((r) => ({
      DocumentId: r.NewDocumentMasterId,
      DocumentType: r.DocumentTypeLabel || "-",
      DocumentSource: r.DocumentSourceLabel || "-",
      DocumentStatus: r.DocumentStatusLabel || "-",
      UserMobile: r.UserMobile || "-",
      DocumentUrl: r.DocumentUrl || "-",
      CreatedAt: r.CreatedAt,
    }));
  }
}

module.exports = DocumentExportService;

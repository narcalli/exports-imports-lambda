/**
 * Transaction Conversion export service.
 *
 * Pulls the per-transaction conversion rows from Athena (iceberg_db) using
 * the same query the live `/trx-conversion-stream` endpoint uses, then pivots
 * them into one row per Transaction+Order — identical in shape to the legacy
 * `/trx-conversion-download` CSV, so existing consumers see no change.
 *
 * The Athena query itself lives in dashboard/conversationWithTransaction.js so
 * the streaming route and this export share a single source of truth.
 */

const {
  getTransactionData,
} = require("../../../dashboard/conversationWithTransaction");

// Fixed leading columns, in the order the legacy download emitted them. Any
// dynamic OrderDataType / attribute columns are appended after these by toCsv
// (which unions keys across all rows in first-seen order).
const BASE_COLUMNS = [
  "TransactionId",
  "OrderId",
  "Date",
  "UserMobileNumber",
  "UserEmail",
  "UserName",
  "User_Type",
  "AgentName",
  "Stage",
  "SourceName",
  "OrderStatus",
];

class ConversionExportService {
  /**
   * @param {import('../../services/athena.service')} [athenaService] - unused
   *   directly (the shared query owns its own Athena client) but accepted to
   *   match the constructor shape of the other export services.
   */
  constructor(athenaService) {
    this.athenaService = athenaService || null;
  }

  // Raw, un-pivoted Athena rows (one row per transaction × orderData × attribute).
  async fetchRows({ institutionId, startDate, endDate }) {
    return getTransactionData({
      iid: institutionId,
      startDate,
      endDate,
    });
  }

  // Collapse the row-multiplied Athena output into one row per
  // Transaction+Order, spreading each OrderDataType and each attribute name
  // into its own column.
  transformToCsv(rawRows) {
    const grouped = (rawRows || []).reduce((acc, item) => {
      const key = `${item.TransactionId}#${item.OrderId}`;

      if (!acc[key]) {
        acc[key] = {
          TransactionId: item.TransactionId,
          OrderId: item.OrderId || "",
          Date: item.Date,
          UserMobileNumber: item.UserMobileNumber,
          UserEmail: item.UserEmail || "",
          UserName: item.UserName,
          User_Type: item.User_Type,
          AgentName: item.AgentName || "",
          Stage: item.Stage,
          SourceName: item.SourceName,
          OrderStatus: item.OrderStatus,
        };
      }

      if (item.OrderDataType) {
        acc[key][item.OrderDataType] = item.OrderData;
      }

      // Attribute name → attribute value (TrxAttributeValue is the alias the
      // query exposes; the legacy route referenced a non-existent `Label`
      // field here, which silently produced blank attribute columns).
      if (item.TrxAttributeInstName) {
        acc[key][item.TrxAttributeInstName] = item.TrxAttributeValue || "";
      }

      return acc;
    }, {});

    return Object.values(grouped);
  }
}

module.exports = ConversionExportService;
module.exports.BASE_COLUMNS = BASE_COLUMNS;

const redisClient = require("../allservices/redisclient");
const AthenaService = require("../src/services/athena.service");

const athenaService = new AthenaService();
const DB = "iceberg_db";

// Shared FROM + JOIN graph and WHERE clause for both the count and the data
// queries so they stay in lock-step. This mirrors the original MySQL joins,
// translated to the iceberg_db snake_case tables/columns.
const buildFromAndWhere = ({ startDate, endDate, iid }) => `
  FROM
    ${DB}.transaction_masters tm
    JOIN ${DB}.user_master um ON tm.user_master_id = um.user_master_id
    JOIN ${DB}.transaction_stage_master tsm ON tm.transaction_stage_id = tsm.id
    LEFT JOIN ${DB}.inst_trx_stage_master its
              ON its.transaction_stage_id = tsm.id AND its.institution_id = tm.institution_id
    LEFT JOIN ${DB}.connect_agent_trx_master catm ON tm.transaction_master_id = catm.transaction_master_id
    LEFT JOIN ${DB}.agent_master am ON catm.agent_master_id = am.agent_master_id
    JOIN ${DB}.source_master sm ON tm.source_id = sm.source_id
    JOIN ${DB}.source_master_inst smi ON tm.source_id = smi.source_id AND smi.institution_id = tm.institution_id
    LEFT JOIN ${DB}.order_master om ON tm.transaction_master_id = om.transaction_master_id
    LEFT JOIN ${DB}.connect_order_product_shipment cops ON om.order_id = cops.order_id
    LEFT JOIN ${DB}.order_log ol ON cops.con_order_product_id = ol.con_order_product_id
    LEFT JOIN ${DB}.order_data_type_master odm ON ol.order_data_type_id = odm.id
    LEFT JOIN ${DB}.order_status_master osm ON om.order_status_id = osm.id
    LEFT JOIN ${DB}.order_status_inst_master osim
              ON osm.id = osim.order_status_id AND osim.institution_id = tm.institution_id
    LEFT JOIN ${DB}.trx_id_attribute_master_inst_connect tx ON tx.transaction_id = tm.transaction_master_id
    LEFT JOIN ${DB}.trx_attributes_inst_items ii
              ON ii.taocsv_inst_id = TRY_CAST(tx.payload AS int) AND tx.trx_attribute_master_inst_id = ii.trx_attribute_master_inst_id
    LEFT JOIN ${DB}.trx_attribute_master_inst ta
              ON tx.trx_attribute_master_inst_id = ta.trx_attribute_master_inst_id AND ta.institution_id = tm.institution_id
  WHERE
    tm.audit_create_time >= TIMESTAMP '${startDate} 00:00:00'
    AND tm.audit_create_time <= TIMESTAMP '${endDate} 23:59:59'
    AND tm.institution_id = ${iid}
    AND NOT (um.user_type = 24)
`;

const conversationWithTransaction = async (data) => {
  try {
    let { startDate, endDate, iid } = data;

    if (!startDate || !endDate || !iid) {
      return {
        error: true,
        message: "startDate, endDate, and iid are required",
      };
    }

    const transactionData = await getTransactionData({ startDate, endDate, iid });

    return {
      error: false,
      message: "Conversation with transaction data fetched successfully",
      data: transactionData,
    };
  } catch (error) {
    console.error("conversationWithTransaction error:", error);
    return {
      error: true,
      message: error.message,
    };
  }
};

const totalTransactionCount = async (data) => {
  try {
    const { startDate, endDate, iid } = data;

    const sql = `
    SELECT
      COUNT(*) AS "totalCount"
    ${buildFromAndWhere({ startDate, endDate, iid })};
    `;

    const result = await athenaService.executeQuery(sql);

    return Number(result?.[0]?.totalCount || 0);
  } catch (error) {
    console.error("totalTransactionCount error:", error);
    return 0;
  }
};

const getTransactionData = async (data) => {
  const { startDate, endDate, iid } = data;
  const sql = `
  SELECT
    tm.transaction_master_id                                       AS "TransactionId",
    um.user_master_id                                              AS "UserMasterId",
    um.user_name                                                  AS "UserName",
    um.user_mobile_number                                         AS "UserMobileNumber",
    COALESCE(um.user_email_id, '')                                AS "UserEmail",
    date_format(tm.audit_create_time, '%Y-%m-%d %H:%i:%s')         AS "Date",
    its.inst_trx_stage_label                                      AS "Stage",
    smi.source_inst_label                                         AS "SourceName",
    am.agent_name                                                 AS "AgentName",
    COALESCE(osim.label, '')                                      AS "OrderStatus",
    COALESCE(ol.order_data, '')                                   AS "OrderData",
    COALESCE(CAST(om.order_id AS varchar), '')                    AS "OrderId",
    COALESCE(odm.order_data_type, '')                             AS "OrderDataType",
    CASE
      WHEN um.audit_create_time >= TIMESTAMP '${startDate} 00:00:00'
        AND um.audit_create_time <= TIMESTAMP '${endDate} 23:59:59' THEN 'New_User'
      ELSE 'Existing_User'
    END                                                           AS "User_Type",
    COALESCE(ta.trx_attribute_inst_name, '')                      AS "TrxAttributeInstName",
    COALESCE(ii.label, '')                                        AS "TrxAttributeValue",
    COALESCE(CAST(ii.taocsv_inst_id AS varchar), '')              AS "TrxAttributeValueId",
    COALESCE(CAST(tx.trx_attribute_master_inst_id AS varchar), '') AS "TrxAttributeMasterInstId"
  ${buildFromAndWhere({ startDate, endDate, iid })};
  `;

  const result = await athenaService.executeQuery(sql);

  return result;
};

const saveInRedis = async (data, identifier, startDate, endDate, iid) => {
  try {
    console.log("Saving in Redis:", data);
    const redisKey = `trx-conversion:${identifier}:${startDate}:${endDate}:${iid}`;
    await redisClient.hSet(redisKey, { trxData: JSON.stringify(data) });
    console.log("Redis saved:", redisKey);
  } catch (error) {
    console.error("saveInRedis error:", error);
  }
};

const getFromRedis = async (identifier, startDate, endDate, iid) => {
  try {
    const redisKey = `trx-conversion:${identifier}:${startDate}:${endDate}:${iid}`;
    console.log("Redis key:", redisKey);
    const data = await redisClient.hGet(redisKey, "trxData");
    return JSON.parse(data);
  } catch (error) {
    console.error("getFromRedis error:", error);
    return [];
  }
};

const deleteFromRedis = async (identifier, startDate, endDate, iid) => {
  try {
    const redisKey = `trx-conversion:${identifier}:${startDate}:${endDate}:${iid}`;
    await redisClient.del(redisKey);
  } catch (error) {
    console.error("deleteFromRedis error:", error);
  }
};

module.exports = {
  conversationWithTransaction,
  totalTransactionCount,
  getTransactionData,
  saveInRedis,
  getFromRedis,
  deleteFromRedis,
};

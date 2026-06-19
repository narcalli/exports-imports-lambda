const benchmarkHandler = require("./benchmark.handler");
const trendHandler = require("./trend.handler");
const { whatsappMetricsAll, whatsappMetricsFiltered } = require("./whatsapp.handler");
const documentHandler = require("./document.handler");
const conversionHandler = require("./conversion.handler");
// const drilldownHandler = require("./drilldown.handler");

const handlers = {
  benchmark: benchmarkHandler,
  trend: trendHandler,
  "whatsapp-metrics": whatsappMetricsAll,
  "whatsapp-metrics-filtered": whatsappMetricsFiltered,
  "document-metrics": documentHandler,
  "trx-conversion": conversionHandler,
  // "attribute-drilldown": drilldownHandler,
};

function getHandler(typeCode) {
  return handlers[typeCode] || null;
}

module.exports = { getHandler, handlers };

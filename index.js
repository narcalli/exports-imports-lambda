"use strict";

/**
 * Lambda entrypoints — one exported handler per domain, all built from the
 * same generic SQS consumer factory + domain registry.
 *
 * Currently wired: exportHandler  (Lambda handler string: "index.exportHandler")
 * Future:          importHandler  — uncomment once worker/domains/import.js exists.
 */

const { makeSqsConsumer } = require("./worker/consumer");
const registry = require("./worker/registry");

exports.exportHandler = makeSqsConsumer(registry.export, { domain: "export" });

// exports.importHandler = makeSqsConsumer(registry.import, { domain: "import" });

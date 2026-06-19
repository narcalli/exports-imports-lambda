"use strict";

/**
 * Domain registry.
 *
 * One entry per business domain. Each value is a record processor
 * `(sqsRecord) => Promise<void>`. To add a new queue (e.g. imports):
 *
 *   1. create worker/domains/import.js exporting a processRecord fn
 *   2. add `import: require("./domains/import")` below
 *   3. add `exports.importHandler = ...` in index.js
 *   4. add the queue + event source mapping in deploy.sh (DOMAINS list)
 *
 * The consumer factory, IAM, logging and batch-failure handling are all shared
 * — adding a domain is just these four lines of wiring.
 */

module.exports = {
  export: require("./domains/export"),
  // import: require("./domains/import"),   // <-- future, not wired yet
};

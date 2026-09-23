// Lambda-adapted MySQL pool.
//
// Differences from the dashboard-backend original:
//   - Dropped the `superAdminIncidentLog` dependency (logs to console instead),
//     so this file is self-contained inside the Lambda bundle.
//   - Removed the 30s `setInterval` keep-alive ping. In Lambda a background
//     timer keeps the event loop alive and prevents the container from
//     freezing cleanly between invocations; the pool reconnects on demand.
//   - Lowered connectionLimit (Lambda runs many small containers; a high
//     per-container limit multiplied by concurrency would storm the DB).
//     Tune together with the function's reserved concurrency.
//
// Everything else (promisified Connection/Pool, SSL, charset, sql_mode reset)
// matches the source so query behaviour is identical.

const mysql = require("mysql");
const Promise = require("bluebird");

Promise.promisifyAll(require("mysql/lib/Connection").prototype);
Promise.promisifyAll(require("mysql/lib/Pool").prototype);

const POOL_LIMIT = parseInt(process.env.MYSQL_POOL_LIMIT, 10) || 10;

const mysqlConnection = mysql.createPool({
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE,
  connectionLimit: POOL_LIMIT,
  // Bounds how many callers can queue waiting for a free connection once
  // the pool is at connectionLimit, instead of queueing without limit.
  queueLimit: POOL_LIMIT,
  connectTimeout: 60000,
  acquireTimeout: 60000, // only covers connect+ping for a NEW physical
  // connection, not time spent waiting in the pool's internal queue for an
  // existing one to free up — see getConnectionAsync override below.
  timeout: 60000,
  ssl: { rejectUnauthorized: true },
  multipleStatements: true,
  charset: "utf8mb4",
  DBCollat: "utf8mb4_bin",
});

mysqlConnection.on("connection", function (connection) {
  connection.query("SET sql_mode = 0", function (error) {
    if (error) console.log("mysqlConnection sql_mode error", error.message);
  });
});

mysqlConnection.on("error", (error) => {
  console.log("mysqlConnection error", error && error.message);
});

// The pool's internal wait queue has no timeout of its own, so a caller can
// wait forever for a connection to free up. Overriding getConnectionAsync
// here covers every caller without touching each call site.
const ACQUIRE_TIMEOUT_MS = 15000;
const rawGetConnectionAsync = mysqlConnection.getConnectionAsync.bind(mysqlConnection);
mysqlConnection.getConnectionAsync = function timedGetConnectionAsync() {
  return new Promise((resolve, reject) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      reject(
        new Error(
          `Timed out after ${ACQUIRE_TIMEOUT_MS}ms waiting for a MySQL connection from the pool (exports-imports-lambda)`
        )
      );
    }, ACQUIRE_TIMEOUT_MS);

    rawGetConnectionAsync().then(
      (conn) => {
        if (settled) {
          conn.release();
          return;
        }
        settled = true;
        clearTimeout(timer);
        resolve(conn);
      },
      (err) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        reject(err);
      }
    );
  });
};

module.exports = mysqlConnection;

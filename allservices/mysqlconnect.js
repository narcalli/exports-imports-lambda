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

const mysqlConnection = mysql.createPool({
  host: process.env.MYSQL_HOST,
  user: process.env.MYSQL_USER,
  password: process.env.MYSQL_PASSWORD,
  database: process.env.MYSQL_DATABASE,
  connectionLimit: parseInt(process.env.MYSQL_POOL_LIMIT, 10) || 10,
  connectTimeout: 60000,
  acquireTimeout: 60000,
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

module.exports = mysqlConnection;

const mysql = require("mysql");
const Promise = require("bluebird");
Promise.promisifyAll(require("mysql/lib/Connection").prototype);
Promise.promisifyAll(require("mysql/lib/Pool").prototype);

const starrocks = mysql.createPool({
  // host: "a247869de4ae1403b85ca141d0eea8c8-1641999813.ap-south-1.elb.amazonaws.com",
  host: "a7dad999f14d1410788d749d5213c7ad-1763188739.ap-south-1.elb.amazonaws.com",
  user: "root",
  database: "test_db",
  port: 9030,
  // No connectionLimit set above, so the mysql package defaults to 10.
  // Bound the wait queue to the same size instead of leaving it unlimited.
  queueLimit: 10,
});

starrocks.on("connection", function (connection) {
  console.log("starrocks connection");
});

starrocks.on("error", (error) => {
  console.log("starrocks error", error);
});

// The pool's internal wait queue has no timeout of its own, so a caller can
// wait forever for a connection to free up. Overriding getConnectionAsync
// here covers every caller without touching each call site.
const STARROCKS_ACQUIRE_TIMEOUT_MS = 15000;
const rawStarrocksGetConnectionAsync = starrocks.getConnectionAsync.bind(starrocks);
starrocks.getConnectionAsync = function timedGetConnectionAsync() {
  return new Promise((resolve, reject) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      reject(
        new Error(
          `Timed out after ${STARROCKS_ACQUIRE_TIMEOUT_MS}ms waiting for a StarRocks connection from the pool`
        )
      );
    }, STARROCKS_ACQUIRE_TIMEOUT_MS);

    rawStarrocksGetConnectionAsync().then(
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

async function getStarrocksConnection() {
  const conn = await Promise.using(getStarrocksConn(), (conn) => conn);
  return conn;
}

function getStarrocksConn() {
  return starrocks.getConnectionAsync().disposer((conn) => {
    conn.release();
  });
}

module.exports = getStarrocksConnection;

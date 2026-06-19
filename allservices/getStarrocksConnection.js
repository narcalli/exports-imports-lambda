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
});

starrocks.on("connection", function (connection) {
  console.log("starrocks connection");
});

starrocks.on("error", (error) => {
  console.log("starrocks error", error);
});

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

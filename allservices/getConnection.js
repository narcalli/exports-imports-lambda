const Promise = require("bluebird");
const mysqlConnection = require("./mysqlconnect");

async function getConnection() {
  const conn = await Promise.using(getConn(), conn => conn);
  return conn;
}

function getConn() {
  return mysqlConnection.getConnectionAsync().disposer(conn => {
    conn.release();
  });
}

module.exports = getConnection;
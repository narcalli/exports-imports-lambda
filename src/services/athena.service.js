const {
  GetQueryExecutionCommand,
  GetQueryResultsCommand,
  StartQueryExecutionCommand,
  AthenaClient,
} = require("@aws-sdk/client-athena");

class AthenaService {
  constructor() {
    this.athena = new AthenaClient({
      region: process.env.AWS_REGION,
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID_ATHENA,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY_ATHENA,
      },
    });
    this.database = "iceberg_db";
    this.resultsLocation = "s3://ncx-prod-iceberg-results-oct-25";
  }

  async executeQuery(queryString) {
    const execution = await this.athena.send(
      new StartQueryExecutionCommand({
        QueryString: queryString,
        ResultConfiguration: { OutputLocation: this.resultsLocation },
        QueryExecutionContext: { Database: this.database },
      })
    );

    // wait for the query to complete
    let status = "RUNNING";
    let stateChangeReason = "";
    while (status === "RUNNING" || status === "QUEUED") {
      await new Promise((resolve) => setTimeout(resolve, 2000));
      const result = await this.athena.send(
        new GetQueryExecutionCommand({
          QueryExecutionId: execution.QueryExecutionId,
        })
      );
      status = result.QueryExecution.Status.State;
      stateChangeReason = result.QueryExecution.Status.StateChangeReason || "";
      console.log("Status:", status);
    }

    if (status === "SUCCEEDED") {
      // Paginate through all results
      let allRows = [];
      let nextToken = undefined;
      let pageCount = 0;

      do {
        const results = await this.athena.send(
          new GetQueryResultsCommand({
            QueryExecutionId: execution.QueryExecutionId,
            NextToken: nextToken,
            MaxResults: 1000, // Maximum allowed by Athena
          })
        );

        allRows = allRows.concat(results.ResultSet.Rows);
        nextToken = results.NextToken;
        pageCount++;

        if (nextToken) {
          console.log(
            `Fetched page ${pageCount}, total rows so far: ${allRows.length}`
          );
        }
      } while (nextToken);

      console.log(
        `Total rows fetched: ${allRows.length} across ${pageCount} pages`
      );
      return this.#formatAthenaResults(allRows);
    } else {
      console.error("Query failed:", stateChangeReason);
      throw new Error(`Query failed: ${stateChangeReason}`);
    }
  }

  #formatAthenaResults(rows) {
    if (!rows || rows.length === 0) return [];
    const headers = rows[0].Data.map((c) => c.VarCharValue);
    return rows.slice(1).map((r) => {
      const obj = {};
      r.Data.forEach((c, i) => {
        obj[headers[i]] = c.VarCharValue;
      });
      return obj;
    });
  }
}

module.exports = AthenaService;

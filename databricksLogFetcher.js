const { DBSQLClient } = require('@databricks/sql');


async function runQuery(query) {
  const client = new DBSQLClient();

  await client.connect({ token, host, path });
  const session = await client.openSession();

  try {
    const operation = await session.executeStatement(query);
    const results = await operation.fetchAll();
    await operation.close();
    return results;
  } finally {
    await session.close();
    await client.close();
  }
}

async function fetchAllLogsAndResultsForUsecases({ catalog, schema, usecases }) {
  const results = {};
  for (const usecase of usecases) {
    try {
      const reconLogQuery = `SELECT * FROM ${catalog}.${schema}.${usecase}_log_recon ORDER BY timestamp DESC LIMIT 100`;
      const historicalLogQuery = `SELECT * FROM ${catalog}.${schema}.${usecase}_log_historical ORDER BY timestamp DESC LIMIT 100`;
      const reconResultQuery = `SELECT * FROM ${catalog}.${schema}.${usecase}_result_recon ORDER BY execution_date DESC LIMIT 10`;

      const [reconLog, historicalLog, reconResult] = await Promise.all([
        runQuery(reconLogQuery),
        runQuery(historicalLogQuery),
        runQuery(reconResultQuery)
      ]);

      results[usecase] = {
        recon_log: reconLog,
        historical_log: historicalLog,
        recon_result: reconResult
      };
    } catch (err) {
      results[usecase] = { error: `❌ ${err.message}` };
    }
  }

  return results;
}

module.exports = { fetchAllLogsAndResultsForUsecases };
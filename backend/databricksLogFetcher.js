const { DBSQLClient } = require('@databricks/sql');
const path = require('path');
const dotenv = require('dotenv');
dotenv.config({ path: path.join(__dirname, '..', '.env') });

const TOKEN = process.env.TABLE_TOKEN;
const HOST = process.env.TABLE_HOST;
const PATH = process.env.TABLE_PATH;

/**
 * Run a single query against an already-open session.
 * Reuses the session instead of opening a new connection each time.
 */
async function runQueryWithSession(session, query) {
  const operation = await session.executeStatement(query, { runAsync: true });
  try {
    const results = await operation.fetchAll();
    return results;
  } finally {
    await operation.close();
  }
}

/**
 * Fetch recon logs, historical logs, and recon results for all usecases.
 * Opens ONE connection and ONE session for the entire batch, then closes both.
 *
 * @param {object} opts
 * @param {string[]} opts.usecases  - list of usecase names
 * @param {Record<string,{catalog:string,schema:string}>} opts.configs - per-usecase catalog/schema
 */
async function connectWithRetry(maxRetries = 5, delayMs = 15000) {
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const client = new DBSQLClient();
      await client.connect({ token: TOKEN, host: HOST, path: PATH });
      const session = await client.openSession();
      return { client, session };
    } catch (err) {
      if (attempt === maxRetries) throw err;
      console.log(`SQL Warehouse not ready (attempt ${attempt}/${maxRetries}), retrying in ${delayMs/1000}s...`);
      await new Promise(r => setTimeout(r, delayMs));
    }
  }
}

async function fetchAllLogsAndResultsForUsecases({ usecases, configs }) {
  const { client, session } = await connectWithRetry();

  const results = {};

  try {
    for (const usecase of usecases) {
      const { catalog, schema } = (configs && configs[usecase]) || { catalog: 'metadata', schema: 'metadata_schema' };
      try {
        const reconLogQuery      = `SELECT * FROM ${catalog}.${schema}.${usecase}_log_recon ORDER BY timestamp DESC LIMIT 100`;
        const historicalLogQuery = `SELECT * FROM ${catalog}.${schema}.${usecase}_log_historical ORDER BY timestamp DESC LIMIT 100`;
        const reconResultQuery   = `SELECT * FROM ${catalog}.${schema}.${usecase}_result_recon ORDER BY execution_date DESC LIMIT 10`;

        const [reconLog, historicalLog, reconResult] = await Promise.all([
          runQueryWithSession(session, reconLogQuery),
          runQueryWithSession(session, historicalLogQuery),
          runQueryWithSession(session, reconResultQuery)
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
  } finally {
    await session.close();
    await client.close();
  }

  return results;
}

module.exports = { fetchAllLogsAndResultsForUsecases };

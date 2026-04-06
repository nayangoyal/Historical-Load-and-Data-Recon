const { DBSQLClient } = require('@databricks/sql');
require('dotenv').config({ path: require('path').join(__dirname, '..', '.env') });

const token           = process.env.TABLE_TOKEN;
const server_hostname = process.env.TABLE_HOST;
const http_path       = process.env.TABLE_PATH;

async function runTestQuery() {
  const client = new DBSQLClient();

  try {
    await client.connect({
      token,
      host: server_hostname,
      path: http_path
    });

    const session = await client.openSession();

    const queryOperation = await session.executeStatement(
      "SELECT * FROM metadata.metadata_schema.usecasec_log_recon ORDER BY timestamp DESC LIMIT 100",
      { runAsync: true }
    );

    const result = await queryOperation.fetchAll();
    console.table(result);

    await queryOperation.close();
    await session.close();
    await client.close();
  } catch (err) {
    console.error("ERROR:", err);
  }
}

runTestQuery();

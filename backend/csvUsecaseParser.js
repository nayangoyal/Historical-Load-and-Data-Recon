// csvUsecaseParser.js
const axios = require('axios');
const { parse } = require('csv-parse/sync');
const path = require('path');
const dotenv = require('dotenv');
dotenv.config({ path: path.join(__dirname, '..', '.env') });

const PAT_TOKEN = process.env.PAT_TOKEN;
const DATABRICKS_HOST = (process.env.DATABRICKS_HOST || '').replace(/\/$/, '');

/**
 * Downloads the CSV from the Databricks Volume and returns:
 *  - usecases: string[]                   — unique usecase names in order
 *  - configs:  Record<string, {catalog, schema}>  — metadata location per usecase
 *  - rows:     object[]                   — all raw CSV rows (for CSV generation use)
 */
async function extractUsecasesFromCSV(fileName) {
  const volumePath = `/Volumes/h_and_r/intput_file/inputs/${fileName}`;

  const response = await axios.get(
    `${DATABRICKS_HOST}/api/2.0/fs/files${volumePath}`,
    {
      headers: { Authorization: `Bearer ${PAT_TOKEN}` },
      responseType: 'arraybuffer'
    }
  );

  const content = response.data.toString('utf-8');
  const rows = parse(content, { columns: true, skip_empty_lines: true });

  const usecases = [];
  const configs = {};

  for (const row of rows) {
    const usecase = row.usecase?.trim();
    if (!usecase) continue;
    if (!usecases.includes(usecase)) {
      usecases.push(usecase);
      configs[usecase] = {
        catalog: (row.catalog || 'metadata').trim(),
        schema:  (row.schema  || 'metadata_schema').trim()
      };
    }
  }

  return { usecases, configs, rows };
}

module.exports = { extractUsecasesFromCSV };

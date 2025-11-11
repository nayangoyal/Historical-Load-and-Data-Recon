// csvUsecaseParser.js
const axios = require('axios');
const { parse } = require('csv-parse/sync');


async function extractUsecasesFromCSV(fileName) {
  const volumePath = `/Volumes/h_and_r/intput_file/inputs/${fileName}`;
  const response = await axios.get(
    `https://dbc-7fdb4b6d-72d3.cloud.databricks.com/api/2.0/fs/files${volumePath}`,
    {
      headers: {
        Authorization: `Bearer`,
        responseType: 'arraybuffer',
      }
    }
  );

  const content = response.data.toString('utf-8');
  const records = parse(content, { columns: true, skip_empty_lines: true });

  const usecases = [...new Set(records.map(row => row.usecase?.trim()).filter(Boolean))];
  return usecases;
}

module.exports = { extractUsecasesFromCSV };
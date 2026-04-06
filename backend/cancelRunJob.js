const axios = require('axios');
const DATABRICKS_HOST = (process.env.DATABRICKS_HOST || '').replace(/\/$/, '')
const PAT_TOKEN = process.env.PAT_TOKEN


const HEADERS = {
  Authorization: `Bearer ${PAT_TOKEN}`,
  'Content-Type': 'application/json'
};


async function cancelRun(runId) {
  try {
    await axios.post(
      `${DATABRICKS_HOST}/api/2.1/jobs/runs/cancel`,
      { run_id: runId },
      { headers: HEADERS }
    );
    console.log(`🛑 Run ${runId} cancelled successfully.`);
  } catch (err) {
    console.error(`❌ Failed to cancel run ${runId}: ${err.response?.data?.message || err.message}`);
  }
}

module.exports = { cancelRun };
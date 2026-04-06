const axios = require('axios');

async function cancelRun(runId) {
  const host  = (process.env.DATABRICKS_HOST || '').replace(/\/$/, '');
  const token = process.env.PAT_TOKEN;
  const headers = { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' };
  try {
    await axios.post(
      `${host}/api/2.1/jobs/runs/cancel`,
      { run_id: runId },
      { headers }
    );
    console.log(`🛑 Run ${runId} cancelled successfully.`);
  } catch (err) {
    console.error(`❌ Failed to cancel run ${runId}: ${err.response?.data?.message || err.message}`);
  }
}

module.exports = { cancelRun };
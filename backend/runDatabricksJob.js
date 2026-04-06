const axios = require('axios');

// ==== CONFIGURATION ====
const DATABRICKS_HOST = process.env.DATABRICKS_HOST;  // double check if it’s "76eb" or "72eb"
const PAT_TOKEN = process.env.PAT_TOKEN;
const JOB_ID = process.env.JOB_ID;


const HEADERS = {
  Authorization: `Bearer ${PAT_TOKEN}`,
  'Content-Type': 'application/json'
};

// ==== FUNCTION TO TRIGGER JOB ====
async function triggerJob() {
  try {
    const response = await axios.post(
      `${DATABRICKS_HOST}/api/2.1/jobs/run-now`,
      { job_id: JOB_ID },
      { headers: HEADERS }
    );

    const runId = response.data.run_id;
    console.log(`✅ Job triggered. Run ID: ${runId}`);
    return runId;

  } catch (error) {
    console.error("❌ Error triggering job:", error.response?.data || error.message);
    return null;
  }
}

// ==== FUNCTION TO POLL JOB STATUS ====
async function pollJobStatus(parentRunId) {
  let isFinished = false;

  while (!isFinished) {
    await new Promise(res => setTimeout(res, 5000)); // Wait 5s between checks

    try {
      const response = await axios.get(
        `${DATABRICKS_HOST}/api/2.1/jobs/runs/get`,
        {
          headers: HEADERS,
          params: { run_id: parentRunId }
        }
      );

      const state = response.data.state;
      console.log(`[*] Job status: ${state.life_cycle_state}`);

      if (["TERMINATED", "SKIPPED", "INTERNAL_ERROR"].includes(state.life_cycle_state)) {
        isFinished = true;

        if (state.result_state === "SUCCESS") {
          console.log("🎉 Job finished successfully. Fetching task outputs...");
          await fetchTaskOutputs(parentRunId);
        } else {
          console.error(`⚠️ Job did not finish successfully: ${state.result_state}`);
        }
      }

    } catch (err) {
      console.error("❌ Error checking job status:", err.response?.data || err.message);
      break;
    }
  }
}

// ==== FUNCTION TO FETCH TASK OUTPUTS ====
async function fetchTaskOutputs(parentRunId) {
  try {
    const detailRes = await axios.get(
      `${DATABRICKS_HOST}/api/2.1/jobs/runs/get`,
      {
        headers: HEADERS,
        params: { run_id: parentRunId }
      }
    );

    const tasks = detailRes.data.tasks || [];
    if (tasks.length === 0) {
      console.log("[!] No tasks found in this job.");
      return;
    }

    for (const task of tasks) {
      const taskRunId = task.run_id;
      const taskKey = task.task_key;

      console.log(`\n--- Output for Task: ${taskKey} (Run ID: ${taskRunId}) ---`);

      const outputRes = await axios.get(
        `${DATABRICKS_HOST}/api/2.1/jobs/runs/get-output`,
        {
          headers: HEADERS,
          params: { run_id: taskRunId }
        }
      );

      const result = outputRes.data?.notebook_output?.result;
      if (result) {
        try {
          // const parsed = JSON.parse(result);
          console.log("📤 Output:", result);
        } catch (e) {
          console.log("📤 Raw Output (not JSON):", result);
        }
      } else {
        console.log("[i] No output returned from this notebook.");
      }
    }

  } catch (err) {
    console.error("❌ Failed to fetch task outputs:", err.response?.data || err.message);
  }
}

// ==== MAIN ====
(async () => {
  const runId = await triggerJob();
  if (runId) await pollJobStatus(runId);
})();

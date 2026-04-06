// databricksJobRunner.js
const axios = require('axios');
const { extractUsecasesFromCSV } = require('./csvUsecaseParser');
const { fetchAllLogsAndResultsForUsecases } = require('./databricksLogFetcher');
const { cancelRun } = require('./cancelRunJob');
const path = require('path');
const dotenv = require('dotenv');
dotenv.config({ path: path.join(__dirname, '..', '.env') });

const DATABRICKS_HOST = (process.env.DATABRICKS_HOST || '').replace(/\/$/, '');
const PAT_TOKEN = process.env.PAT_TOKEN;
const JOB_ID = process.env.JOB_ID;

const HEADERS = {
  Authorization: `Bearer ${PAT_TOKEN}`,
  'Content-Type': 'application/json'
};

// Terminal states — poll loop exits when either is reached
const TERMINAL_STATES = ['TERMINATED', 'INTERNAL_ERROR', 'SKIPPED'];

async function triggerAndMonitorJob(logCallback, csvFileName, sharedState) {
  let runId = null;
  try {
    const response = await axios.post(
      `${DATABRICKS_HOST}/api/2.1/jobs/run-now`,
      { job_id: JOB_ID, notebook_params: { file_name: csvFileName } },
      { headers: HEADERS }
    );

    runId = response.data.run_id;
    sharedState.currentRunId = runId;
    logCallback(`🚀 Job started — Run ID: ${runId}`);
    await pollJobStatus(runId, logCallback, csvFileName, sharedState);
  } catch (error) {
    logCallback(`❌ Error triggering job: ${error.response?.data?.message || error.message}`);
    if (runId) await cancelRun(runId);
  } finally {
    // Only reset shared state if we still own it (guard against a new job starting after a cancel)
    if (!runId || sharedState.currentRunId === runId) {
      sharedState.isJobRunning = false;
      sharedState.currentRunId = null;
    }
  }
}

async function pollJobStatus(runId, logCallback, csvFileName, sharedState) {
  const seenParentStatus = {};
  const seenChildRunIds  = new Set();
  let isFinished     = false;
  let parentStartTime = null;

  // Build record_id → usecase map so child run logs show usecase name
  let recordIdToUsecase = {};
  try {
    const { usecases, rows } = await extractUsecasesFromCSV(csvFileName);
    rows.forEach((row, idx) => {
      recordIdToUsecase[idx] = row.usecase?.trim() || `row_${idx}`;
    });
  } catch (_) {
    // Non-critical — fall back to iteration index
  }

  while (!isFinished) {
    await new Promise(res => setTimeout(res, 5000));
    try {
      const response = await axios.get(`${DATABRICKS_HOST}/api/2.1/jobs/runs/get`, {
        headers: HEADERS,
        params: { run_id: runId }
      });

      const data = response.data;
      const { life_cycle_state, result_state } = data.state;
      const tasks = data.tasks || [];

      if (!parentStartTime && data.start_time) {
        parentStartTime = data.start_time;
      }

      // Log parent task state changes
      for (const task of tasks) {
        if (!task.start_time) continue;

        const taskKey    = task.task_key;
        const taskRunId  = task.run_id;
        const taskStatus = task.state.life_cycle_state;
        const prevStatus = seenParentStatus[taskRunId];

        if (taskStatus === 'RUNNING' && prevStatus !== 'RUNNING') {
          logCallback(`▶ ${taskKey} — Running`);
          seenParentStatus[taskRunId] = taskStatus;
        }

        if (TERMINAL_STATES.includes(taskStatus) && prevStatus !== taskStatus) {
          const resultState = task.state.result_state || taskStatus;
          const icon = resultState === 'SUCCESS' ? '✅' : '❌';
          logCallback(`${icon} ${taskKey} — ${resultState}`);
          seenParentStatus[taskRunId] = taskStatus;
        }

        // Track child iterations spawned by the for-each task
        if (task.for_each_task) {
          await trackForEachChildRuns(
            taskRunId,
            seenChildRunIds,
            recordIdToUsecase,
            logCallback
          );
        }
      }

      // Exit poll loop on any terminal state (fixes infinite loop on INTERNAL_ERROR)
      if (TERMINAL_STATES.includes(life_cycle_state)) {
        isFinished = true;
        if (result_state === 'SUCCESS') {
          logCallback('✅ All iterations completed successfully. Fetching results...');
          const { usecases, configs } = await extractUsecasesFromCSV(csvFileName);
          const logs = await fetchAllLogsAndResultsForUsecases({ usecases, configs });
          sharedState.finalLogsResult = { usecases, configs, logs };
          logCallback('📊 Results ready.');
        } else {
          logCallback(`❌ Job ended with state: ${life_cycle_state} / ${result_state || 'N/A'}`);
        }
      }
    } catch (err) {
      logCallback(`❌ Polling error: ${err.response?.data?.message || err.message}`);
      if (runId) await cancelRun(runId);
      break;
    }
  }
}

/**
 * Fetch all child runs of the for-each job that belong to this parent execution.
 * Logs each iteration with the usecase name derived from record_id.
 */
async function trackForEachChildRuns(forEachRunId, seenChildRunIds, recordIdToUsecase, logCallback) {
  try {
    // Get the for-each task's run details which includes its iterations
    const resp = await axios.get(`${DATABRICKS_HOST}/api/2.1/jobs/runs/get`, {
      headers: HEADERS,
      params: { run_id: forEachRunId }
    });

    const iterations = resp.data.iterations || [];

    for (const iter of iterations) {
      const iterRunId = iter.run_id;
      if (!iterRunId || seenChildRunIds.has(iterRunId)) continue;

      const iterState = iter.state?.life_cycle_state;

      if (iterState === 'RUNNING') {
        seenChildRunIds.add(iterRunId);
        const idx = iter.for_each_task_iteration?.iteration_index;
        const usecaseName = (idx !== undefined && recordIdToUsecase[idx]) || `iteration_${idx ?? '?'}`;
        logCallback(`▶ ${usecaseName} — Running`);
      }

      if (TERMINAL_STATES.includes(iterState)) {
        seenChildRunIds.add(iterRunId);
        const resultState = iter.state?.result_state || iterState;
        const icon = resultState === 'SUCCESS' ? '✅' : '❌';
        const idx = iter.for_each_task_iteration?.iteration_index;
        const usecaseName = (idx !== undefined && recordIdToUsecase[idx]) || `iteration_${idx ?? '?'}`;
        logCallback(`${icon} ${usecaseName} — ${resultState}`);
      }
    }
  } catch (err) {
    logCallback(`⚠ Could not fetch iterations for run ${forEachRunId}: ${err.message}`);
  }
}

module.exports = { triggerAndMonitorJob };

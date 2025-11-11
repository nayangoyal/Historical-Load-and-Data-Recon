// // databricksJobRunner.js
// const axios = require('axios');
// const { extractUsecasesFromCSV } = require('./csvUsecaseParser');
// const { fetchAllLogsAndResultsForUsecases } = require('./databricksLogFetcher');
// const { cancelRun } = require('./cancelRunJob');


// const HEADERS = {
//   Authorization: `Bearer ${PAT_TOKEN}`,
//   'Content-Type': 'application/json'
// };

// async function triggerAndMonitorJob(logCallback, csvFileName, catalog, schema) {
//   try {
//     const response = await axios.post(
//       `${DATABRICKS_HOST}/api/2.1/jobs/run-now`,
//       { job_id: JOB_ID },
//       { headers: HEADERS }
//     );

//     const runId = response.data.run_id;
//     logCallback(`[${new Date().toLocaleString()}] started — Run triggered by you`);
//     await pollJobStatus(runId, logCallback, csvFileName, catalog, schema);
//   } catch (error) {
//     logCallback(`❌ Error triggering job: ${error.response?.data?.message || error.message}`);
//     await cancelRun(runId);
//   }
// }

// async function pollJobStatus(runId, logCallback, csvFileName, catalog, schema) {
//   let previousState = null;
//   let isFinished = false;

//   while (!isFinished) {
//     await new Promise(res => setTimeout(res, 5000)); // wait 5s

//     try {
//       const response = await axios.get(
//         `${DATABRICKS_HOST}/api/2.1/jobs/runs/get`,
//         { headers: HEADERS, params: { run_id: runId } }
//       );

//       const { life_cycle_state, result_state, state_message } = response.data.state;

//       if (life_cycle_state !== previousState) {
//         previousState = life_cycle_state;

//         const timestamp = new Date().toLocaleString("en-IN", { timeZone: "Asia/Kolkata" });

//         const message = ({
//           "PENDING": "Queued due to reaching max concurrent runs.",
//           "WAITING_FOR_RESOURCES": "Waiting for cluster resources.",
//           "RUNNING": "Notebook execution started.",
//           "TERMINATED": result_state === "SUCCESS"
//             ? "Run succeeded"
//             : `Run failed: ${state_message}`
//         })[life_cycle_state] || state_message;

//         logCallback(`[${timestamp}] ${life_cycle_state} — ${message}`);
//       }

//       if (life_cycle_state === "TERMINATED") {
//         isFinished = true;
//         if (result_state === "SUCCESS") {
//           logCallback("✅ Job completed. Fetching notebook output...");
//           console.log("Respones: ", response.data)
//           await fetchTaskOutputs(runId, logCallback);
//           console.log("gonee1");
          
//           // 🧠 Fetch final result logs
//           const usecases = await extractUsecasesFromCSV(csvFileName);
//           console.log("usecases: ",usecases);
//           const logs = await fetchAllLogsAndResultsForUsecases({
//             catalog,
//             schema,
//             usecases
//           });
          
//           console.log("Logs: ", logs);
//           // Write to server.js memory (or could be saved to cache file)
//           require('./server').finalLogsResult = { usecases, logs };
//           // const serverState = require('./server');
//           // serverState.setFinalLogsResult({ usecases, logs });
//           // serverState.setFinalCatalogSchema(catalog, schema);
//           logCallback("📤 Final logs/results ready. Frontend can now access them.");
//         } else {
//           logCallback(`⚠️ Job failed: ${result_state}`);
//         }
//       }
//     } catch (err) {
//       logCallback(`❌ Error polling job status: ${err.response?.data?.message || err.message}`);
//       await cancelRun(runId);
//       break;
//     }
//   }
// }



// async function fetchTaskOutputs(parentRunId, logCallback) {
//   try {
//     // Step 1: Get parent job run details
//     const detailRes = await axios.get(`${DATABRICKS_HOST}/api/2.1/jobs/runs/get`, {
//       headers: HEADERS,
//       params: { run_id: parentRunId }
//     });

//     const tasks = detailRes.data.tasks || [];

//     for (const task of tasks) {
//       const taskRunId = task.run_id;
//       const taskKey = task.task_key;

//       logCallback(`\n--- Task: ${taskKey} (Run ID: ${taskRunId}) ---`);

//       // Step 2: If this is a for_each_task (like "utility"), list subruns
//       if (taskKey === "utility" && task.for_each_task) {
        
//         // console.log("------------for-each--------", task.for_each_task);
//         // console.log("...............", task.for_each_task.task);
//         // console.log("_____________", task.for_each_task.task.run_job_task);
        
//         console.log("+++++++++++++++++", task.for_each_task.task.run_job_task.job_id);
  
        
//         const for_each_job_id = task.for_each_task.task.run_job_task.job_id
//         // Step 2.1: Get latest run_id
//         const runListResp = await axios.get(`${DATABRICKS_HOST}/api/2.1/jobs/runs/list`, {
//           headers: HEADERS,
//           params: {
//             for_each_job_id,
//             limit: 1
//           }
//         });


//         const latestRunId = runListResp.data.runs[0].run_id;
//         console.log("Latest Run ID:", latestRunId);

//         // Step 2.2: Get run details
//         const detailedRunResp = await axios.get(`${DATABRICKS_HOST}/api/2.1/jobs/runs/get`, {
//           headers: HEADERS,
//           params: {
//             run_id: latestRunId
//           }
//         });
//         console.log("+++++++++++ ::::: ", detailedRunResp);

//         const tasks = detailedRunResp.data.tasks || [];

//         // Step 2.3: For each task, fetch output
//         for (const task of tasks) {
//           const taskKey = task.task_key;
//           const taskRunId = task.run_id;

//           console.log(`\n--- Task: ${taskKey} (Run ID: ${taskRunId}) ---`);

//           const outputResp = await axios.get(`${DATABRICKS_HOST}/api/2.1/jobs/runs/get-output`, {
//             headers: HEADERS,
//             params: {
//               run_id: taskRunId
//             }
//           });

//           if (outputResp.status !== 200) {
//             console.log(`[!] Failed to get output for task ${taskKey}:`, outputResp.data);
//             continue;
//           }

//           const result = outputResp.data?.notebook_output?.result;

//           if (result) {
//             console.log(`For-Each-Task -> ${taskKey}: [✓] Output: ${result}`);
//             logCallback(`For-Each-Task -> ${taskKey}: [✓] Output: ${result}`)
//           } else {
//             console.log(`For-Each-Task -> ${taskKey} No notebook output returned for this task.`);
//             logCallback(`For-Each-Task -> ${taskKey} No notebook output returned for this task.`)
//           }
//         }
//       }

//       // Step 3: Process regular task output
//       const outputRes = await axios.get(`${DATABRICKS_HOST}/api/2.1/jobs/runs/get-output`, {
//         headers: HEADERS,
//         params: { run_id: taskRunId }
//       });

//       const result = outputRes.data?.notebook_output?.result;
//       if (result) {
//         logCallback(`📤 Output: ${result}`);
//       } else {
//         logCallback(`${taskKey} No notebook output found.`);
//       }
//     }



//   } catch (err) {
//     logCallback(`❌ Failed to fetch outputs: ${err.response?.data?.message || err.message}`);
//     await cancelRun(parentRunId); // parentRunId used here
//   }
// }


// module.exports = { triggerAndMonitorJob };






// databricksJobRunner.js
const axios = require('axios');
const { extractUsecasesFromCSV } = require('./csvUsecaseParser');
const { fetchAllLogsAndResultsForUsecases } = require('./databricksLogFetcher');
const { cancelRun } = require('./cancelRunJob');


const HEADERS = {
  Authorization: `Bearer ${PAT_TOKEN}`,
  'Content-Type': 'application/json'
};

async function triggerAndMonitorJob(logCallback, csvFileName, catalog, schema) {
  let runId = null;
  try {
    const response = await axios.post(
      `${DATABRICKS_HOST}/api/2.1/jobs/run-now`,
      { job_id: JOB_ID },
      { headers: HEADERS }
    );

    runId = response.data.run_id;
    logCallback(`[${new Date().toLocaleString()}] started — Run triggered by you`);
    await pollJobStatus(runId, logCallback, csvFileName, catalog, schema);
  } catch (error) {
    logCallback(`❌ Error triggering job: ${error.response?.data?.message || error.message}`);
    if (runId) await cancelRun(runId);
  }
}

async function pollJobStatus(runId, logCallback, csvFileName, catalog, schema) {
  let seenParentStatus = {};
  let seenChildStatus = {};
  let isFinished = false;

  while (!isFinished) {
    await new Promise(res => setTimeout(res, 5000));
    try {
      const response = await axios.get(`${DATABRICKS_HOST}/api/2.1/jobs/runs/get`, {
        headers: HEADERS,
        params: { run_id: runId }
      });

      const data = response.data;
      const { life_cycle_state, result_state, state_message } = data.state;
      const tasks = data.tasks || [];

      for (const task of tasks) {
        if (!task.start_time) continue;

        const taskKey = task.task_key;
        const taskRunId = task.run_id;
        const taskStatus = task.state.life_cycle_state;
        const prevStatus = seenParentStatus[taskRunId];

        // Handle for_each tasks
        let childJobId = null;
        let childRunId = null;

        if (task.for_each_task?.task?.run_job_task?.job_id) {
          childJobId = task.for_each_task.task.run_job_task.job_id;

          const childRunResp = await axios.get(`${DATABRICKS_HOST}/api/2.1/jobs/runs/list`, {
            headers: HEADERS,
            params: { job_id: childJobId, limit: 1 }
          });

          const childRuns = childRunResp.data.runs || [];
          if (childRuns.length) {
            childRunId = childRuns[0].run_id;
          }
        }

        if (taskStatus === 'RUNNING' && prevStatus !== 'RUNNING') {
          logCallback(`${taskKey} Running${childJobId ? ` (child_job_id: ${childJobId}, run_id: ${childRunId})` : ''}`);
          seenParentStatus[taskRunId] = taskStatus;
        }

        if (["TERMINATED", "SUCCESS", "FAILED", "INTERNAL_ERROR", "SKIPPED"].includes(taskStatus) &&
            !["TERMINATED", "SUCCESS", "FAILED", "INTERNAL_ERROR", "SKIPPED"].includes(prevStatus)) {
          logCallback(`${taskKey} Ending`);
          seenParentStatus[taskRunId] = taskStatus;
        }

        if (childRunId) {
          await trackChildTasksInParentLoop(childRunId, taskKey, seenChildStatus, logCallback);
        }
      }

      if (life_cycle_state === "TERMINATED") {
        isFinished = true;
        if (result_state === "SUCCESS") {
          logCallback("✅ Job completed. Fetching notebook output...");
          await fetchTaskOutputs(runId, logCallback);

          const usecases = await extractUsecasesFromCSV(csvFileName);
          const logs = await fetchAllLogsAndResultsForUsecases({ catalog, schema, usecases });
          console.log(usecases);
          console.log(logs);
          
          
          require('./server').finalLogsResult = { usecases, logs };
          logCallback("📤 Final logs/results ready. Frontend can now access them.");
        } else {
          logCallback(`⚠️ Job failed: ${result_state}`);
        }
      }
    } catch (err) {
      logCallback(`❌ Error polling job status: ${err.response?.data?.message || err.message}`);
      await cancelRun(runId);
      break;
    }
  }
}


async function trackChildTasksInParentLoop(childRunId, parentTaskKey, seenChildStatus, logCallback) {
  const childResponse = await axios.get(`${DATABRICKS_HOST}/api/2.1/jobs/runs/get`, {
    headers: HEADERS,
    params: { run_id: childRunId }
  });

  const childTasks = childResponse.data.tasks || [];
  for (const ctask of childTasks) {
    if (!ctask.start_time) continue;

    const ctaskKey = `${parentTaskKey} > ${ctask.task_key}`;
    const ctaskRunId = ctask.run_id;
    const ctaskStatus = ctask.state.life_cycle_state;
    const prevStatus = seenChildStatus[ctaskRunId];

    if (ctaskStatus === "RUNNING" && prevStatus !== "RUNNING") {
      logCallback(`${ctaskKey} Running`);
      seenChildStatus[ctaskRunId] = ctaskStatus;

      while (true) {
        await new Promise(res => setTimeout(res, 3000)); // wait 3 seconds between checks

        const checkRes = await axios.get(`${DATABRICKS_HOST}/api/2.1/jobs/runs/get`, {
          headers: HEADERS,
          params: { run_id: childRunId }
        });

        const updatedChildTasks = checkRes.data.tasks || [];
        const updatedTask = updatedChildTasks.find(t => t.run_id === ctaskRunId);

        if (!updatedTask) continue; // skip if not found

        const currentStatus = updatedTask.state.life_cycle_state;

        if (["TERMINATED", "SUCCESS", "FAILED", "INTERNAL_ERROR", "SKIPPED"].includes(currentStatus)) {
          logCallback(`${ctaskKey} Ending`);
          seenChildStatus[ctaskRunId] = currentStatus;

          const outRes = await axios.get(`${DATABRICKS_HOST}/api/2.1/jobs/runs/get-output`, {
            headers: HEADERS,
            params: { run_id: ctaskRunId }
          });

          const result = outRes.data?.notebook_output?.result;
          logCallback(result
            ? `output of ${ctaskKey}: ${result}`
            : `output of ${ctaskKey}: No output passed`);
          break;
        }
      }
    }
  }
}



async function fetchTaskOutputs(parentRunId, logCallback) {
  const detailRes = await axios.get(`${DATABRICKS_HOST}/api/2.1/jobs/runs/get`, {
    headers: HEADERS,
    params: { run_id: parentRunId }
  });

  const tasks = detailRes.data.tasks || [];
  for (const task of tasks) {
    const taskRunId = task.run_id;
    const taskKey = task.task_key;
    logCallback(`\n--- Task: ${taskKey} (Run ID: ${taskRunId}) ---`);

    if (taskKey === "utility" && task.for_each_task?.task?.run_job_task?.job_id) {
      const childJobId = task.for_each_task.task.run_job_task.job_id;

      const runListResp = await axios.get(`${DATABRICKS_HOST}/api/2.1/jobs/runs/list`, {
        headers: HEADERS,
        params: { job_id: childJobId, limit: 1 }
      });

      const latestRunId = runListResp.data.runs?.[0]?.run_id;
      if (!latestRunId) continue;

      const detailedRunResp = await axios.get(`${DATABRICKS_HOST}/api/2.1/jobs/runs/get`, {
        headers: HEADERS,
        params: { run_id: latestRunId }
      });

      const childTasks = detailedRunResp.data.tasks || [];
      for (const childTask of childTasks) {
        const childKey = childTask.task_key;
        const childRunId = childTask.run_id;

        const outputResp = await axios.get(`${DATABRICKS_HOST}/api/2.1/jobs/runs/get-output`, {
          headers: HEADERS,
          params: { run_id: childRunId }
        });

        const result = outputResp.data?.notebook_output?.result;
        logCallback(result
          ? `For-Each-Task -> ${childKey}: [✓] Output: ${result}`
          : `For-Each-Task -> ${childKey}: No notebook output returned.`);
      }
    }

    const outputRes = await axios.get(`${DATABRICKS_HOST}/api/2.1/jobs/runs/get-output`, {
      headers: HEADERS,
      params: { run_id: taskRunId }
    });

    const result = outputRes.data?.notebook_output?.result;
    logCallback(result
      ? `📤 Output: ${result}`
      : `${taskKey} No notebook output found.`);
  }
}

module.exports = { triggerAndMonitorJob };

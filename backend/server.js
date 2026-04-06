// server.js
const express = require('express');
const multer  = require('multer');
const fs      = require('fs');
const path    = require('path');
const axios   = require('axios');
const cors    = require('cors');
const { DBSQLClient } = require('@databricks/sql');
const { triggerAndMonitorJob }             = require('./databricksJobRunner');
const { extractUsecasesFromCSV }           = require('./csvUsecaseParser');
const { fetchAllLogsAndResultsForUsecases } = require('./databricksLogFetcher');
const { cancelRun }                        = require('./cancelRunJob');
const { connectDB, isMongoReady }          = require('./db');
const History                              = require('./models/History');
const dotenv = require('dotenv');
dotenv.config({ path: path.join(__dirname, '..', '.env') });

// Connect to MongoDB on startup (non-blocking — falls back to JSON if unavailable)
connectDB();

// Ensure uploads directory exists (Render doesn't persist empty dirs from git)
const uploadsDir = path.join(__dirname, 'uploads');
if (!fs.existsSync(uploadsDir)) fs.mkdirSync(uploadsDir, { recursive: true });

const app  = express();
const port = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

// Serve built React frontend (production)
const distPath = path.join(__dirname, '..', 'frontend', 'dist');
if (fs.existsSync(distPath)) {
  app.use(express.static(distPath));
}

// Read env vars dynamically so they're always fresh (avoids cached empty values on Render)
const getEnv = () => ({
  PAT_TOKEN:       process.env.PAT_TOKEN,
  DATABRICKS_HOST: (process.env.DATABRICKS_HOST || '').replace(/\/$/, ''),
  JOB_ID:          process.env.JOB_ID,
  TABLE_TOKEN:     process.env.TABLE_TOKEN,
  TABLE_HOST:      process.env.TABLE_HOST,
  TABLE_PATH:      process.env.TABLE_PATH,
});

// Keep top-level refs for backwards compat with other modules
const PAT_TOKEN        = process.env.PAT_TOKEN;
const DATABRICKS_HOST  = (process.env.DATABRICKS_HOST || '').replace(/\/$/, '');
const JOB_ID           = process.env.JOB_ID;
const TABLE_TOKEN      = process.env.TABLE_TOKEN;
const TABLE_HOST       = process.env.TABLE_HOST;
const TABLE_PATH       = process.env.TABLE_PATH;

const HEADERS = {
  Authorization: `Bearer ${PAT_TOKEN}`,
  'Content-Type': 'application/json'
};

// Numeric Spark/SQL types — used to flag checkbox columns vs free-text columns
const NUMERIC_TYPES = new Set([
  'int', 'bigint', 'long', 'double', 'float', 'decimal',
  'tinyint', 'smallint', 'numeric', 'real'
]);

// Ordered list of all CSV columns (matches the notebook DDL expectations)
const CSV_COLUMNS = [
  'catalog', 'schema', 'container_type', 'modes',
  'source_catalog_name', 'source_schema_name',
  'target_catalog_name', 'target_schema_name',
  'filter_condition', 'date_column_format', 'column_excluded',
  'if_recon_then_statistics_for', 'if_recon_then_checksum_for', 'if_recon_then_data_quality_for',
  'storage_name', 'container_name', 'source_path', 'source_file_format',
  'access_token_adls', 'aws_access_key', 'aws_secret_key', 's3_bucket',
  'snowflake_account', 'snowflake_user', 'snowflake_password', 'snowflake_warehouse',
  'snowflake_database', 'snowflake_schema', 'snowflake_table',
  'gcp_project', 'bigquery_dataset', 'bigquery_table',
  'sql_host', 'sql_port', 'sql_database', 'sql_table', 'sql_username', 'sql_password',
  'api_url', 'api_method', 'api_headers', 'api_auth_token',
  'usecase', 'source_table_name', 'target_table_name'
];

// Shared state — written by jobRunner, read by result endpoints
const sharedState = {
  finalLogsResult: null,
  currentRunId:    null,
  isJobRunning:    false
};
module.exports.sharedState = sharedState;

// ── Results History (MongoDB + JSON fallback) ────────────────────────────────
const HISTORY_FILE = path.join(__dirname, 'results_history.json');

async function loadHistory() {
  if (isMongoReady()) {
    return await History.find().sort({ timestamp: -1 }).lean();
  }
  // Fallback: local JSON file (local dev without MongoDB)
  try { return JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf-8')); }
  catch { return []; }
}

async function saveToHistory(entry) {
  if (isMongoReady()) {
    await History.create(entry);
    return;
  }
  // Fallback: local JSON file
  try {
    const history = JSON.parse(fs.readFileSync(HISTORY_FILE, 'utf-8'));
    history.unshift(entry);
    fs.writeFileSync(HISTORY_FILE, JSON.stringify(history, null, 2));
  } catch {
    fs.writeFileSync(HISTORY_FILE, JSON.stringify([entry], null, 2));
  }
}


// ── Helpers ──────────────────────────────────────────────────────────────────

/** Open a SQL Warehouse session (with retry for cold-start), run one query, return rows, close session. */
async function runWarehouseQuery(sql) {
  const { TABLE_TOKEN, TABLE_HOST, TABLE_PATH } = getEnv();
  let client, session;
  for (let attempt = 1; attempt <= 5; attempt++) {
    try {
      client = new DBSQLClient();
      await client.connect({ token: TABLE_TOKEN, host: TABLE_HOST, path: TABLE_PATH });
      session = await client.openSession();
      break;
    } catch (err) {
      if (attempt === 5) throw err;
      console.log(`SQL Warehouse not ready (attempt ${attempt}/5), retrying in 15s...`);
      await new Promise(r => setTimeout(r, 15000));
    }
  }
  try {
    const op = await session.executeStatement(sql, { runAsync: true });
    try {
      return await op.fetchAll();
    } finally {
      await op.close();
    }
  } finally {
    await session.close();
    await client.close();
  }
}

/** Build a valid CSV cell value — quote if needed, join arrays with comma. */
function csvCell(val) {
  if (Array.isArray(val)) val = val.join(',');
  if (val === undefined || val === null) return '';
  const str = String(val);
  return (str.includes(',') || str.includes('"') || str.includes('\n'))
    ? `"${str.replace(/"/g, '""')}"`
    : str;
}

/** Build a CSV string from an array of entry objects. */
function buildCsv(entries) {
  const header = CSV_COLUMNS.join(',');
  const rows   = entries.map(e => CSV_COLUMNS.map(col => csvCell(e[col])).join(','));
  return [header, ...rows].join('\n');
}

/** Upload a Buffer to a Databricks Volume path. */
async function uploadToVolume(fileName, buffer) {
  const { DATABRICKS_HOST: host, PAT_TOKEN: token } = getEnv();
  if (!host) throw new Error('DATABRICKS_HOST env var is not configured');
  if (!token) throw new Error('PAT_TOKEN env var is not configured');
  const databricksPath = `/Volumes/h_and_r/intput_file/inputs/${fileName}`;
  const url = `${host}/api/2.0/fs/files${databricksPath}?overwrite=true`;
  // Validate URL format before calling axios (gives a clear error if host is malformed)
  try { new URL(url); } catch {
    throw new Error(`DATABRICKS_HOST is invalid — got: "${host}". Must be like https://adb-1234567890.1.azuredatabricks.net`);
  }
  const response = await axios.put(url, buffer, {
    headers: {
      Authorization: `Bearer ${token}`,
      'Content-Type': 'application/octet-stream'
    }
  });
  if (response.status !== 204) {
    throw new Error(`Unexpected upload status: ${response.status}`);
  }
}


// ── SSE — live log stream ─────────────────────────────────────────────────────

let logClients = [];

app.get('/logs/stream', (req, res) => {
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.flushHeaders();

  logClients.push(res);
  req.on('close', () => {
    logClients = logClients.filter(c => c !== res);
  });
});

function sendLogToClients(message) {
  logClients.forEach(res => res.write(`data: ${message}\n\n`));
}


// ── File upload setup ─────────────────────────────────────────────────────────

const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, path.join(__dirname, 'uploads')),
  filename:    (req, file, cb) => cb(null, file.originalname)
});
const upload = multer({ storage });


// ── POST /upload-csv ──────────────────────────────────────────────────────────
// Upload a pre-built CSV file → Databricks Volume → trigger job

app.post('/upload-csv', (req, res, next) => {
  upload.single('csvFile')(req, res, err => {
    if (err) return res.status(400).json({ error: `File upload error: ${err.message}` });
    next();
  });
}, async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'No file uploaded.' });
  }
  if (!req.file.originalname.toLowerCase().endsWith('.csv')) {
    fs.unlinkSync(req.file.path);
    return res.status(400).json({ error: 'Only CSV files are accepted.' });
  }
  if (sharedState.isJobRunning) {
    fs.unlinkSync(req.file.path);
    return res.status(409).json({ error: 'A job is already running. Please wait for it to finish.' });
  }

  try {
    const filePath   = path.join(__dirname, 'uploads', req.file.originalname);
    // Strip trailing empty lines to prevent notebook parsing None table names
    const rawContent = fs.readFileSync(filePath, 'utf-8').replace(/[\r\n]+$/, '');
    const fileBuffer = Buffer.from(rawContent, 'utf-8');

    sharedState.finalLogsResult = null;
    sharedState.isJobRunning    = true;

    await uploadToVolume(req.file.originalname, fileBuffer);

    res.status(200).json({ message: '✅ File uploaded. Job starting...', fileName: req.file.originalname });

    // Fire-and-forget — save to history on completion
    const csvName = req.file.originalname;
    triggerAndMonitorJob(sendLogToClients, csvName, sharedState).then(async () => {
      if (sharedState.finalLogsResult) await saveToHistory({ timestamp: new Date().toISOString(), fileName: csvName, ...sharedState.finalLogsResult });
    });
  } catch (err) {
    sharedState.isJobRunning = false;
    console.error('Upload error:', err.message);
    res.status(500).json({ error: err.message });
  }
});


// ── POST /run-from-form ───────────────────────────────────────────────────────
// Accept JSON usecase config(s), build CSV in-memory, upload, trigger job
//
// Body: single object OR array of objects, each with:
//   Required: usecase, catalog, schema, container_type, modes,
//             source_table_name, target_table_name
//   Optional arrays (joined as comma-separated strings in CSV):
//     column_excluded, if_recon_then_statistics_for,
//     if_recon_then_checksum_for, if_recon_then_data_quality_for
//   All other CSV columns as plain strings.

const FORM_REQUIRED = ['usecase', 'catalog', 'schema', 'container_type', 'modes', 'source_table_name', 'target_table_name'];

app.post('/run-from-form', async (req, res) => {
  if (sharedState.isJobRunning) {
    return res.status(409).json({ error: 'A job is already running. Please wait for it to finish.' });
  }

  const entries = Array.isArray(req.body) ? req.body : [req.body];
  if (entries.length === 0) {
    return res.status(400).json({ error: 'No usecase entries provided.' });
  }

  for (const [i, entry] of entries.entries()) {
    for (const field of FORM_REQUIRED) {
      if (!entry[field]) {
        return res.status(400).json({ error: `Entry ${i + 1}: missing required field '${field}'.` });
      }
    }
  }

  const fileName   = `form_run_${Date.now()}.csv`;
  const csvContent = buildCsv(entries).replace(/[\r\n]+$/, '');

  try {
    sharedState.finalLogsResult = null;
    sharedState.isJobRunning    = true;

    await uploadToVolume(fileName, Buffer.from(csvContent, 'utf-8'));

    res.status(200).json({ message: '✅ Form submitted. Job starting...', fileName });

    triggerAndMonitorJob(sendLogToClients, fileName, sharedState).then(async () => {
      if (sharedState.finalLogsResult) await saveToHistory({ timestamp: new Date().toISOString(), fileName, ...sharedState.finalLogsResult });
    });
  } catch (err) {
    sharedState.isJobRunning = false;
    res.status(500).json({ error: err.message });
  }
});


// ── GET /table-columns ────────────────────────────────────────────────────────
// Returns column names + data types for a Unity Catalog table.
// Marks each column is_numeric so the frontend can render the right control.
//
// Query params: catalog, schema, table

app.get('/table-columns', async (req, res) => {
  const { catalog, schema, table } = req.query;
  if (!catalog || !schema || !table) {
    return res.status(400).json({ error: 'catalog, schema, and table query params are required.' });
  }

  try {
    const rows = await runWarehouseQuery(`DESCRIBE TABLE \`${catalog}\`.\`${schema}\`.\`${table}\``);

    const columns = rows
      .filter(r => r.col_name && !r.col_name.startsWith('#'))  // drop partition-info headers
      .map(r => ({
        name:       r.col_name,
        data_type:  r.data_type,
        is_numeric: NUMERIC_TYPES.has((r.data_type || '').toLowerCase().split('(')[0].trim())
      }));

    res.status(200).json({ catalog, schema, table, columns });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


// ── GET /results/:usecase ─────────────────────────────────────────────────────
// Fetch recon_log, historical_log, and recon_result for a single usecase.
// Query params: catalog (default: metadata), schema (default: metadata_schema)

app.get('/results/:usecase', async (req, res) => {
  const { usecase } = req.params;
  const catalog = req.query.catalog || 'metadata';
  const schema  = req.query.schema  || 'metadata_schema';

  try {
    const data = await fetchAllLogsAndResultsForUsecases({
      usecases: [usecase],
      configs:  { [usecase]: { catalog, schema } }
    });
    res.status(200).json({ usecase, ...data[usecase] });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


// ── GET /final-result ─────────────────────────────────────────────────────────
// Returns the full result set once the running job completes.

app.get('/final-result', (req, res) => {
  if (!sharedState.finalLogsResult) {
    return res.status(202).json({ message: 'Result not ready yet' });
  }
  res.status(200).json(sharedState.finalLogsResult);
});


// ── GET /results-history ──────────────────────────────────────────────────────

app.get('/results-history', async (req, res) => {
  try {
    const history = await loadHistory();
    res.status(200).json(history);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


// ── POST /cancel-job ──────────────────────────────────────────────────────────

app.post('/cancel-job', async (req, res) => {
  if (!sharedState.isJobRunning || !sharedState.currentRunId) {
    return res.status(400).json({ error: 'No job is currently running.' });
  }
  try {
    await cancelRun(sharedState.currentRunId);
    sharedState.isJobRunning = false;
    sendLogToClients(`🛑 Job run ${sharedState.currentRunId} was cancelled by user.`);
    sharedState.currentRunId = null;
    res.status(200).json({ message: 'Job cancelled successfully.' });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


// ── GET /health ───────────────────────────────────────────────────────────────
app.get('/health', (req, res) => {
  const vars = ['DATABRICKS_HOST', 'PAT_TOKEN', 'JOB_ID', 'TABLE_TOKEN', 'TABLE_HOST', 'TABLE_PATH', 'MONGODB_URI'];
  const status = {};
  vars.forEach(v => { status[v] = process.env[v] ? '✅ set' : '❌ MISSING'; });

  // Show sanitised DATABRICKS_HOST so format can be verified without exposing secrets
  const rawHost = process.env.DATABRICKS_HOST || '';
  const cleanHost = rawHost.replace(/\/$/, '');
  let hostOk = false;
  try { new URL(cleanHost); hostOk = true; } catch {}
  status['DATABRICKS_HOST_value'] = cleanHost
    ? `${cleanHost.slice(0, 12)}...${cleanHost.slice(-20)} (${hostOk ? '✅ valid URL' : '❌ NOT a valid URL'})`
    : '(empty)';

  res.json(status);
});

// ── GET /job-status ───────────────────────────────────────────────────────────

app.get('/job-status', async (req, res) => {
  if (!sharedState.currentRunId) {
    return res.status(200).json({ status: 'idle', isJobRunning: false });
  }
  try {
    const { DATABRICKS_HOST: host, PAT_TOKEN: token } = getEnv();
    const response = await axios.get(`${host}/api/2.1/jobs/runs/get`, {
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      params:  { run_id: sharedState.currentRunId }
    });
    const { life_cycle_state, result_state, state_message } = response.data.state;
    res.status(200).json({
      isJobRunning:     sharedState.isJobRunning,
      runId:            sharedState.currentRunId,
      life_cycle_state,
      result_state:     result_state   || null,
      state_message:    state_message  || null
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


// ── GET /run-history ──────────────────────────────────────────────────────────

app.get('/run-history', async (req, res) => {
  const limit = parseInt(req.query.limit) || 10;
  try {
    const { DATABRICKS_HOST: host, PAT_TOKEN: token, JOB_ID: jobId } = getEnv();
    const response = await axios.get(`${host}/api/2.1/jobs/runs/list`, {
      headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
      params:  { job_id: jobId, limit }
    });
    const runs = (response.data.runs || []).map(run => ({
      run_id:           run.run_id,
      start_time:       run.start_time,
      end_time:         run.end_time         || null,
      life_cycle_state: run.state?.life_cycle_state,
      result_state:     run.state?.result_state || null,
      run_duration:     run.run_duration     || null
    }));
    res.status(200).json({ runs });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});


// ── GET /fetch-all-logs/:csvFile ──────────────────────────────────────────────
// Download a CSV from the volume, parse it, fetch all logs for all usecases in it.

app.get('/fetch-all-logs/:csvFile', async (req, res) => {
  const { csvFile } = req.params;
  try {
    const { usecases, configs } = await extractUsecasesFromCSV(csvFile);
    const logs = await fetchAllLogsAndResultsForUsecases({ usecases, configs });
    res.status(200).json({ usecases, logs });
  } catch (err) {
    console.error('Fetch logs error:', err.message);
    res.status(500).json({ error: err.message });
  }
});


// ── Global Error Handler ──────────────────────────────────────────────────────
// Catches multer/busboy parse errors (e.g. "Unexpected end of form") and any
// other unhandled errors from middleware, returning consistent JSON instead of
// Express's default HTML error page.

app.use((err, req, res, next) => {
  // Multer/busboy errors → 400
  if (
    err.message === 'Unexpected end of form' ||
    (err.code && err.code.toString().startsWith('LIMIT_'))
  ) {
    return res.status(400).json({ error: 'No file uploaded.' });
  }
  // Everything else → 500
  console.error('Unhandled error:', err.message);
  res.status(500).json({ error: err.message });
});


// ── Catch-all: serve React app for any non-API route ─────────────────────────
if (fs.existsSync(distPath)) {
  app.get('/{*path}', (req, res) => res.sendFile(path.join(distPath, 'index.html')));
}

// ── Start Server ──────────────────────────────────────────────────────────────

app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});

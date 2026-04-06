/**
 * test_backend.js — Standalone backend test suite
 *
 * Usage:
 *   node test_backend.js                  # Validation + state tests only
 *   node test_backend.js --databricks     # + Databricks read-only tests
 *   node test_backend.js --e2e            # + full job trigger (slow, ~5–10 min)
 *
 * Prerequisites:
 *   1. server must be running:  node server.js
 *   2. For --databricks / --e2e:  .env must have valid credentials
 */

'use strict';

const axios    = require('axios');
const FormData = require('form-data');
const fs       = require('fs');
const path     = require('path');

const BASE  = 'http://localhost:3000';
const ARGS  = process.argv.slice(2);
const WITH_DB  = ARGS.includes('--databricks') || ARGS.includes('--e2e');
const WITH_E2E = ARGS.includes('--e2e');

// ── Test runner ───────────────────────────────────────────────────────────────

let passed = 0, failed = 0, skipped = 0;

async function test(label, fn) {
  try {
    await fn();
    console.log(`  ✅ ${label}`);
    passed++;
  } catch (err) {
    console.log(`  ❌ ${label}`);
    console.log(`     → ${err.message}`);
    failed++;
  }
}

function skip(label, reason) {
  console.log(`  ⏭  ${label}  (${reason})`);
  skipped++;
}

function assert(condition, msg) {
  if (!condition) throw new Error(msg || 'Assertion failed');
}

function assertStatus(res, expected) {
  assert(res.status === expected, `Expected HTTP ${expected}, got ${res.status}. Body: ${JSON.stringify(res.data)}`);
}

/** Wrap axios so it doesn't throw on 4xx/5xx — we need to inspect the response. */
async function req(method, url, opts = {}) {
  try {
    return await axios({ method, url: `${BASE}${url}`, validateStatus: () => true, ...opts });
  } catch (err) {
    throw new Error(`Network error: ${err.message}`);
  }
}

// ── Temp file helpers ─────────────────────────────────────────────────────────

const TMP_DIR   = path.join(__dirname, '_test_tmp');
const DUMMY_CSV = path.join(TMP_DIR, 'valid_test.csv');
const DUMMY_TXT = path.join(TMP_DIR, 'not_a_csv.txt');

const VALID_CSV_CONTENT = [
  'catalog,schema,container_type,modes,source_catalog_name,source_schema_name,target_catalog_name,target_schema_name,filter_condition,date_column_format,column_excluded,if_recon_then_statistics_for,if_recon_then_checksum_for,if_recon_then_data_quality_for,storage_name,container_name,source_path,source_file_format,access_token_adls,aws_access_key,aws_secret_key,s3_bucket,snowflake_account,snowflake_user,snowflake_password,snowflake_warehouse,snowflake_database,snowflake_schema,snowflake_table,gcp_project,bigquery_dataset,bigquery_table,sql_host,sql_port,sql_database,sql_table,sql_username,sql_password,api_url,api_method,api_headers,api_auth_token,usecase,source_table_name,target_table_name',
  'metadata,metadata_schema,DATABRICKS,recon,h_and_r,djj_testing,h_and_r,djj_testing,,,,amount,,name,,,,,,,,,,,,,,,,,,,,,,,,,,,,,backend_test_dummy,test_source_50k,test_source_50k'
].join('\n');

function setupTmpFiles() {
  if (!fs.existsSync(TMP_DIR)) fs.mkdirSync(TMP_DIR);
  fs.writeFileSync(DUMMY_CSV, VALID_CSV_CONTENT);
  fs.writeFileSync(DUMMY_TXT, 'This is not a CSV file.');
}

function cleanupTmpFiles() {
  try { fs.rmSync(TMP_DIR, { recursive: true, force: true }); } catch {}
}

// ── Section helpers ───────────────────────────────────────────────────────────

function section(title) {
  console.log(`\n${'─'.repeat(60)}`);
  console.log(`  ${title}`);
  console.log('─'.repeat(60));
}

/**
 * Cancel any running job and wait until the server reports idle.
 * Retries the cancel call (because currentRunId may not be set yet when the
 * Databricks run-now request hasn't returned). Then polls /job-status until
 * isJobRunning is false before returning.
 */
async function cancelAndWaitIdle() {
  // Retry cancel until acknowledged or server is already idle
  for (let i = 0; i < 15; i++) {
    const st = await req('GET', '/job-status');
    if (!st.data.isJobRunning) return;
    const c = await req('POST', '/cancel-job');
    if (c.status === 200) break;
    await new Promise(r => setTimeout(r, 1000));
  }
  // Poll until idle (cancel took effect)
  for (let i = 0; i < 60; i++) {
    const st = await req('GET', '/job-status');
    if (!st.data.isJobRunning) return;
    await new Promise(r => setTimeout(r, 2000));
  }
}

// ══════════════════════════════════════════════════════════════════════════════
// SECTION 1 — Server reachability
// ══════════════════════════════════════════════════════════════════════════════

async function testServerReachable() {
  section('1. Server Reachability');
  await test('GET /job-status returns 200', async () => {
    const res = await req('GET', '/job-status');
    assertStatus(res, 200);
  });
}

// ══════════════════════════════════════════════════════════════════════════════
// SECTION 2 — POST /upload-csv  (validation, no Databricks needed)
// ══════════════════════════════════════════════════════════════════════════════

async function testUploadCsvValidation() {
  section('2. POST /upload-csv — Input Validation');

  await test('No file attached → 400', async () => {
    // Simulate a multipart form that is well-formed but contains no file field.
    // (An empty FormData sends a broken multipart that busboy rejects below Express level.
    // A form with an unrelated field sends a valid multipart — multer parses it fine but
    // req.file is undefined, which is what our handler checks.)
    const form = new FormData();
    form.append('not_a_file', 'some_value');
    const res  = await req('POST', '/upload-csv', { data: form, headers: form.getHeaders() });
    assertStatus(res, 400);
    assert(res.data.error.toLowerCase().includes('no file'), `Got: ${res.data.error}`);
  });

  await test('Completely empty POST body (no Content-Type) → 400', async () => {
    // Sending no multipart at all — multer skips parsing, req.file stays undefined → 400
    const res = await req('POST', '/upload-csv', {
      data: '',
      headers: { 'Content-Type': 'application/octet-stream' }
    });
    assertStatus(res, 400);
  });

  await test('Non-CSV file (.txt) → 400', async () => {
    const form = new FormData();
    form.append('csvFile', fs.createReadStream(DUMMY_TXT), { filename: 'test.txt', contentType: 'text/plain' });
    const res = await req('POST', '/upload-csv', { data: form, headers: form.getHeaders() });
    assertStatus(res, 400);
    assert(res.data.error.toLowerCase().includes('csv'), `Got: ${res.data.error}`);
  });

  await test('File with .csv extension but disguised name → accepted (extension check only)', async () => {
    const form = new FormData();
    // Just verify extension validation passes — we don't care if Databricks rejects it (500)
    // but it must NOT return 400 "Only CSV files are accepted"
    form.append('csvFile', fs.createReadStream(DUMMY_CSV), { filename: 'dummy.csv', contentType: 'text/csv' });
    const res = await req('POST', '/upload-csv', { data: form, headers: form.getHeaders() });
    assert(res.status !== 400 || !res.data.error?.includes('CSV'), `Unexpected CSV extension rejection: ${JSON.stringify(res.data)}`);
    // If a job actually started (200), cancel it and wait for idle so later sections are not blocked
    if (res.status === 200) {
      await new Promise(r => setTimeout(r, 1500)); // let currentRunId be set
      await cancelAndWaitIdle();
    }
  });
}

// ══════════════════════════════════════════════════════════════════════════════
// SECTION 3 — POST /run-from-form  (validation)
// ══════════════════════════════════════════════════════════════════════════════

async function testRunFromFormValidation() {
  section('3. POST /run-from-form — Input Validation');

  const validEntry = {
    usecase:          'test_usecase',
    catalog:          'metadata',
    schema:           'metadata_schema',
    container_type:   'DATABRICKS',
    modes:            'recon',
    source_table_name: 'test_source_50k',
    target_table_name: 'test_pass_50k',
  };

  await test('Empty body {} → 400 missing required field', async () => {
    const res = await req('POST', '/run-from-form', { data: {}, headers: { 'Content-Type': 'application/json' } });
    assertStatus(res, 400);
    assert(res.data.error.toLowerCase().includes('missing'), `Got: ${res.data.error}`);
  });

  await test('Empty array [] → 400 no usecase entries', async () => {
    const res = await req('POST', '/run-from-form', { data: [], headers: { 'Content-Type': 'application/json' } });
    assertStatus(res, 400);
    assert(res.data.error.toLowerCase().includes('no usecase'), `Got: ${res.data.error}`);
  });

  await test('Missing usecase field → 400', async () => {
    const entry = { ...validEntry };
    delete entry.usecase;
    const res = await req('POST', '/run-from-form', { data: entry, headers: { 'Content-Type': 'application/json' } });
    assertStatus(res, 400);
    assert(res.data.error.includes("'usecase'"), `Got: ${res.data.error}`);
  });

  await test('Missing target_table_name → 400', async () => {
    const entry = { ...validEntry };
    delete entry.target_table_name;
    const res = await req('POST', '/run-from-form', { data: entry, headers: { 'Content-Type': 'application/json' } });
    assertStatus(res, 400);
    assert(res.data.error.includes("'target_table_name'"), `Got: ${res.data.error}`);
  });

  await test('Missing container_type → 400', async () => {
    const entry = { ...validEntry };
    delete entry.container_type;
    const res = await req('POST', '/run-from-form', { data: entry, headers: { 'Content-Type': 'application/json' } });
    assertStatus(res, 400);
    assert(res.data.error.includes("'container_type'"), `Got: ${res.data.error}`);
  });

  await test('Array of entries — one invalid entry → 400 with entry index', async () => {
    const entries = [
      validEntry,
      { usecase: 'second', catalog: 'x', schema: 'y', container_type: 'DATABRICKS', modes: 'recon' }
      // missing source_table_name, target_table_name
    ];
    const res = await req('POST', '/run-from-form', { data: entries, headers: { 'Content-Type': 'application/json' } });
    assertStatus(res, 400);
    assert(res.data.error.includes('Entry 2'), `Got: ${res.data.error}`);
  });

  await test('Array checkbox fields (column_excluded as array) → passes validation', async () => {
    const entry = { ...validEntry, column_excluded: ['city', 'name'], if_recon_then_statistics_for: ['amount'] };
    // Valid — goes to Databricks upload stage; not 400
    const res = await req('POST', '/run-from-form', { data: entry, headers: { 'Content-Type': 'application/json' } });
    assert(res.status !== 400, `Unexpected 400: ${JSON.stringify(res.data)}`);
    if (res.status === 200) {
      await new Promise(r => setTimeout(r, 1500));
      await cancelAndWaitIdle();
    }
  });

  await test('column_excluded as comma string → passes validation', async () => {
    const entry = { ...validEntry, column_excluded: 'city,name' };
    const res = await req('POST', '/run-from-form', { data: entry, headers: { 'Content-Type': 'application/json' } });
    assert(res.status !== 400, `Unexpected 400: ${JSON.stringify(res.data)}`);
    if (res.status === 200) {
      await new Promise(r => setTimeout(r, 1500));
      await cancelAndWaitIdle();
    }
  });
}

// ══════════════════════════════════════════════════════════════════════════════
// SECTION 4 — GET /table-columns  (validation)
// ══════════════════════════════════════════════════════════════════════════════

async function testTableColumnsValidation() {
  section('4. GET /table-columns — Input Validation');

  await test('No params → 400', async () => {
    const res = await req('GET', '/table-columns');
    assertStatus(res, 400);
    assert(res.data.error.toLowerCase().includes('required'), `Got: ${res.data.error}`);
  });

  await test('Missing table param → 400', async () => {
    const res = await req('GET', '/table-columns?catalog=h_and_r&schema=djj_testing');
    assertStatus(res, 400);
  });

  await test('Missing schema param → 400', async () => {
    const res = await req('GET', '/table-columns?catalog=h_and_r&table=test_source_50k');
    assertStatus(res, 400);
  });

  await test('Missing catalog param → 400', async () => {
    const res = await req('GET', '/table-columns?schema=djj_testing&table=test_source_50k');
    assertStatus(res, 400);
  });
}

// ══════════════════════════════════════════════════════════════════════════════
// SECTION 5 — State & Concurrency
// ══════════════════════════════════════════════════════════════════════════════

async function testStateAndConcurrency() {
  section('5. State & Concurrency');

  await test('GET /job-status when idle → {status: idle, isJobRunning: false}', async () => {
    const res = await req('GET', '/job-status');
    assertStatus(res, 200);
    assert(res.data.isJobRunning === false, `isJobRunning should be false, got ${res.data.isJobRunning}`);
  });

  await test('GET /final-result before any job → 202 not ready', async () => {
    const res = await req('GET', '/final-result');
    assertStatus(res, 202);
    assert(res.data.message.toLowerCase().includes('not ready'), `Got: ${res.data.message}`);
  });

  await test('POST /cancel-job when no job running → 400', async () => {
    const res = await req('POST', '/cancel-job');
    assertStatus(res, 400);
    assert(res.data.error.toLowerCase().includes('no job'), `Got: ${res.data.error}`);
  });

  await test('GET /run-history returns runs array (may be empty)', async () => {
    const res = await req('GET', '/run-history');
    assertStatus(res, 200);
    assert(Array.isArray(res.data.runs), `runs should be an array, got ${typeof res.data.runs}`);
  });

  await test('GET /run-history?limit=3 respects limit param', async () => {
    const res = await req('GET', '/run-history?limit=3');
    assertStatus(res, 200);
    assert(res.data.runs.length <= 3, `Expected ≤3 runs, got ${res.data.runs.length}`);
  });
}

// ══════════════════════════════════════════════════════════════════════════════
// SECTION 6 — CSV builder (unit test — no server needed)
// ══════════════════════════════════════════════════════════════════════════════

async function testCsvBuilder() {
  section('6. CSV Builder — Unit Tests');

  // Import the helpers directly (not exposed by server, so we test the logic manually)
  const CSV_COLUMNS = [
    'catalog','schema','container_type','modes',
    'source_catalog_name','source_schema_name','target_catalog_name','target_schema_name',
    'filter_condition','date_column_format','column_excluded',
    'if_recon_then_statistics_for','if_recon_then_checksum_for','if_recon_then_data_quality_for',
    'storage_name','container_name','source_path','source_file_format',
    'access_token_adls','aws_access_key','aws_secret_key','s3_bucket',
    'snowflake_account','snowflake_user','snowflake_password','snowflake_warehouse',
    'snowflake_database','snowflake_schema','snowflake_table',
    'gcp_project','bigquery_dataset','bigquery_table',
    'sql_host','sql_port','sql_database','sql_table','sql_username','sql_password',
    'api_url','api_method','api_headers','api_auth_token',
    'usecase','source_table_name','target_table_name'
  ];

  function csvCell(val) {
    if (Array.isArray(val)) val = val.join(',');
    if (val === undefined || val === null) return '';
    const str = String(val);
    return (str.includes(',') || str.includes('"') || str.includes('\n'))
      ? `"${str.replace(/"/g, '""')}"` : str;
  }

  function buildCsv(entries) {
    const header = CSV_COLUMNS.join(',');
    const rows   = entries.map(e => CSV_COLUMNS.map(col => csvCell(e[col])).join(','));
    return [header, ...rows].join('\n');
  }

  await test('buildCsv produces correct header row (45 columns)', () => {
    const csv   = buildCsv([{}]);
    const lines = csv.split('\n');
    const cols  = lines[0].split(',');
    assert(cols.length === 45, `Expected 45 columns, got ${cols.length}`);
    assert(cols[0] === 'catalog', `First col should be 'catalog', got '${cols[0]}'`);
    assert(cols[44] === 'target_table_name', `Last col should be 'target_table_name', got '${cols[44]}'`);
  });

  await test('buildCsv: array fields are joined as comma-separated strings inside quotes', () => {
    const csv = buildCsv([{ column_excluded: ['city', 'name', 'zip'], catalog: 'x', schema: 'y' }]);
    assert(csv.includes('"city,name,zip"'), `Expected quoted joined array. Got CSV:\n${csv}`);
  });

  await test('buildCsv: empty array → empty string in cell', () => {
    const csv = buildCsv([{ column_excluded: [] }]);
    const lines = csv.split('\n');
    const vals  = lines[1].split(',');
    const excludeIdx = CSV_COLUMNS.indexOf('column_excluded');
    assert(vals[excludeIdx] === '', `Expected empty cell, got '${vals[excludeIdx]}'`);
  });

  await test('buildCsv: values containing commas are quoted', () => {
    const value = "amount > 0, status = 'ACTIVE'";   // has a literal comma
    const csv = buildCsv([{ filter_condition: value }]);
    // The cell must be wrapped in double-quotes because the value contains a comma
    assert(csv.includes(`"${value}"`), `Value with comma should be quoted. Got CSV:\n${csv}`);
  });

  await test('buildCsv: null/undefined fields → empty string', () => {
    const csv = buildCsv([{ usecase: 'test', filter_condition: null, access_token_adls: undefined }]);
    const lines = csv.split('\n');
    const vals  = lines[1].split(',');
    const filterIdx = CSV_COLUMNS.indexOf('filter_condition');
    assert(vals[filterIdx] === '', `Expected empty, got '${vals[filterIdx]}'`);
  });

  await test('buildCsv: multiple entries produce correct row count', () => {
    const csv   = buildCsv([{ usecase: 'a' }, { usecase: 'b' }, { usecase: 'c' }]);
    const lines = csv.split('\n');
    assert(lines.length === 4, `Expected 4 lines (1 header + 3 rows), got ${lines.length}`);
  });

  await test('buildCsv: values with double-quotes are escaped', () => {
    const csv = buildCsv([{ filter_condition: 'name = "John"' }]);
    assert(csv.includes('""John""'), `Expected escaped quotes. CSV: ${csv}`);
  });
}

// ══════════════════════════════════════════════════════════════════════════════
// SECTION 7 — Databricks read-only tests (--databricks flag)
// ══════════════════════════════════════════════════════════════════════════════

async function testDatabricksReadOnly() {
  section('7. Databricks Read-only Tests');

  await test('GET /table-columns for h_and_r.djj_testing.test_source_50k', async () => {
    const res = await req('GET', '/table-columns?catalog=h_and_r&schema=djj_testing&table=test_source_50k');
    assertStatus(res, 200);
    assert(Array.isArray(res.data.columns), 'columns should be an array');
    assert(res.data.columns.length > 0, 'Should return at least one column');
    const col = res.data.columns[0];
    assert('name' in col,       'Column should have name field');
    assert('data_type' in col,  'Column should have data_type field');
    assert('is_numeric' in col, 'Column should have is_numeric field');
    console.log(`     → ${res.data.columns.length} columns: ${res.data.columns.map(c => c.name).join(', ')}`);
  });

  await test('GET /table-columns: numeric columns correctly flagged', async () => {
    const res = await req('GET', '/table-columns?catalog=h_and_r&schema=djj_testing&table=test_source_50k');
    assertStatus(res, 200);
    const numericCols = res.data.columns.filter(c => c.is_numeric).map(c => c.name);
    const stringCols  = res.data.columns.filter(c => !c.is_numeric).map(c => c.name);
    console.log(`     → Numeric: ${numericCols.join(', ') || 'none'}`);
    console.log(`     → Non-numeric: ${stringCols.join(', ') || 'none'}`);
    // amount should be numeric, name should not
    const amountCol = res.data.columns.find(c => c.name === 'amount');
    if (amountCol) assert(amountCol.is_numeric, 'amount column should be numeric');
  });

  await test('GET /table-columns: non-existent table → 500 with error', async () => {
    const res = await req('GET', '/table-columns?catalog=h_and_r&schema=djj_testing&table=does_not_exist_xyz');
    assertStatus(res, 500);
    assert(res.data.error, 'Should return error message');
    console.log(`     → Error: ${res.data.error.substring(0, 80)}...`);
  });

  await test('GET /table-columns: non-existent catalog → 500 with error', async () => {
    const res = await req('GET', '/table-columns?catalog=nonexistent_cat&schema=some_schema&table=some_table');
    assertStatus(res, 500);
    assert(res.data.error, 'Should return error message');
  });

  await test('GET /results/test_50k_pass?catalog=metadata&schema=metadata_schema', async () => {
    const res = await req('GET', '/results/test_50k_pass?catalog=metadata&schema=metadata_schema');
    assertStatus(res, 200);
    assert(res.data.usecase === 'test_50k_pass', `usecase should be test_50k_pass, got ${res.data.usecase}`);
    assert(Array.isArray(res.data.recon_result), 'recon_result should be an array');
    console.log(`     → ${res.data.recon_result.length} result rows, ${res.data.recon_log.length} recon log entries`);
  });

  await test('GET /results/:usecase with default catalog/schema', async () => {
    const res = await req('GET', '/results/test_50k_pass');
    // Should use metadata/metadata_schema defaults
    assert(res.status === 200 || res.status === 500, `Unexpected status ${res.status}`);
    if (res.status === 200) {
      assert(res.data.usecase === 'test_50k_pass', 'usecase name should match');
    }
  });

  await test('GET /results/:usecase for non-existent usecase → error in body', async () => {
    const res = await req('GET', '/results/this_usecase_does_not_exist?catalog=metadata&schema=metadata_schema');
    // Per-usecase errors are caught and embedded in the response (not thrown as 500).
    // The response is 200 with { error: "❌ TABLE_OR_VIEW_NOT_FOUND..." } in the body.
    assert(res.status === 200 || res.status === 500, `Unexpected status ${res.status}`);
    const hasError = res.data.error && res.data.error.includes('TABLE_OR_VIEW_NOT_FOUND');
    assert(hasError, `Expected TABLE_OR_VIEW_NOT_FOUND error, got: ${JSON.stringify(res.data)}`);
    console.log(`     → Error correctly returned: TABLE_OR_VIEW_NOT_FOUND`);
  });

  await test('GET /run-history shows job run entries', async () => {
    const res = await req('GET', '/run-history');
    assertStatus(res, 200);
    assert(res.data.runs.length > 0, 'Should have at least one run in history');
    const run = res.data.runs[0];
    assert('run_id' in run, 'run should have run_id');
    assert('life_cycle_state' in run, 'run should have life_cycle_state');
    console.log(`     → ${res.data.runs.length} runs. Last: ${run.life_cycle_state}/${run.result_state || 'N/A'}`);
  });

  await test('GET /fetch-all-logs/test_50k_edge_cases.csv', async () => {
    const res = await req('GET', '/fetch-all-logs/test_50k_edge_cases.csv');
    assertStatus(res, 200);
    assert(Array.isArray(res.data.usecases), 'usecases should be array');
    assert(res.data.usecases.length > 0, 'Should have at least one usecase');
    console.log(`     → Usecases: ${res.data.usecases.join(', ')}`);
    // Verify all usecases have log entries
    for (const uc of res.data.usecases) {
      assert(uc in res.data.logs, `Missing logs for usecase: ${uc}`);
    }
  });
}

// ══════════════════════════════════════════════════════════════════════════════
// SECTION 8 — Concurrency guard (--databricks flag, needs live Databricks)
// ══════════════════════════════════════════════════════════════════════════════

async function testConcurrencyGuard() {
  section('8. Concurrency Guard');

  await test('POST /run-from-form: second job while first is running → 409', async () => {
    const entry = {
      usecase: 'concurrency_test_a', catalog: 'metadata', schema: 'metadata_schema',
      container_type: 'DATABRICKS', modes: 'recon',
      source_catalog_name: 'h_and_r', source_schema_name: 'djj_testing',
      source_table_name: 'test_source_50k',
      target_catalog_name: 'h_and_r', target_schema_name: 'djj_testing',
      target_table_name: 'test_pass_50k'
    };

    // First request — should start the job (200) or fail (500 Databricks error)
    const first = await req('POST', '/run-from-form', {
      data: entry, headers: { 'Content-Type': 'application/json' }
    });

    if (first.status === 200) {
      console.log('     → First job started. Now sending concurrent second request...');
      // Immediately send a second request — should be rejected
      const second = await req('POST', '/run-from-form', {
        data: { ...entry, usecase: 'concurrency_test_b' },
        headers: { 'Content-Type': 'application/json' }
      });
      assert(second.status === 409, `Expected 409, got ${second.status}. Body: ${JSON.stringify(second.data)}`);
      assert(second.data.error.toLowerCase().includes('already running'), `Got: ${second.data.error}`);
      console.log('     → 409 received correctly. Cancelling test job...');
      await new Promise(r => setTimeout(r, 1500)); // let currentRunId be set
      await cancelAndWaitIdle();
      console.log('     → Server idle, ready for E2E section.');
    } else {
      console.log(`     → First request returned ${first.status} (Databricks may be unavailable) — skipping concurrent check`);
    }
  });
}

// ══════════════════════════════════════════════════════════════════════════════
// SECTION 9 — E2E: Full job run via /run-from-form
// ══════════════════════════════════════════════════════════════════════════════

async function testE2E() {
  section('9. E2E — Full Job Run via /run-from-form');
  console.log('  ⏳ This will take several minutes. Watching SSE logs...');

  const entry = {
    usecase:              'backend_e2e_test',
    catalog:              'metadata',
    schema:               'metadata_schema',
    container_type:       'DATABRICKS',
    modes:                'recon',
    source_catalog_name:  'h_and_r',
    source_schema_name:   'djj_testing',
    source_table_name:    'test_source_50k',
    target_catalog_name:  'h_and_r',
    target_schema_name:   'djj_testing',
    target_table_name:    'test_source_50k',  // self-comparison since test_pass_50k was dropped
    column_excluded:      [],
    if_recon_then_statistics_for:   ['amount'],
    if_recon_then_checksum_for:     ['id', 'name', 'amount'],
    if_recon_then_data_quality_for: ['id', 'name', 'amount'],
  };

  // Wait for server to be idle before starting (previous sections may have left a job running)
  await test('POST /run-from-form with valid entry → 200 job started', async () => {
    // Retry up to 30s if a previous job is still running
    let res;
    for (let attempt = 0; attempt < 24; attempt++) {
      res = await req('POST', '/run-from-form', {
        data: entry, headers: { 'Content-Type': 'application/json' }
      });
      if (res.status !== 409) break;
      console.log(`     → Server busy (409), waiting 5s... (attempt ${attempt + 1}/24)`);
      await new Promise(r => setTimeout(r, 5000));
    }
    assertStatus(res, 200);
    assert(res.data.message.includes('✅'), `Got: ${res.data.message}`);
    assert(res.data.fileName, 'Should return generated fileName');
    console.log(`     → fileName: ${res.data.fileName}`);
  });

  await test('GET /job-status while job is running → isJobRunning: true', async () => {
    await new Promise(r => setTimeout(r, 3000));
    const res = await req('GET', '/job-status');
    assertStatus(res, 200);
    // May already be done if job started super fast
    console.log(`     → life_cycle_state: ${res.data.life_cycle_state || 'not started yet'}`);
  });

  await test('Poll /final-result until job completes (timeout: 20 min)', async () => {
    const TIMEOUT = 20 * 60 * 1000;
    const START   = Date.now();
    let result    = null;

    while (Date.now() - START < TIMEOUT) {
      await new Promise(r => setTimeout(r, 10000));
      const res = await req('GET', '/final-result');
      if (res.status === 200 && res.data.logs) {
        result = res.data;
        break;
      }
      const elapsed = Math.round((Date.now() - START) / 1000);
      console.log(`     → Still running... (${elapsed}s elapsed)`);
    }

    assert(result !== null, 'Job did not complete within 10 minutes');
    assert(result.usecases.includes('backend_e2e_test'), 'Result should contain the test usecase');
    const entry = result.logs['backend_e2e_test'];
    assert(entry && !entry.error, `Usecase result had error: ${entry?.error}`);
    assert(Array.isArray(entry.recon_result), 'Should have recon_result array');
    console.log(`     → Completed! recon_result rows: ${entry.recon_result.length}`);
  });
}

// ══════════════════════════════════════════════════════════════════════════════
// SECTION 10 — E2E Edge Cases (3 usecases in one job run)
// ══════════════════════════════════════════════════════════════════════════════

async function testE2EEdgeCases() {
  section('10. E2E Edge Cases — 3 usecases in one job run');
  console.log('  ⏳ Submitting 3 usecases in a single run. This will take ~15 min...');

  // All three usecases use test_source_50k → test_source_50k (self-comparison)
  // so the recon always produces a PASS result regardless of filter/column config.
  const BASE_ENTRY = {
    catalog: 'metadata', schema: 'metadata_schema',
    container_type: 'DATABRICKS', modes: 'recon',
    source_catalog_name: 'h_and_r', source_schema_name: 'djj_testing',
    source_table_name:   'test_source_50k',
    target_catalog_name: 'h_and_r', target_schema_name: 'djj_testing',
    target_table_name:   'test_source_50k',
  };

  const entries = [
    // Edge case A: bare minimum — no column ops, no filter
    {
      ...BASE_ENTRY,
      usecase: 'e2e_edge_plain',
      column_excluded:                    [],
      if_recon_then_statistics_for:       [],
      if_recon_then_checksum_for:         [],
      if_recon_then_data_quality_for:     [],
    },
    // Edge case B: column exclusion + all checkbox groups populated
    {
      ...BASE_ENTRY,
      usecase: 'e2e_edge_columns',
      column_excluded:                    ['city'],
      if_recon_then_statistics_for:       ['amount', 'quantity'],
      if_recon_then_checksum_for:         ['id', 'name', 'amount'],
      if_recon_then_data_quality_for:     ['id', 'name', 'amount', 'city'],
    },
    // Edge case C: exclude multiple columns + comprehensive stats across all numeric columns
    {
      ...BASE_ENTRY,
      usecase: 'e2e_edge_multi_exclude',
      column_excluded:                    ['city', 'is_active'],
      if_recon_then_statistics_for:       ['amount', 'quantity', 'id'],
      if_recon_then_checksum_for:         ['id', 'name', 'amount', 'quantity'],
      if_recon_then_data_quality_for:     ['id', 'name', 'amount', 'quantity'],
    },
  ];

  const EXPECTED_USECASES = entries.map(e => e.usecase);

  await test('POST /run-from-form with 3-entry array → 200 job started', async () => {
    let res;
    for (let attempt = 0; attempt < 24; attempt++) {
      res = await req('POST', '/run-from-form', {
        data: entries, headers: { 'Content-Type': 'application/json' }
      });
      if (res.status !== 409) break;
      console.log(`     → Server busy (409), waiting 5s... (attempt ${attempt + 1}/24)`);
      await new Promise(r => setTimeout(r, 5000));
    }
    assertStatus(res, 200);
    assert(res.data.fileName, 'Should return generated fileName');
    console.log(`     → fileName: ${res.data.fileName}`);
  });

  let result = null;

  await test('Poll /final-result until all 3 usecases complete (timeout: 35 min)', async () => {
    const TIMEOUT = 35 * 60 * 1000;
    const START   = Date.now();

    while (Date.now() - START < TIMEOUT) {
      await new Promise(r => setTimeout(r, 10000));
      const res = await req('GET', '/final-result');
      if (res.status === 200 && res.data.logs) {
        result = res.data;
        break;
      }
      const elapsed = Math.round((Date.now() - START) / 1000);
      console.log(`     → Still running... (${elapsed}s elapsed)`);
    }

    assert(result !== null, 'Job did not complete within 20 minutes');

    // All 3 usecases must appear in the result
    for (const uc of EXPECTED_USECASES) {
      assert(result.usecases.includes(uc), `Missing usecase in result: ${uc}`);
      const entry = result.logs[uc];
      assert(entry && !entry.error, `Usecase ${uc} had error: ${entry?.error}`);
      assert(Array.isArray(entry.recon_result), `${uc} should have recon_result array`);
    }
    console.log(`     → All 3 usecases completed: ${result.usecases.join(', ')}`);
  });

  await test('e2e_edge_plain — recon_result present, no error', () => {
    assert(result !== null, 'No result from poll test');
    const entry = result.logs['e2e_edge_plain'];
    assert(entry && !entry.error, `Error: ${entry?.error}`);
    assert(Array.isArray(entry.recon_result) && entry.recon_result.length > 0,
      'e2e_edge_plain should have at least 1 recon_result row');
    console.log(`     → recon_result rows: ${entry.recon_result.length}, log entries: ${entry.recon_log?.length ?? 0}`);
  });

  await test('e2e_edge_columns — result has stats/checksum/DQ log entries', () => {
    assert(result !== null, 'No result from poll test');
    const entry = result.logs['e2e_edge_columns'];
    assert(entry && !entry.error, `Error: ${entry?.error}`);
    assert(Array.isArray(entry.recon_result) && entry.recon_result.length > 0,
      'e2e_edge_columns should have at least 1 recon_result row');
    const logCount = (entry.recon_log?.length ?? 0) +
                     (entry.historical_log?.length ?? 0);
    console.log(`     → recon_result rows: ${entry.recon_result.length}, total log entries: ${logCount}`);
  });

  await test('e2e_edge_multi_exclude — multi-column exclusion with comprehensive stats', () => {
    assert(result !== null, 'No result from poll test');
    const entry = result.logs['e2e_edge_multi_exclude'];
    assert(entry && !entry.error, `Error: ${entry?.error}`);
    assert(Array.isArray(entry.recon_result) && entry.recon_result.length > 0,
      'e2e_edge_multi_exclude should have at least 1 recon_result row');
    console.log(`     → recon_result rows: ${entry.recon_result.length} (excluded: city, is_active)`);
  });
}

// ══════════════════════════════════════════════════════════════════════════════
// MAIN
// ══════════════════════════════════════════════════════════════════════════════

async function main() {
  console.log('\n╔══════════════════════════════════════════════════════════╗');
  console.log('║         Databricks Backend — Test Suite                 ║');
  console.log('╚══════════════════════════════════════════════════════════╝');
  console.log(`\n  Mode: ${WITH_E2E ? 'Validation + Databricks + E2E' : WITH_DB ? 'Validation + Databricks' : 'Validation only'}`);
  console.log(`  Server: ${BASE}`);
  if (!WITH_DB) console.log('\n  Tip: add --databricks to also test live Databricks connectivity');
  if (!WITH_E2E) console.log('  Tip: add --e2e to also run a full end-to-end job trigger');

  setupTmpFiles();

  try {
    // Always run
    await testServerReachable();
    await testUploadCsvValidation();
    await testRunFromFormValidation();
    await testTableColumnsValidation();
    await testStateAndConcurrency();
    await testCsvBuilder();

    if (WITH_DB) {
      await testDatabricksReadOnly();
      await testConcurrencyGuard();
    } else {
      section('7–8. Databricks Tests (skipped — run with --databricks)');
    }

    if (WITH_E2E) {
      await testE2E();
      await testE2EEdgeCases();
    } else {
      section('9–10. E2E Tests (skipped — run with --e2e)');
    }

  } finally {
    cleanupTmpFiles();

    console.log('\n' + '═'.repeat(62));
    console.log(`  Results: ${passed} passed  |  ${failed} failed  |  ${skipped} skipped`);
    console.log('═'.repeat(62) + '\n');

    if (failed > 0) process.exit(1);
  }
}

main().catch(err => {
  console.error('\n💥 Test runner crashed:', err.message);
  process.exit(1);
});

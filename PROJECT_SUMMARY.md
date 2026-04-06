# Databricks Historical Load & Reconciliation Utility — Project Summary

**Date:** 2026-03-22
**Stack:** Node.js / Express backend · Single-page HTML/JS frontend (in progress)

---

## Overview

A full-stack utility that lets users trigger and monitor Databricks reconciliation / historical-load jobs without touching Databricks directly. Users either upload a pre-built CSV or fill a form; the backend builds the CSV, uploads it to a Unity Catalog Volume, triggers a Databricks job, streams live logs via SSE, and surfaces the results.

---

## Repository Layout

```
databricks-backend/
├── server.js                  # Express app — all 9 endpoints
├── databricksJobRunner.js     # Job trigger, polling, child-run tracking
├── databricksLogFetcher.js    # Reads result/log tables from SQL Warehouse
├── csvUsecaseParser.js        # Downloads + parses CSV from Databricks Volume
├── cancelRunJob.js            # Wraps runs/cancel API call
├── test_backend.js            # 47-test suite (Sections 1–10)
├── index.html                 # Frontend SPA (in progress)
└── .env                       # Credentials (not committed)
```

---

## Backend — `server.js`

### Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/upload-csv` | Accept CSV file → validate extension → upload to Volume → trigger job |
| POST | `/run-from-form` | Accept JSON entry/array → build 45-col CSV → upload to Volume → trigger job |
| GET | `/table-columns` | `DESCRIBE TABLE` via SQL Warehouse → `{name, data_type, is_numeric}[]` |
| GET | `/results/:usecase` | Fetch recon/historical logs + results for one usecase |
| GET | `/fetch-all-logs/:csvFile` | Download CSV from Volume → extract all usecases → fetch all results |
| GET | `/final-result` | Return last completed job's result set (202 if not ready) |
| GET | `/job-status` | `{isJobRunning, runId, life_cycle_state}` |
| POST | `/cancel-job` | Cancel the currently running Databricks run |
| GET | `/run-history` | List recent runs from Databricks Jobs API (optional `?limit=N`) |
| GET | `/sse` | Server-Sent Events stream for real-time log lines |

### Key Design Decisions

- **Fire-and-forget trigger** — POST endpoints return 200 immediately; job monitoring runs in background via `triggerAndMonitorJob()`
- **SSE log streaming** — clients connect to `/sse` and receive real-time log lines as the job progresses
- **Shared state concurrency guard** — `sharedState.isJobRunning` prevents concurrent job triggers (returns 409 Conflict)
- **Per-usecase catalog/schema** — each CSV row carries its own `catalog`/`schema` for where its result tables live; no global defaults
- **CSV builder** — `buildCsv()` / `csvCell()` produce a 45-column CSV matching the notebook's DDL, with proper comma/quote escaping and array-join for checkbox fields
- **Global error handler** — 4-arg Express middleware catches Multer/busboy errors (`Unexpected end of form`) and returns 400 JSON instead of 500 HTML

### CSV Column Schema (45 columns)

```
catalog, schema, container_type, modes,
source_catalog_name, source_schema_name, target_catalog_name, target_schema_name,
filter_condition, date_column_format, column_excluded,
if_recon_then_statistics_for, if_recon_then_checksum_for, if_recon_then_data_quality_for,
storage_name, container_name, source_path, source_file_format,
access_token_adls, aws_access_key, aws_secret_key, s3_bucket,
snowflake_account, snowflake_user, snowflake_password, snowflake_warehouse,
snowflake_database, snowflake_schema, snowflake_table,
gcp_project, bigquery_dataset, bigquery_table,
sql_host, sql_port, sql_database, sql_table, sql_username, sql_password,
api_url, api_method, api_headers, api_auth_token,
usecase, source_table_name, target_table_name
```

### Supported Container Types

`DATABRICKS`, `SQL_SERVER`, `SNOWFLAKE`, `BIGQUERY`, `ADLS`, `S3`, `REST_API`

---

## Supporting Modules

### `databricksJobRunner.js`

- Calls `POST /api/2.1/jobs/run-now` with `notebook_params: { file_name: csvFileName }`
- Polls parent run every 5s; detects task state changes and logs them
- Tracks child (for-each) iterations via `runs/list` on the child job ID
- On SUCCESS: calls `fetchAllLogsAndResultsForUsecases` and stores in `sharedState.finalLogsResult`
- `finally` guard: only resets `sharedState` if this run still owns it (`currentRunId === runId`) — prevents a stale polling loop from clearing a new job's state

### `databricksLogFetcher.js`

- For each usecase, queries three tables via SQL Warehouse:
  - `{catalog}.{schema}.{usecase}_log_recon`
  - `{catalog}.{schema}.{usecase}_log_historical`
  - `{catalog}.{schema}.{usecase}_result_recon`
- Per-usecase catalog/schema from `configs` map (with `metadata`/`metadata_schema` fallback)
- Per-usecase errors are caught and embedded in result body (not thrown as HTTP 500)

### `csvUsecaseParser.js`

- Downloads CSV from `/Volumes/h_and_r/intput_file/inputs/{fileName}` via Files API
- Returns `{ usecases: string[], configs: Record<string,{catalog,schema}>, rows: object[] }`

---

## Databricks Environment

| Item | Value |
|------|-------|
| Parent job ID | `814178182790777` |
| For-each child job ID | `894732156058812` |
| Volume path | `/Volumes/h_and_r/intput_file/inputs/` |
| Source/target catalog | `h_and_r` |
| Source/target schema | `djj_testing` |
| Known good source table | `test_source_50k` (7 cols: id, name, city, amount, quantity, is_active, txn_date) |
| Metadata catalog/schema | `metadata` / `metadata_schema` |
| SQL Warehouse token env var | `TABLE_TOKEN` |
| PAT token env var | `PAT_TOKEN` |

**Important notes:**
- `filter_condition` field only works with date-based filters (requires `date_column_format` specifying a date column). Generic SQL like `id > 0` raises `Exception: Invalid Filter Column` in the notebook.
- `test_pass_50k` table was dropped — E2E tests use `test_source_50k` → `test_source_50k` (self-comparison).
- For-each iterations run **sequentially** on the cluster — 3 usecases take ~27 min total vs ~13 min for 1 usecase.

---

## Test Suite — `test_backend.js`

### Run Modes

```bash
node test_backend.js                # Sections 1–6  (no Databricks needed)
node test_backend.js --databricks   # + Sections 7–8 (read-only Databricks)
node test_backend.js --e2e          # + Sections 9–10 (live job trigger)
```

### Results: 47/47 passing

| Section | Tests | Coverage |
|---------|-------|----------|
| 1. Server Reachability | 1 | `GET /job-status` returns 200 |
| 2. POST /upload-csv Validation | 4 | No file, wrong type, empty body, valid CSV |
| 3. POST /run-from-form Validation | 8 | Missing required fields, array inputs, checkbox formats |
| 4. GET /table-columns Validation | 4 | Missing catalog / schema / table params |
| 5. State & Concurrency | 5 | Idle check, 202 before job, cancel-when-idle, run history + limit |
| 6. CSV Builder Unit Tests | 7 | Header, array join, empty array, comma quoting, nulls, multi-row, quote escaping |
| 7. Databricks Read-only | 8 | Column discovery, numeric flags, results fetch, non-existent table/catalog |
| 8. Concurrency Guard | 1 | 409 on second job, retry-cancel, wait-for-idle |
| 9. E2E — Single Usecase | 3 | `backend_e2e_test` full run: trigger → poll → result verify |
| 10. E2E — 3 Edge Cases | 6 | Plain / full column selection / multi-column exclusion (one job, 3 iterations) |

### Helpers

```javascript
cancelAndWaitIdle()   // retry cancel until currentRunId is set, then poll idle
```

### Timeouts

| Section | Timeout | Reason |
|---------|---------|--------|
| 9 poll | 20 min | Single usecase ~13 min (includes cold cluster start) |
| 10 poll | 35 min | 3 sequential iterations × ~6 min + cluster start |

---

## Bugs Found & Fixed

| Bug | Root Cause | Fix |
|-----|-----------|-----|
| Multer v2 "Unexpected end of form" → 500 HTML | Busboy throws below Express level on malformed multipart | 4-arg global error handler → 400 JSON |
| Section 3 tests returning 409 | Valid CSV upload in Section 2 started a job with no cancel | `cancelAndWaitIdle()` after any 200 response in validation tests |
| `/cancel-job` returns 400 immediately after job start | `currentRunId` not yet set when cancel called (async Databricks API call) | Retry cancel up to 15× with 1s waits |
| Old polling loop clears new job's shared state | `finally` block reset unconditionally after a cancel | Guard: only reset if `sharedState.currentRunId === runId` |
| E2E job running wrong usecase (`concurrency_test_a`) | `jobs/run-now` called without parameters; notebook used stale `csv_table_1` | Added `notebook_params: { file_name: csvFileName }` to job trigger |
| `test_pass_50k` TABLE_OR_VIEW_NOT_FOUND | Table was dropped from Databricks | E2E tests switched to self-comparison on `test_source_50k` |
| Edge case filter failing with "Invalid Filter Column" | Notebook's filter handler expects date-based format + `date_column_format` | Replaced with `e2e_edge_multi_exclude` (multi-column exclusion) |
| Sections 3/5 failing on re-run without restart | Previous E2E's background polling held `isJobRunning = true` | Kill + restart server before each test run |

---

## Frontend — `index.html` (Next)

Three-tab single-page app planned:

### Tab 1 — Upload CSV
- Drag-and-drop zone
- Extension validation (`.csv` only)
- Upload & Run button
- Shows filename on drop

### Tab 2 — Manual Entry
Five sections:
1. **Run Config** — usecase, catalog, schema, modes, container type dropdown
2. **Source** — catalog, schema, table (connector-specific fields shown/hidden by container type)
3. **Target** — catalog, schema, table
4. **Optional Filters** — filter condition, date column format, column excluded
5. **Column Selection** — "Load Columns" button calls `GET /table-columns`; renders 4 checkbox groups:
   - Exclude (all columns)
   - Statistics (numeric only — `#` chip)
   - Checksum (all columns)
   - Data Quality (all columns)
   - Each group has Select All / None buttons

### Tab 3 — Results
- Auto-switches when SSE receives `📊 Results ready.`
- Card per usecase with PASS/FAIL badge
- Expandable sections: Structure · Statistics · Checksum · Data Quality
- Collapsible log tables (recon log, historical log)

### Shared UI
- Header: job status badge (IDLE / RUNNING / DONE) + Cancel Job button
- SSE log panel pinned at bottom, color-coded by line prefix
- Auto-reconnect on SSE disconnect

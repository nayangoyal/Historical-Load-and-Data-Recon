import React, { useState } from 'react';

const CONNECTOR_TYPES = ['DATABRICKS', 'SQL_SERVER', 'SNOWFLAKE', 'BIGQUERY', 'ADLS', 'S3', 'REST_API'];

export default function ManualTab({ jobRunning, onLog, onJobStart }) {
  const [containerType, setContainerType] = useState('DATABRICKS');
  const [columns, setColumns] = useState([]);
  const [colsLoaded, setColsLoaded] = useState(false);
  const [colsStatus, setColsStatus] = useState('');
  const [loadingCols, setLoadingCols] = useState(false);
  const [submitting, setSubmitting] = useState(false);

  // Form refs via state
  const [form, setForm] = useState({
    usecase: '', catalog: 'metadata', schema: 'metadata_schema', modes: 'recon',
    src_catalog: '', src_schema: '', src_table: '',
    tgt_catalog: '', tgt_schema: '', tgt_table: '',
    filter_condition: '', date_col_format: '',
    // SQL Server
    sql_host: '', sql_port: '1433', sql_database: '', sql_table: '', sql_username: '', sql_password: '',
    // Snowflake
    sf_account: '', sf_user: '', sf_password: '', sf_warehouse: '', sf_database: '', sf_schema: '', sf_table: '',
    // BigQuery
    gcp_project: '', bq_dataset: '', bq_table: '',
    // ADLS
    storage_name: '', container_name: '', adls_path: '', adls_format: 'parquet', adls_token: '',
    // S3
    s3_bucket: '', s3_path: '', s3_format: 'parquet', aws_key: '', aws_secret: '',
    // REST API
    api_url: '', api_method: 'GET', api_headers: '', api_token: '',
  });

  const [checked, setChecked] = useState({ exclude: [], stats: [], checksum: [], quality: [] });

  function updateForm(key, val) {
    setForm(prev => ({ ...prev, [key]: val }));
  }

  function toggleCheck(group, colName) {
    setChecked(prev => {
      const arr = prev[group];
      return { ...prev, [group]: arr.includes(colName) ? arr.filter(c => c !== colName) : [...arr, colName] };
    });
  }

  function selectAll(group, cols, on) {
    setChecked(prev => ({ ...prev, [group]: on ? cols.map(c => c.name) : [] }));
  }

  async function loadColumns() {
    let catalog, schema, table;
    if (containerType === 'DATABRICKS') {
      catalog = form.src_catalog; schema = form.src_schema; table = form.src_table;
    } else {
      catalog = form.tgt_catalog; schema = form.tgt_schema; table = form.tgt_table;
    }
    if (!catalog || !schema || !table) {
      alert(`Please fill in the ${containerType === 'DATABRICKS' ? 'source' : 'target'} catalog, schema, and table.`);
      return;
    }
    setLoadingCols(true); setColsStatus('');
    try {
      const res = await fetch(`/table-columns?catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}`);
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Failed');
      setColumns(data.columns);
      setColsLoaded(true);
      setColsStatus(`${data.columns.length} columns loaded from ${catalog}.${schema}.${table}`);
    } catch (err) {
      setColsStatus(`\u274C ${err.message}`);
    } finally {
      setLoadingCols(false);
    }
  }

  async function submit() {
    if (jobRunning) { onLog('\u26A0 A job is already running.'); return; }
    if (!form.usecase) return alert('Please enter a Usecase Name.');
    if (!form.tgt_table) return alert('Please enter the Target Table name.');

    setSubmitting(true);
    const entry = {
      usecase: form.usecase, catalog: form.catalog, schema: form.schema,
      container_type: containerType, modes: form.modes,
      source_catalog_name: form.src_catalog, source_schema_name: form.src_schema, source_table_name: form.src_table,
      target_catalog_name: form.tgt_catalog, target_schema_name: form.tgt_schema, target_table_name: form.tgt_table,
      filter_condition: form.filter_condition, date_column_format: form.date_col_format,
      column_excluded: checked.exclude, if_recon_then_statistics_for: checked.stats,
      if_recon_then_checksum_for: checked.checksum, if_recon_then_data_quality_for: checked.quality,
      sql_host: form.sql_host, sql_port: form.sql_port, sql_database: form.sql_database,
      sql_table: form.sql_table, sql_username: form.sql_username, sql_password: form.sql_password,
      snowflake_account: form.sf_account, snowflake_user: form.sf_user, snowflake_password: form.sf_password,
      snowflake_warehouse: form.sf_warehouse, snowflake_database: form.sf_database,
      snowflake_schema: form.sf_schema, snowflake_table: form.sf_table,
      gcp_project: form.gcp_project, bigquery_dataset: form.bq_dataset, bigquery_table: form.bq_table,
      storage_name: form.storage_name, container_name: form.container_name,
      source_path: form.adls_path || form.s3_path,
      source_file_format: form.adls_format || form.s3_format,
      access_token_adls: form.adls_token,
      s3_bucket: form.s3_bucket, aws_access_key: form.aws_key, aws_secret_key: form.aws_secret,
      api_url: form.api_url, api_method: form.api_method, api_headers: form.api_headers, api_auth_token: form.api_token,
    };

    try {
      const res = await fetch('/run-from-form', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(entry),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Submission failed');
      onLog(`\u2699\uFE0F ${data.message}`);
      onJobStart();
    } catch (err) {
      onLog(`\u274C ${err.message}`);
    } finally {
      setSubmitting(false);
    }
  }

  function Input({ label, field, type = 'text', placeholder = '' }) {
    return (
      <div className="form-group">
        <label>{label}</label>
        <input type={type} value={form[field]} placeholder={placeholder}
          onChange={e => updateForm(field, e.target.value)} />
      </div>
    );
  }

  function CheckboxGroup({ title, hint, group, filterFn }) {
    const cols = columns.filter(filterFn || (() => true));
    return (
      <div className="checkbox-group-card">
        <div className="checkbox-group-header">
          <span>{title} <span className="checkbox-group-hint">({hint})</span></span>
          <div className="checkbox-group-actions">
            <button className="btn btn-sm btn-secondary" onClick={() => selectAll(group, cols, true)}>All</button>
            <button className="btn btn-sm btn-secondary" onClick={() => selectAll(group, cols, false)}>None</button>
          </div>
        </div>
        <div className="checkbox-list">
          {cols.length === 0 ? (
            <span style={{ color: 'var(--muted)', fontSize: 12, padding: 4 }}>No eligible columns</span>
          ) : cols.map(col => (
            <label key={col.name}>
              <input type="checkbox" checked={checked[group].includes(col.name)}
                onChange={() => toggleCheck(group, col.name)} />
              <span>{col.name}{col.is_numeric && <span className="chip-numeric">#</span>}</span>
            </label>
          ))}
        </div>
      </div>
    );
  }

  return (
    <>
      {/* Run Configuration */}
      <div className="card">
        <h3>Run Configuration</h3>
        <div className="form-row">
          <Input label="Usecase Name" field="usecase" placeholder="e.g. sales_recon_2024" />
          <div className="form-group">
            <label>Modes</label>
            <select value={form.modes} onChange={e => updateForm('modes', e.target.value)}>
              <option value="recon">Recon only</option>
              <option value="historical_load">Historical Load only</option>
              <option value="historical_load,recon">Both (Historical Load + Recon)</option>
            </select>
          </div>
          <Input label="Result Catalog" field="catalog" />
          <Input label="Result Schema" field="schema" />
        </div>
      </div>

      {/* Source Configuration */}
      <div className="card">
        <h3>Source Configuration</h3>
        <div className="form-row">
          <div className="form-group">
            <label>Container Type (Source)</label>
            <select value={containerType} onChange={e => { setContainerType(e.target.value); setColsLoaded(false); setColumns([]); }}>
              {CONNECTOR_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
            </select>
          </div>
        </div>

        {containerType === 'DATABRICKS' && (
          <div className="form-row">
            <Input label="Source Catalog" field="src_catalog" placeholder="h_and_r" />
            <Input label="Source Schema" field="src_schema" placeholder="djj_testing" />
            <Input label="Source Table" field="src_table" placeholder="my_source_table" />
          </div>
        )}
        {containerType === 'SQL_SERVER' && (
          <div className="form-row">
            <Input label="Host" field="sql_host" />
            <Input label="Port" field="sql_port" />
            <Input label="Database" field="sql_database" />
            <Input label="Table" field="sql_table" />
            <Input label="Username" field="sql_username" />
            <Input label="Password" field="sql_password" type="password" />
          </div>
        )}
        {containerType === 'SNOWFLAKE' && (
          <div className="form-row">
            <Input label="Account" field="sf_account" />
            <Input label="User" field="sf_user" />
            <Input label="Password" field="sf_password" type="password" />
            <Input label="Warehouse" field="sf_warehouse" />
            <Input label="Database" field="sf_database" />
            <Input label="Schema" field="sf_schema" />
            <Input label="Table" field="sf_table" />
          </div>
        )}
        {containerType === 'BIGQUERY' && (
          <div className="form-row">
            <Input label="GCP Project" field="gcp_project" />
            <Input label="Dataset" field="bq_dataset" />
            <Input label="Table" field="bq_table" />
          </div>
        )}
        {containerType === 'ADLS' && (
          <div className="form-row">
            <Input label="Storage Account Name" field="storage_name" />
            <Input label="Container Name" field="container_name" />
            <Input label="Source Path" field="adls_path" placeholder="/folder/file.parquet" />
            <Input label="File Format" field="adls_format" />
            <Input label="Access Token" field="adls_token" type="password" />
          </div>
        )}
        {containerType === 'S3' && (
          <div className="form-row">
            <Input label="S3 Bucket" field="s3_bucket" />
            <Input label="Source Path" field="s3_path" placeholder="folder/file.parquet" />
            <Input label="File Format" field="s3_format" />
            <Input label="AWS Access Key" field="aws_key" />
            <Input label="AWS Secret Key" field="aws_secret" type="password" />
          </div>
        )}
        {containerType === 'REST_API' && (
          <div className="form-row">
            <Input label="API URL" field="api_url" />
            <div className="form-group">
              <label>Method</label>
              <select value={form.api_method} onChange={e => updateForm('api_method', e.target.value)}>
                <option>GET</option><option>POST</option>
              </select>
            </div>
            <div className="form-group">
              <label>Headers (JSON)</label>
              <textarea value={form.api_headers} placeholder='{"Authorization": "Bearer ..."}'
                onChange={e => updateForm('api_headers', e.target.value)} style={{ height: 56 }} />
            </div>
            <Input label="Auth Token" field="api_token" type="password" />
          </div>
        )}
      </div>

      {/* Target Configuration */}
      <div className="card">
        <h3>Target Configuration (Databricks)</h3>
        <div className="form-row">
          <Input label="Target Catalog" field="tgt_catalog" placeholder="h_and_r" />
          <Input label="Target Schema" field="tgt_schema" placeholder="djj_testing" />
          <Input label="Target Table" field="tgt_table" placeholder="my_target_table" />
        </div>
      </div>

      {/* Optional Filters */}
      <div className="card">
        <h3>Optional Filters</h3>
        <div className="form-row">
          <Input label="Filter Condition" field="filter_condition" placeholder="e.g. date >= '2024-01-01'" />
          <Input label="Date Column Format" field="date_col_format" placeholder="e.g. yyyy-MM-dd" />
        </div>
      </div>

      {/* Column Selection */}
      <div className="card">
        <h3>Column Selection</h3>
        <p style={{ fontSize: 12, color: 'var(--muted)', marginBottom: 12 }}>
          Loads columns from the source table (DATABRICKS) or target table (other connectors).
        </p>
        <div className="flex-gap">
          <button className="btn btn-secondary" disabled={loadingCols} onClick={loadColumns}>
            {loadingCols ? <><span className="spinner" style={{ borderTopColor: 'var(--blue)' }} /> Loading...</> : 'Load Columns'}
          </button>
          <span style={{ fontSize: 12, color: 'var(--muted)' }}>{colsStatus}</span>
        </div>

        {colsLoaded && (
          <div className="mt-12">
            <CheckboxGroup title="Columns to Exclude" hint="will be ignored in all checks" group="exclude" />
            <CheckboxGroup title="Statistics Columns" hint="numeric only - mean / min / max" group="stats" filterFn={c => c.is_numeric} />
            <CheckboxGroup title="Checksum Columns" hint="xxhash64 row-level hash" group="checksum" />
            <CheckboxGroup title="Data Quality Columns" hint="null / duplicate / missing value checks" group="quality" />
          </div>
        )}

        <div className="mt-12">
          <button className="btn btn-primary" disabled={submitting} onClick={submit}>
            {submitting ? <><span className="spinner" /> Submitting...</> : 'Submit & Run Job'}
          </button>
        </div>
      </div>
    </>
  );
}

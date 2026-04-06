import React, { useEffect } from 'react';
import { boolDisplay, formatArray, allChecksPass } from '../utils';

export default function ResultModal({ usecase, entry, onClose }) {
  useEffect(() => {
    function handleKey(e) { if (e.key === 'Escape') onClose(); }
    document.addEventListener('keydown', handleKey);
    return () => document.removeEventListener('keydown', handleKey);
  }, [onClose]);

  if (!usecase || !entry) return null;

  return (
    <div className="modal-overlay visible" onClick={e => { if (e.target === e.currentTarget) onClose(); }}>
      <div className="modal-content">
        <div className="modal-header">
          <h2>{usecase}</h2>
          <button className="modal-close" onClick={onClose}>{'\u2715'}</button>
        </div>
        <div className="modal-body">
          {entry.error ? (
            <div style={{ color: 'var(--red)', fontSize: 14, padding: 20 }}>{entry.error}</div>
          ) : (
            <ModalDetails entry={entry} />
          )}
        </div>
      </div>
    </div>
  );
}

function LogTable({ rows }) {
  if (!rows || rows.length === 0) return <span style={{ color: 'var(--muted)', fontSize: 11 }}>No entries</span>;
  const cols = Object.keys(rows[0]);
  return (
    <table className="log-table">
      <thead><tr>{cols.map(c => <th key={c}>{c}</th>)}</tr></thead>
      <tbody>
        {rows.map((r, i) => (
          <tr key={i}>{cols.map(c => <td key={c}>{r[c] ?? ''}</td>)}</tr>
        ))}
      </tbody>
    </table>
  );
}

function CollapsibleLog({ title, rows }) {
  const [open, setOpen] = React.useState(false);
  return (
    <div className="detail-section">
      <h5 style={{ cursor: 'pointer' }} onClick={() => setOpen(!open)}>
        {open ? '\u25BC' : '\u25B6'} {title} ({(rows || []).length})
      </h5>
      {open && <div style={{ marginTop: 6, overflowX: 'auto' }}><LogTable rows={rows} /></div>}
    </div>
  );
}

function ModalDetails({ entry }) {
  const row = (entry.recon_result && entry.recon_result[0]) || {};
  const rowMatch = boolDisplay(row.row_count_match);
  const colMatch = boolDisplay(row.column_count_match);
  const schMatch = boolDisplay(row.schema_match);
  const hashMatch = boolDisplay(row.hash_match);
  const dupMatch = boolDisplay(row.row_dup_match);

  const pass = Object.keys(row).length > 0 && allChecksPass(row);
  const statusBadge = Object.keys(row).length === 0
    ? <span className="badge badge-na">NO DATA</span>
    : pass
      ? <span className="badge badge-pass" style={{ fontSize: 13, padding: '5px 14px' }}>ALL PASS</span>
      : <span className="badge badge-fail" style={{ fontSize: 13, padding: '5px 14px' }}>FAILED</span>;

  return (
    <>
      <div style={{ marginBottom: 20 }}>{statusBadge}</div>

      <div className="detail-section">
        <h5>Reconciliation Checks</h5>
        <div className="result-checks" style={{ padding: 0, marginBottom: 8 }}>
          <div className="check-item"><span className="check-icon">{rowMatch.icon}</span> Row Count ({row.row_count_source ?? '?'} vs {row.row_count_target ?? '?'})</div>
          <div className="check-item"><span className="check-icon">{colMatch.icon}</span> Column Count ({row.column_count_source ?? '?'} vs {row.column_count_target ?? '?'})</div>
          <div className="check-item"><span className="check-icon">{schMatch.icon}</span> Schema Match</div>
          <div className="check-item"><span className="check-icon">{hashMatch.icon}</span> Checksum Match</div>
          <div className="check-item"><span className="check-icon">{dupMatch.icon}</span> Duplicates ({row.row_dup_source ?? '?'} vs {row.row_dup_target ?? '?'})</div>
          {row.execution_date && <div className="check-item" style={{ fontSize: 11, color: 'var(--muted)' }}>{'\uD83D\uDCC5'} {row.execution_date}</div>}
        </div>
      </div>

      <DetailSection title="Structure" rows={[
        ['Source table', row.source_table], ['Target table', row.target_table],
        ['Schema (source)', formatArray(row.schema_source)], ['Schema (target)', formatArray(row.schema_target)],
        ['Excluded columns', formatArray(row.excluded_columns_in_source)],
      ]} />

      <DetailSection title="Statistics" rows={[
        ['Columns checked', formatArray(row.columns_for_numerical_check)],
        ['Mean (source)', formatArray(row.mean_source)], ['Mean (target)', formatArray(row.mean_target)],
        ['Mean match', formatArray(row.mean_match)],
      ]} />

      <DetailSection title="Checksum" rows={[
        ['Columns checked', formatArray(row.columns_for_hash_check)],
        ['Hash match', row.hash_match ?? '\u2014'], ['Message', row.hash_message ?? '\u2014'],
      ]} />

      <DetailSection title="Data Quality" rows={[
        ['Columns checked', formatArray(row.columns_for_quality_check)],
        ['Nulls (source)', formatArray(row.null_source)], ['Nulls (target)', formatArray(row.null_target)],
        ['Null match', formatArray(row.null_match)],
        ['Duplicates (source)', formatArray(row.dup_source)], ['Duplicates (target)', formatArray(row.dup_target)],
        ['Missing values (source)', formatArray(row.miss_source)], ['Missing values (target)', formatArray(row.miss_target)],
      ]} />

      <CollapsibleLog title="Recon Log" rows={entry.recon_log} />
      <CollapsibleLog title="Historical Log" rows={entry.historical_log} />
    </>
  );
}

function DetailSection({ title, rows }) {
  return (
    <div className="detail-section">
      <h5>{title}</h5>
      {rows.map(([key, val], i) => (
        <div className="detail-row" key={i}>
          <span className="detail-key">{key}</span>
          <span className="detail-val">{val ?? '\u2014'}</span>
        </div>
      ))}
    </div>
  );
}

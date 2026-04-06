import React, { useState } from 'react';
import { boolDisplay, allChecksPass } from '../utils';
import ResultModal from './ResultModal';

// view: 'empty' | 'history' | 'results'
export default function ResultsTab({ results, onLog }) {
  const [view, setView]             = useState('empty');   // what's shown
  const [historyList, setHistoryList] = useState([]);      // full history array
  const [currentData, setCurrentData] = useState(null);   // run data being displayed
  const [viewLabel, setViewLabel]   = useState('');
  const [fromHistory, setFromHistory] = useState(false);  // whether current results came from history drill-in
  const [modal, setModal]           = useState(null);

  // If a fresh job just completed, show those results
  const displayData = currentData || results;
  const hasResults  = displayData && displayData.usecases && displayData.usecases.length > 0;

  async function fetchLatest() {
    try {
      const res = await fetch('/final-result');
      const data = await res.json();
      if (res.status === 202 || !data.logs) return;
      setCurrentData(data);
      setFromHistory(false);
      setView('results');
      setViewLabel(`Last updated: ${new Date().toLocaleTimeString('en-GB', { hour12: false })}`);
    } catch (err) {
      onLog(`❌ Failed to load results: ${err.message}`);
    }
  }

  async function loadHistory() {
    try {
      const res  = await fetch('/results-history');
      const data = await res.json();
      setHistoryList(data && data.length > 0 ? data : []);
      setCurrentData(null);
      setFromHistory(false);
      setView('history');
      setViewLabel('');
    } catch (err) {
      onLog(`Failed to load history: ${err.message}`);
    }
  }

  function showHistoryRun(run) {
    const ts = new Date(run.timestamp).toLocaleString('en-GB', { hour12: false });
    setCurrentData(run);
    setFromHistory(true);
    setView('results');
    setViewLabel(`Run from ${ts}`);
  }

  function backToHistory() {
    setCurrentData(null);
    setFromHistory(false);
    setView('history');
    setViewLabel('');
  }

  return (
    <>
      {/* Toolbar */}
      <div className="results-toolbar">
        <div className="results-toolbar-left">
          {fromHistory && (
            <button className="btn btn-secondary btn-sm" onClick={backToHistory}>
              ← Back to History
            </button>
          )}
          {viewLabel && (
            <span style={{ fontSize: 11, color: 'var(--muted)' }}>{viewLabel}</span>
          )}
        </div>
        <div className="results-toolbar-right">
          <button className="btn btn-secondary btn-sm" onClick={fetchLatest}>Fetch Latest</button>
          <button className="btn btn-secondary btn-sm" onClick={loadHistory}>Load History</button>
        </div>
      </div>

      {/* History list */}
      {view === 'history' && (
        historyList.length === 0 ? (
          <NoResults />
        ) : (
          <div>
            <div className="section-heading">🕐 Run History</div>
            {historyList.map((run, i) => {
              const ts       = new Date(run.timestamp).toLocaleString('en-GB', { hour12: false });
              const ucCount  = (run.usecases || []).length;
              return (
                <div key={i} className="history-run" onClick={() => showHistoryRun(run)}>
                  <div className="history-run-info">
                    <span className="history-run-file">{run.fileName || 'Unknown'}</span>
                    <span className="history-run-time">{ts}</span>
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                    <span className="history-run-usecases">{ucCount} usecase{ucCount !== 1 ? 's' : ''}</span>
                    <span style={{ fontSize: 11, color: 'var(--muted)' }}>View →</span>
                  </div>
                </div>
              );
            })}
          </div>
        )
      )}

      {/* Results grid */}
      {view === 'results' && (
        hasResults ? (
          <div className="results-grid">
            {displayData.usecases.map(uc => {
              const entry = displayData.logs[uc];
              if (!entry) return null;
              return (
                <ResultCard key={uc} usecase={uc} entry={entry} onClick={() => setModal({ usecase: uc, entry })} />
              );
            })}
          </div>
        ) : (
          <NoResults />
        )
      )}

      {/* Empty state */}
      {view === 'empty' && !results && <NoResults />}
      {view === 'empty' && results && (
        <div className="results-grid">
          {results.usecases.map(uc => {
            const entry = results.logs[uc];
            if (!entry) return null;
            return <ResultCard key={uc} usecase={uc} entry={entry} onClick={() => setModal({ usecase: uc, entry })} />;
          })}
        </div>
      )}

      {modal && (
        <ResultModal usecase={modal.usecase} entry={modal.entry} onClose={() => setModal(null)} />
      )}
    </>
  );
}

function NoResults() {
  return (
    <div className="no-results">
      <div className="no-results-icon">📊</div>
      <p>No results yet. Run a job to see reconciliation results here.</p>
    </div>
  );
}

function ResultCard({ usecase, entry, onClick }) {
  if (entry.error) {
    return (
      <div className="result-card" onClick={onClick}>
        <div className="result-card-header">
          <span className="result-card-title">{usecase}</span>
          <span className="badge badge-fail">ERROR</span>
        </div>
        <div style={{ padding: '12px 16px', fontSize: 12, color: 'var(--red)' }}>{entry.error}</div>
      </div>
    );
  }

  const row      = (entry.recon_result && entry.recon_result[0]) || {};
  const rowMatch = boolDisplay(row.row_count_match);
  const colMatch = boolDisplay(row.column_count_match);
  const schMatch = boolDisplay(row.schema_match);
  const hashMatch = boolDisplay(row.hash_match);
  const dupMatch = boolDisplay(row.row_dup_match);
  const pass     = Object.keys(row).length > 0 && allChecksPass(row);

  const overallBadge = Object.keys(row).length === 0
    ? <span className="badge badge-na">NO DATA</span>
    : pass
      ? <span className="badge badge-pass">ALL PASS</span>
      : <span className="badge badge-fail">FAILED</span>;

  return (
    <div className="result-card" onClick={onClick}>
      <div className="result-card-header">
        <span className="result-card-title">{usecase}</span>
        {overallBadge}
      </div>
      <div className="result-checks">
        <div className="check-item"><span className="check-icon">{rowMatch.icon}</span> Row Count ({row.row_count_source ?? '?'} vs {row.row_count_target ?? '?'})</div>
        <div className="check-item"><span className="check-icon">{colMatch.icon}</span> Column Count ({row.column_count_source ?? '?'} vs {row.column_count_target ?? '?'})</div>
        <div className="check-item"><span className="check-icon">{schMatch.icon}</span> Schema Match</div>
        <div className="check-item"><span className="check-icon">{hashMatch.icon}</span> Checksum Match</div>
        <div className="check-item"><span className="check-icon">{dupMatch.icon}</span> Duplicates ({row.row_dup_source ?? '?'} vs {row.row_dup_target ?? '?'})</div>
        {row.execution_date && (
          <div className="check-item" style={{ fontSize: 11, color: 'var(--muted)' }}>📅 {row.execution_date}</div>
        )}
      </div>
    </div>
  );
}

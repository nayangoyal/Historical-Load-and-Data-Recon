import React, { useEffect, useRef, useState } from 'react';

export default function LogPanel({ logs, onClear, jobRunning }) {
  const [open, setOpen] = useState(true);
  const bodyRef = useRef();

  useEffect(() => {
    if (bodyRef.current && open) {
      bodyRef.current.scrollTop = bodyRef.current.scrollHeight;
    }
  }, [logs, open]);

  function getLineClass(text) {
    if (text.includes('✅') || text.includes('SUCCESS')) return 'success';
    if (text.includes('❌') || text.includes('FAILED') || text.includes('Error')) return 'error';
    if (text.includes('⚠') || text.includes('SKIPPED')) return 'warn';
    if (text.includes('🚀') || text.includes('▶') || text.includes('📊') || text.includes('📤') || text.includes('🔄')) return 'info';
    return 'default';
  }

  const height = open ? 'var(--log-h)' : '32px';

  return (
    <div className="log-panel" style={{ height }}>
      <div className="log-panel-header" onClick={() => setOpen(o => !o)}>
        <span className={jobRunning ? 'live' : ''}>
          {open ? '▼' : '▶'} &nbsp;Live Logs
          {logs.length > 0 && <span style={{ color: 'var(--muted)', marginLeft: 6, fontSize: 10 }}>({logs.length})</span>}
        </span>
        <div className="log-panel-actions">
          <button className="clear-logs-btn" onClick={e => { e.stopPropagation(); onClear(); }}>
            Clear
          </button>
        </div>
      </div>

      {open && (
        <div className="log-body" ref={bodyRef}>
          {logs.length === 0 ? (
            <div style={{ color: '#374151', fontSize: 11, paddingTop: 8, fontFamily: 'monospace' }}>
              Waiting for job activity...
            </div>
          ) : (
            logs.map((entry, i) => (
              <div key={i} className={`log-line ${getLineClass(entry.text)}`}>
                <span className="log-ts">[{entry.ts}]</span>
                <span className="log-text">{entry.text}</span>
              </div>
            ))
          )}
        </div>
      )}
    </div>
  );
}

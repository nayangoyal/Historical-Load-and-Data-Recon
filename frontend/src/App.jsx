import React, { useState, useEffect, useCallback } from 'react';
import Header from './components/Header';
import StatsStrip from './components/StatsStrip';
import TabNav from './components/TabNav';
import UploadTab from './components/UploadTab';
import ManualTab from './components/ManualTab';
import ResultsTab from './components/ResultsTab';
import LogPanel from './components/LogPanel';
import Toast from './components/Toast';
import useSSE from './hooks/useSSE';

export default function App() {
  const [activeTab, setActiveTab]   = useState('csv');
  const [jobRunning, setJobRunning] = useState(false);
  const [statusText, setStatusText] = useState('IDLE');
  const [logs, setLogs]             = useState([]);
  const [results, setResults]       = useState(null);
  const [toasts, setToasts]         = useState([]);
  const [stats, setStats]           = useState({ totalRuns: 0, lastRun: '—', passRate: '—', activeUsecase: null });

  // Theme — persist in localStorage
  const [theme, setTheme] = useState(() => localStorage.getItem('reconflow-theme') || 'dark');

  // Apply theme to <html> so CSS [data-theme] selectors work globally
  React.useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem('reconflow-theme', theme);
  }, [theme]);

  function toggleTheme() {
    setTheme(t => t === 'dark' ? 'light' : 'dark');
  }

  const addLog = useCallback((text) => {
    const ts = new Date().toLocaleTimeString('en-GB', { hour12: false });
    setLogs(prev => [...prev, { text, ts }]);
  }, []);

  // SSE live logs
  useSSE(useCallback((text) => {
    addLog(text);
    if (text === '📊 Results ready.') {
      fetchResults();
    } else if (text.startsWith('❌ Job ended with state:') || text.includes('was cancelled by user')) {
      setJobRunning(false);
      setStatusText('IDLE');
      setStats(s => ({ ...s, activeUsecase: null }));
    }
  }, [addLog]));

  // On mount: check job status + load history stats
  useEffect(() => {
    (async () => {
      try {
        const [statusRes, histRes] = await Promise.all([
          fetch('/job-status'),
          fetch('/results-history'),
        ]);
        const statusData = await statusRes.json();
        const histData   = await histRes.json();

        if (statusData.isJobRunning) {
          setJobRunning(true);
          setStatusText('RUNNING');
          addLog('🔄 Reconnected — a job is currently running.');
        }
        updateStats(histData);
      } catch { /* server not reachable yet */ }
    })();
  }, [addLog]);

  function updateStats(history) {
    if (!history || history.length === 0) {
      setStats({ totalRuns: 0, lastRun: '—', passRate: '—', activeUsecase: null });
      return;
    }
    const total = history.length;
    const lastTs = new Date(history[0].timestamp);
    const lastRun = lastTs.toLocaleDateString('en-GB', { day: '2-digit', month: 'short' })
      + ' ' + lastTs.toLocaleTimeString('en-GB', { hour: '2-digit', minute: '2-digit', hour12: false });

    // Calculate pass rate from latest run
    let passed = 0, total_uc = 0;
    const latest = history[0];
    if (latest.logs) {
      for (const uc of (latest.usecases || [])) {
        const entry = latest.logs[uc];
        if (!entry || entry.error) { total_uc++; continue; }
        const row = (entry.recon_result && entry.recon_result[0]) || {};
        const allPass = ['row_count_match','column_count_match','schema_match','hash_match']
          .every(k => row[k] === true || row[k] === 'true' || row[k] === 1 || row[k] === 'True');
        if (allPass) passed++;
        total_uc++;
      }
    }
    const passRate = total_uc > 0 ? `${Math.round((passed / total_uc) * 100)}%` : '—';
    setStats(s => ({ ...s, totalRuns: total, lastRun, passRate }));
  }

  async function fetchResults() {
    try {
      const res = await fetch('/final-result');
      const data = await res.json();
      if (res.status === 202 || !data.logs) return;
      setResults(data);
      setJobRunning(false);
      setStatusText('DONE');
      setStats(s => ({ ...s, activeUsecase: null }));
      setActiveTab('results');
      addToast('✅ Results are ready!', 'success');

      // Refresh stats from history
      const histRes = await fetch('/results-history');
      const histData = await histRes.json();
      updateStats(histData);
    } catch (err) {
      addLog(`❌ Failed to load results: ${err.message}`);
    }
  }

  function handleJobStart() {
    setJobRunning(true);
    setStatusText('RUNNING');
    setStats(s => ({ ...s, activeUsecase: 'Running...' }));
  }

  async function handleCancel() {
    setStatusText('CANCELLING');
    try {
      const res = await fetch('/cancel-job', { method: 'POST' });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error);
    } catch (err) {
      addLog(`❌ Cancel failed: ${err.message}`);
      setStatusText('RUNNING');
    }
  }

  function addToast(message, type = 'info') {
    const id = Date.now();
    setToasts(prev => [...prev, { id, message, type }]);
  }

  function removeToast(id) {
    setToasts(prev => prev.filter(t => t.id !== id));
  }

  return (
    <>
      <Header jobRunning={jobRunning} statusText={statusText} onCancel={handleCancel} theme={theme} onThemeToggle={toggleTheme} />
      <StatsStrip stats={stats} />
      <TabNav activeTab={activeTab} onSwitch={setActiveTab} />

      <div className="content">
        {activeTab === 'csv' && (
          <div className="tab-panel">
            <UploadTab jobRunning={jobRunning} onLog={addLog} onJobStart={handleJobStart} />
          </div>
        )}
        {activeTab === 'form' && (
          <div className="tab-panel">
            <ManualTab jobRunning={jobRunning} onLog={addLog} onJobStart={handleJobStart} />
          </div>
        )}
        {activeTab === 'results' && (
          <div className="tab-panel">
            <ResultsTab results={results} onLog={addLog} />
          </div>
        )}
      </div>

      <LogPanel logs={logs} onClear={() => setLogs([])} jobRunning={jobRunning} />
      <Toast toasts={toasts} onRemove={removeToast} />
    </>
  );
}

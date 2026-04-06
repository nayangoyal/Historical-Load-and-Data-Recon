import React from 'react';

export default function StatsStrip({ stats }) {
  const { totalRuns, lastRun, passRate, activeUsecase } = stats;

  return (
    <div className="stats-strip">
      <div className="stat-chip">
        <span className="stat-icon">🗂️</span>
        <div className="stat-info">
          <span className="stat-value">{totalRuns}</span>
          <span className="stat-label">Total Runs</span>
        </div>
      </div>

      <div className="stat-divider" />

      <div className="stat-chip">
        <span className="stat-icon">🕐</span>
        <div className="stat-info">
          <span className="stat-value">{lastRun || '—'}</span>
          <span className="stat-label">Last Run</span>
        </div>
      </div>

      <div className="stat-divider" />

      <div className="stat-chip">
        <span className="stat-icon">📈</span>
        <div className="stat-info">
          <span className="stat-value" style={{ color: passRate === '—' ? 'var(--muted)' : passRate === '100%' ? 'var(--green)' : 'var(--orange)' }}>
            {passRate}
          </span>
          <span className="stat-label">Pass Rate</span>
        </div>
      </div>

      <div className="stat-divider" />

      <div className="stat-chip">
        <span className="stat-icon">⚡</span>
        <div className="stat-info">
          <span className="stat-value" style={{ fontSize: 12, color: activeUsecase ? 'var(--orange)' : 'var(--muted)' }}>
            {activeUsecase || 'Idle'}
          </span>
          <span className="stat-label">Current Job</span>
        </div>
      </div>
    </div>
  );
}

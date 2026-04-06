import React from 'react';

export default function Header({ jobRunning, statusText, onCancel, theme, onThemeToggle }) {
  const badgeClass = statusText === 'RUNNING' ? 'running' : statusText === 'DONE' ? 'done' : '';

  return (
    <header>
      <h1>⚡ ReconFlow</h1>
      <span className={`status-badge ${badgeClass}`}>● {statusText}</span>
      {jobRunning && (
        <button className="cancel-btn" onClick={onCancel}>✕ Cancel Job</button>
      )}
      <button className="theme-toggle" onClick={onThemeToggle} title="Toggle theme">
        {theme === 'dark' ? '☀️' : '🌙'}
      </button>
    </header>
  );
}

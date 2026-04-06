import React from 'react';

const TABS = [
  { key: 'csv', label: 'Upload CSV', icon: '\uD83D\uDCC1' },
  { key: 'form', label: 'Manual Entry', icon: '\u2699\uFE0F' },
  { key: 'results', label: 'Results', icon: '\uD83D\uDCCA' },
];

export default function TabNav({ activeTab, onSwitch }) {
  return (
    <nav>
      {TABS.map(t => (
        <button
          key={t.key}
          className={`tab-btn ${activeTab === t.key ? 'active' : ''}`}
          onClick={() => onSwitch(t.key)}
        >
          {t.icon} {t.label}
        </button>
      ))}
    </nav>
  );
}

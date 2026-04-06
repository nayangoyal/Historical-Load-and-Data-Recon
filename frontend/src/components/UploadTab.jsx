import React, { useRef, useState } from 'react';

export default function UploadTab({ jobRunning, onLog, onJobStart }) {
  const [file, setFile] = useState(null);
  const [uploading, setUploading] = useState(false);
  const [dragOver, setDragOver] = useState(false);
  const inputRef = useRef();

  function handleFile(f) {
    if (f && f.name.endsWith('.csv')) setFile(f);
  }

  async function submit() {
    if (!file || uploading) return;
    if (jobRunning) { onLog('⚠ A job is already running — please wait or cancel first.'); return; }

    const formData = new FormData();
    formData.append('csvFile', file);
    setUploading(true);

    try {
      const res = await fetch('/upload-csv', { method: 'POST', body: formData });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error || 'Upload failed');
      onLog(`📁 ${data.message}`);
      onJobStart();
    } catch (err) {
      onLog(`❌ ${err.message}`);
    } finally {
      setUploading(false);
    }
  }

  return (
    <div className="card">
      <h3>📤 Upload CSV Configuration File</h3>

      <div
        className={`upload-zone ${dragOver ? 'drag-over' : ''}`}
        onClick={() => inputRef.current.click()}
        onDragOver={e => { e.preventDefault(); setDragOver(true); }}
        onDragLeave={() => setDragOver(false)}
        onDrop={e => {
          e.preventDefault();
          setDragOver(false);
          handleFile(e.dataTransfer.files[0]);
        }}
      >
        <input
          ref={inputRef}
          type="file"
          accept=".csv"
          style={{ display: 'none' }}
          onChange={e => handleFile(e.target.files[0])}
        />
        <div className="upload-zone-icon">{dragOver ? '📂' : '📋'}</div>
        <span className="upload-zone-label">
          {dragOver ? 'Drop it here!' : 'Click to browse or drag & drop'}
        </span>
        <span className="upload-zone-sub">Accepts .csv files only</span>
        {file && (
          <div className="selected-file-name">📄 {file.name}</div>
        )}
      </div>

      <div className="mt-12" style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
        <button
          className="btn btn-primary"
          disabled={!file || uploading}
          onClick={submit}
        >
          {uploading
            ? <><span className="spinner" />Uploading...</>
            : '🚀 Upload & Run Job'}
        </button>
        {file && !uploading && (
          <button
            className="btn btn-secondary btn-sm"
            onClick={() => setFile(null)}
          >
            ✕ Clear
          </button>
        )}
      </div>
    </div>
  );
}

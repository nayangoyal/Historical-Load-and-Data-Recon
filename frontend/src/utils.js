export function boolDisplay(val) {
  if (val === true || val === 'true' || val === 1 || val === 'True')
    return { icon: '\u2705', text: 'PASS', cls: 'badge-pass' };
  if (val === false || val === 'false' || val === 0 || val === 'False')
    return { icon: '\u274C', text: 'FAIL', cls: 'badge-fail' };
  return { icon: '\u2014', text: 'N/A', cls: 'badge-na' };
}

export function formatArray(arr) {
  if (!arr) return '\u2014';
  if (Array.isArray(arr)) return arr.join(', ') || '\u2014';
  if (typeof arr === 'string') {
    try { return JSON.parse(arr).join(', ') || '\u2014'; } catch { return arr; }
  }
  return String(arr);
}

export function allChecksPass(row) {
  return [row.row_count_match, row.column_count_match, row.schema_match, row.hash_match]
    .every(v => v === true || v === 'true' || v === 1 || v === 'True');
}

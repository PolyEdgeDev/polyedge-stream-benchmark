/**
 * CLI Terminal Formatting Utilities
 */

export const colors = {
  reset: '\x1b[0m',
  bold: '\x1b[1m',
  dim: '\x1b[2m',
  green: '\x1b[32m',
  cyan: '\x1b[36m',
  yellow: '\x1b[33m',
  magenta: '\x1b[35m',
  red: '\x1b[31m',
  gray: '\x1b[90m'
};

export function formatLead(val, suffix = 'ms') {
  if (val === undefined || val === null || isNaN(val)) return '-';
  if (val > 0) {
    return colors.green + `+${val.toLocaleString()} ${suffix} ✓` + colors.reset;
  } else if (val < 0) {
    return colors.yellow + `-${Math.abs(val).toLocaleString()} ${suffix}` + colors.reset;
  }
  return `0 ${suffix}`;
}

export function stripAnsi(str) {
  return str.replace(/\x1b\[[0-9;]*m/g, '');
}

export function renderTable(headers, rows) {
  const colWidths = headers.map((h, i) => {
    const maxRowWidth = Math.max(...rows.map(r => stripAnsi(String(r[i] || '')).length), 0);
    return Math.max(stripAnsi(h).length, maxRowWidth) + 2;
  });

  const headerLine = headers.map((h, i) => h.padEnd(colWidths[i])).join(' ');
  const sepLine = colWidths.map(w => '─'.repeat(w)).join(' ');

  console.log(colors.bold + headerLine + colors.reset);
  console.log(colors.gray + sepLine + colors.reset);

  for (const row of rows) {
    const rowLine = row.map((cell, i) => {
      const cellStr = String(cell || '');
      const pad = ' '.repeat(Math.max(0, colWidths[i] - stripAnsi(cellStr).length));
      return cellStr + pad;
    }).join(' ');
    console.log(rowLine);
  }
}

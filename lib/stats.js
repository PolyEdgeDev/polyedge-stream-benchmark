/**
 * Mathematical statistics and percentile computations
 */

export function computeStats(values) {
  if (!values || values.length === 0) {
    return {
      count: 0,
      min: 0,
      p50: 0,
      p90: 0,
      p99: 0,
      max: 0
    };
  }

  const sorted = [...values].sort((a, b) => a - b);
  const n = sorted.length;

  const getPercentile = (p) => {
    const idx = (p / 100) * (n - 1);
    const low = Math.floor(idx);
    const high = Math.ceil(idx);
    if (low === high) return sorted[low];
    return Math.round(sorted[low] + (sorted[high] - sorted[low]) * (idx - low));
  };

  return {
    count: n,
    min: sorted[0],
    p50: getPercentile(50),
    p90: getPercentile(90),
    p99: getPercentile(99),
    max: sorted[n - 1]
  };
}

/** Nearest-rank percentile over an ascending array. */
export function percentile(sorted, p) {
  if (sorted.length === 0) return Number.NaN;
  const rank = Math.ceil((p / 100) * sorted.length);
  return sorted[Math.min(sorted.length, Math.max(1, rank)) - 1];
}

export function summarize(values) {
  const sorted = [...values].sort((a, b) => a - b);
  const sum = sorted.reduce((acc, v) => acc + v, 0);
  return {
    count: sorted.length,
    mean: sorted.length ? sum / sorted.length : Number.NaN,
    min: sorted[0] ?? Number.NaN,
    p50: percentile(sorted, 50),
    p95: percentile(sorted, 95),
    p99: percentile(sorted, 99),
    max: sorted.at(-1) ?? Number.NaN,
  };
}

export const round = (n, digits = 1) => (Number.isFinite(n) ? Math.round(n * 10 ** digits) / 10 ** digits : n);

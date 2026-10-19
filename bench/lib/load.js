import { performance } from 'node:perf_hooks';
import { setTimeout as sleep } from 'node:timers/promises';
import { Pool } from 'undici';

/**
 * Fires `total` POST requests using `concurrency` keep-alive connections.
 *
 * Closed loop (default): each connection sends its next request as soon as
 * the previous one returns, which finds the saturation throughput.
 * Open loop (ratePerSec): request i is scheduled at t0 + i/rate, which holds
 * a steady arrival rate for latency measurements below saturation.
 */
export async function runLoad({ baseUrl, path = '/orders', total, concurrency, ratePerSec, makeBody }) {
  const pool = new Pool(baseUrl, { connections: concurrency, pipelining: 1 });
  const results = new Array(total);
  let next = 0;
  const t0 = performance.now();

  async function worker() {
    for (;;) {
      const i = next;
      next += 1;
      if (i >= total) return;

      if (ratePerSec) {
        const wait = t0 + (i * 1000) / ratePerSec - performance.now();
        if (wait > 0) await sleep(wait);
      }

      const started = performance.now();
      try {
        const { statusCode, body } = await pool.request({
          path,
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(makeBody(i)),
        });
        const json = await body.json();
        results[i] = {
          status: statusCode,
          latencyMs: performance.now() - started,
          eventId: json.event_id,
          committedAtMs: json.committed_at_ms,
          txnMs: json.txn_ms,
        };
      } catch (err) {
        results[i] = { status: 0, latencyMs: performance.now() - started, error: err.message };
      }
    }
  }

  await Promise.all(Array.from({ length: concurrency }, worker));
  const durationMs = performance.now() - t0;
  await pool.close();

  const ok = results.filter((r) => r.status === 201);
  return { durationMs, results, ok, errors: total - ok.length, throughput: ok.length / (durationMs / 1000) };
}

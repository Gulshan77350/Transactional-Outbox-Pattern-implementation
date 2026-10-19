import { stopAll } from '../../scripts/lib/processes.js';
import { summarize } from '../lib/stats.js';
import { e2eLatencies, loadOrders, resetDbs, startApi, startConsumer, startRelay, waitForDelivery } from '../lib/stack.js';

async function runOnce(admin, scale, pollIntervalMs) {
  await resetDbs(admin);
  const services = [];
  try {
    services.push(await startRelay(undefined, { POLL_INTERVAL_MS: String(pollIntervalMs) }));
    services.push(await startConsumer());
    services.push(await startApi());

    const warmup = Math.ceil(scale.e2eRate);
    await loadOrders({ total: warmup, concurrency: 20, ratePerSec: scale.e2eRate });
    await waitForDelivery(admin, warmup);

    const load = await loadOrders({ total: scale.e2eRate * scale.e2eSeconds, concurrency: 50, ratePerSec: scale.e2eRate });
    await waitForDelivery(admin, warmup + load.ok.length);
    return { load, e2e: await e2eLatencies(admin, load.ok) };
  } finally {
    await stopAll(services);
  }
}

/**
 * Outbox lag at a steady arrival rate below saturation: time from the
 * order transaction committing to the consumer receiving the event, for
 * several relay poll intervals. Each configuration runs `e2eRepeats` times
 * on a fresh stack and the samples are pooled, so one disk hiccup on a
 * laptop does not decide the result; the per-run p50 spread is reported.
 */
export async function run({ admin, scale, log }) {
  const rows = [];
  for (const pollIntervalMs of scale.pollIntervals) {
    const runs = [];
    for (let i = 1; i <= scale.e2eRepeats; i += 1) {
      log(`poll ${pollIntervalMs}ms, run ${i}/${scale.e2eRepeats}: ${scale.e2eRate * scale.e2eSeconds} orders at ${scale.e2eRate}/s`);
      runs.push(await runOnce(admin, scale, pollIntervalMs));
    }
    const runP50s = runs.map((r) => summarize(r.e2e).p50);
    rows.push({
      pollIntervalMs,
      ratePerSec: scale.e2eRate,
      runs: runs.length,
      orders: runs.reduce((n, r) => n + r.load.ok.length, 0),
      errors: runs.reduce((n, r) => n + r.load.errors, 0),
      e2e: summarize(runs.flatMap((r) => r.e2e)),
      e2eP50Range: [Math.min(...runP50s), Math.max(...runP50s)],
      requestLatency: summarize(runs.flatMap((r) => r.load.ok.map((x) => x.latencyMs))),
      txnLatency: summarize(runs.flatMap((r) => r.load.ok.map((x) => x.txnMs))),
    });
  }
  return rows;
}

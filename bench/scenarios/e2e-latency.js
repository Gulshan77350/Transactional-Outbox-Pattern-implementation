import { stopAll } from '../../scripts/lib/processes.js';
import { summarize } from '../lib/stats.js';
import { e2eLatencies, loadOrders, resetDbs, startApi, startConsumer, startRelay, waitForDelivery } from '../lib/stack.js';

/**
 * Outbox lag at a steady arrival rate below saturation: time from the
 * order transaction committing to the consumer receiving the event, for
 * several relay poll intervals.
 */
export async function run({ admin, scale, log }) {
  const rows = [];
  for (const pollIntervalMs of scale.pollIntervals) {
    await resetDbs(admin);
    const services = [];
    try {
      services.push(await startRelay(undefined, { POLL_INTERVAL_MS: String(pollIntervalMs) }));
      services.push(await startConsumer());
      services.push(await startApi());

      const warmup = Math.ceil(scale.e2eRate);
      await loadOrders({ total: warmup, concurrency: 20, ratePerSec: scale.e2eRate });
      await waitForDelivery(admin, warmup);

      const total = scale.e2eRate * scale.e2eSeconds;
      log(`poll ${pollIntervalMs}ms: ${total} orders at ${scale.e2eRate}/s`);
      const load = await loadOrders({ total, concurrency: 50, ratePerSec: scale.e2eRate });
      await waitForDelivery(admin, warmup + load.ok.length);

      rows.push({
        pollIntervalMs,
        ratePerSec: scale.e2eRate,
        orders: total,
        errors: load.errors,
        e2e: summarize(await e2eLatencies(admin, load.ok)),
        requestLatency: summarize(load.ok.map((r) => r.latencyMs)),
      });
    } finally {
      await stopAll(services);
    }
  }
  return rows;
}

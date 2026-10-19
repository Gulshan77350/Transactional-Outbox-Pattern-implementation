import { performance } from 'node:perf_hooks';
import { stopAll } from '../../scripts/lib/processes.js';
import { summarize } from '../lib/stats.js';
import { e2eLatencies, loadOrders, resetDbs, startApi, startConsumer, startRelay, waitForDelivery } from '../lib/stack.js';

/**
 * Saturation run against the full stack (api + relay + consumer sharing one
 * MongoDB): how many orders/sec the write path accepts, the latency of the
 * order+outbox transaction, and how far behind the relay falls under load.
 */
export async function run({ admin, scale, log }) {
  await resetDbs(admin);
  const services = [];
  try {
    services.push(await startRelay());
    services.push(await startConsumer());
    services.push(await startApi());

    log(`warmup: ${scale.warmup} orders`);
    await loadOrders({ total: scale.warmup, concurrency: 20 });
    await waitForDelivery(admin, scale.warmup);

    log(`load: ${scale.throughputOrders} orders, ${scale.concurrency} concurrent connections`);
    const load = await loadOrders({ total: scale.throughputOrders, concurrency: scale.concurrency });
    const loadEndedAt = performance.now();
    await waitForDelivery(admin, scale.warmup + load.ok.length);
    const drainAfterLoadMs = performance.now() - loadEndedAt;

    return {
      orders: scale.throughputOrders,
      concurrency: scale.concurrency,
      errors: load.errors,
      durationMs: load.durationMs,
      throughput: load.throughput,
      requestLatency: summarize(load.ok.map((r) => r.latencyMs)),
      txnLatency: summarize(load.ok.map((r) => r.txnMs)),
      e2eUnderSaturation: summarize(await e2eLatencies(admin, load.ok)),
      drainAfterLoadMs,
    };
  } finally {
    await stopAll(services);
  }
}

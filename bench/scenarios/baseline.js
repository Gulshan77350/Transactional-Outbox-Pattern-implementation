import { setTimeout as sleep } from 'node:timers/promises';
import { mongoUri, sampleOrder } from '../../scripts/lib/helpers.js';
import { startService, stopAll } from '../../scripts/lib/processes.js';
import { runLoad } from '../lib/load.js';
import { summarize } from '../lib/stats.js';
import { DBS, loadOrders, resetDbs, startApi, startConsumer, startRelay, waitForDelivery } from '../lib/stack.js';

const NAIVE_PORT = 3310;
const NAIVE_SOCKET_PORT = 4310;

/** Waits until the count stops changing for `quietMs` (naive has no "done" signal). */
async function waitUntilStable(count, quietMs = 2_000) {
  let last = -1;
  let stableSince = Date.now();
  for (;;) {
    const current = await count();
    if (current !== last) {
      last = current;
      stableSince = Date.now();
    } else if (Date.now() - stableSince >= quietMs) {
      return current;
    }
    await sleep(100);
  }
}

async function runNaive(admin, scale, log) {
  await resetDbs(admin, [DBS.naive, DBS.naiveBilling]);
  const services = [];
  try {
    services.push(
      await startService('naive', 'src/baseline/naive-api.js', {
        LOG_LEVEL: 'warn',
        MONGO_URI: mongoUri(DBS.naive),
        PORT: String(NAIVE_PORT),
        SOCKET_PORT: String(NAIVE_SOCKET_PORT),
        FAULT_DROP_RATE: String(scale.faultRate),
      }),
    );
    services.push(await startConsumer([NAIVE_SOCKET_PORT], DBS.naiveBilling));

    log(`naive dual-write: ${scale.baselineOrders} orders, fault rate ${scale.faultRate * 100}%`);
    const load = await runLoad({ baseUrl: `http://localhost:${NAIVE_PORT}`, total: scale.baselineOrders, concurrency: 50, makeBody: sampleOrder });
    const processed = await waitUntilStable(() => admin.db(DBS.naiveBilling).collection('processed_events').countDocuments());
    const orders = await admin.db(DBS.naive).collection('orders').countDocuments();
    return summarizeRun('naive dual-write', load, orders, processed);
  } finally {
    await stopAll(services);
  }
}

async function runOutbox(admin, scale, log) {
  await resetDbs(admin);
  const services = [];
  try {
    services.push(await startRelay(undefined, { FAULT_DROP_RATE: String(scale.faultRate), LEASE_MS: '3000', ACK_TIMEOUT_MS: '2000' }));
    services.push(await startConsumer());
    services.push(await startApi());

    log(`transactional outbox: ${scale.baselineOrders} orders, fault rate ${scale.faultRate * 100}%`);
    const load = await loadOrders({ total: scale.baselineOrders, concurrency: 50 });
    await waitForDelivery(admin, load.ok.length);
    const orders = await admin.db(DBS.outbox).collection('orders').countDocuments();
    const processed = await admin.db(DBS.billing).collection('processed_events').countDocuments();
    return summarizeRun('transactional outbox', load, orders, processed);
  } finally {
    await stopAll(services);
  }
}

function summarizeRun(design, load, orders, processed) {
  return {
    design,
    orders,
    eventsDelivered: processed,
    eventsLost: orders - processed,
    lossRatePct: ((orders - processed) / orders) * 100,
    throughput: load.throughput,
    requestLatency: summarize(load.ok.map((r) => r.latencyMs)),
  };
}

/**
 * Same load, same injected fault (the process "dies" between the database
 * write and the publish for faultRate of events) against the naive
 * dual-write and the outbox. The point of comparison is consistency, not
 * speed: the outbox adds a transaction and a relay hop.
 */
export async function run({ admin, scale, log }) {
  const naive = await runNaive(admin, scale, log);
  const outbox = await runOutbox(admin, scale, log);
  return { faultRate: scale.faultRate, naive, outbox };
}

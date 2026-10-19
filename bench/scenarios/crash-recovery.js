import { performance } from 'node:perf_hooks';
import { outboxCounts } from '../../scripts/lib/helpers.js';
import { stopAll } from '../../scripts/lib/processes.js';
import { DBS, duplicatesSkipped, loadOrders, resetDbs, startApi, startConsumer, startRelay, waitForDelivery } from '../lib/stack.js';

const RELAY_ENV = { LEASE_MS: '5000', ACK_TIMEOUT_MS: '2000' };

/**
 * The relay is killed mid-run (after consumers acked, before rows were
 * marked SENT) while the API keeps taking orders. Measures how long a fresh
 * relay takes to deliver everything that piled up, and confirms no event
 * was lost or processed twice.
 */
export async function run({ admin, scale, log }) {
  await resetDbs(admin);
  const services = [];
  try {
    const doomedRelay = await startRelay(undefined, { ...RELAY_ENV, FAULT_CRASH_AFTER_EMITS: String(scale.crashAfterEmits) });
    services.push(doomedRelay);
    services.push(await startConsumer());
    services.push(await startApi());

    log(`load: ${scale.recoveryOrders} orders; relay crashes after ${scale.crashAfterEmits} emits`);
    const load = await loadOrders({ total: scale.recoveryOrders, concurrency: 50 });
    const { code } = await doomedRelay.exited;
    const stuck = await outboxCounts(admin.db(DBS.outbox));

    log(`relay exited with ${code}; ${stuck.PENDING} PENDING + ${stuck.PROCESSING} PROCESSING. Restarting relay`);
    const restartedAt = performance.now();
    services.push(await startRelay(undefined, RELAY_ENV));
    await waitForDelivery(admin, load.ok.length);
    const recoveryMs = performance.now() - restartedAt;

    const orders = await admin.db(DBS.outbox).collection('orders').countDocuments();
    const processed = await admin.db(DBS.billing).collection('processed_events').countDocuments();
    return {
      orders,
      crashAfterEmits: scale.crashAfterEmits,
      relayExitCode: code,
      stuckPending: stuck.PENDING,
      stuckProcessing: stuck.PROCESSING,
      stuckTotal: stuck.PENDING + stuck.PROCESSING,
      recoveryMs,
      recoveryEventsPerSec: (stuck.PENDING + stuck.PROCESSING) / (recoveryMs / 1000),
      lost: orders - processed,
      duplicatesSkipped: await duplicatesSkipped(admin),
    };
  } finally {
    await stopAll(services);
  }
}

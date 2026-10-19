/**
 * Demo: the relay dies between "consumer received the event" and "outbox row
 * marked SENT", while the API keeps accepting orders. On restart nothing is
 * lost, and the events delivered right before the crash are redelivered and
 * deduplicated by the consumer.
 *
 * Requires MongoDB from docker compose:  docker compose up -d mongo
 */
import { setTimeout as sleep } from 'node:timers/promises';
import { check, connectAdmin, dropDatabases, mongoUri, outboxCounts, postOrder, sampleOrder, step, waitFor } from './lib/helpers.js';
import { startService, stopAll } from './lib/processes.js';

const OUTBOX_DB = 'demo_crash_outbox';
const BILLING_DB = 'demo_crash_billing';
const API_URL = 'http://localhost:3100';
const RELAY_PORT = 4100;
const FIRST_WAVE = 20;
const SECOND_WAVE = 10;
const CRASH_AFTER = 5;
const LEASE_MS = 3_000;

const relayEnv = (extra = {}) => ({
  MONGO_URI: mongoUri(OUTBOX_DB),
  SOCKET_PORT: String(RELAY_PORT),
  LEASE_MS: String(LEASE_MS),
  ACK_TIMEOUT_MS: '2000',
  BATCH_SIZE: '20',
  POLL_INTERVAL_MS: '200',
  ...extra,
});

const admin = await connectAdmin();
const outboxDb = admin.db(OUTBOX_DB);
const billingDb = admin.db(BILLING_DB);
const services = [];

try {
  step(1, 'Start api, relay (rigged to crash after 5 emits) and billing consumer');
  await dropDatabases(admin, [OUTBOX_DB, BILLING_DB]);
  const relay = await startService('relay', 'src/entrypoints/relay.js', relayEnv({ FAULT_CRASH_AFTER_EMITS: String(CRASH_AFTER) }), { echo: true });
  services.push(relay);
  services.push(
    await startService('consumer', 'src/entrypoints/consumer.js', { MONGO_URI: mongoUri(BILLING_DB), RELAY_URLS: `http://localhost:${RELAY_PORT}` }, { echo: true }),
  );
  services.push(await startService('api', 'src/entrypoints/api.js', { MONGO_URI: mongoUri(OUTBOX_DB), PORT: '3100', LOG_LEVEL: 'warn' }, { echo: true }));

  step(2, `Create ${FIRST_WAVE} orders`);
  await Promise.all(Array.from({ length: FIRST_WAVE }, (_, i) => postOrder(API_URL, sampleOrder(i))));
  const { code } = await relay.exited;
  console.log(`  relay process exited with code ${code}`);

  step(3, 'State right after the crash');
  const afterCrash = await outboxCounts(outboxDb);
  const billedAfterCrash = await billingDb.collection('billing_notifications').countDocuments();
  console.table(afterCrash);
  console.log(`  billing notifications so far: ${billedAfterCrash}`);
  console.log(`  ${afterCrash.PROCESSING} rows are stuck PROCESSING under the dead relay's lease;`);
  console.log('  some of them already reached the consumer, but the relay never marked them SENT.');

  step(4, `Relay is down, but the write path is not: create ${SECOND_WAVE} more orders`);
  await Promise.all(Array.from({ length: SECOND_WAVE }, (_, i) => postOrder(API_URL, sampleOrder(FIRST_WAVE + i))));
  console.table(await outboxCounts(outboxDb));

  step(5, `Restart the relay (no fault). Stuck rows are released once their ${LEASE_MS}ms lease expires`);
  const restartedAt = Date.now();
  services.push(await startService('relay-2', 'src/entrypoints/relay.js', relayEnv(), { echo: true }));
  const total = FIRST_WAVE + SECOND_WAVE;
  await waitFor(async () => (await outboxCounts(outboxDb)).SENT === total, { timeoutMs: 30_000, label: 'all events SENT' });
  console.log(`  all ${total} events SENT ${Date.now() - restartedAt}ms after restart`);
  await sleep(200);

  step(6, 'Verify');
  const final = await outboxCounts(outboxDb);
  const orders = await outboxDb.collection('orders').countDocuments();
  const notifications = await billingDb.collection('billing_notifications').countDocuments();
  const distinctOrders = (await billingDb.collection('billing_notifications').distinct('order_id')).length;
  const duplicates = (await billingDb.collection('consumer_stats').findOne({ _id: 'billing' }))?.duplicates_skipped ?? 0;
  console.table({ orders, outbox_sent: final.SENT, billing_notifications: notifications, duplicates_skipped: duplicates });

  check(final.SENT === orders, `every committed order has a SENT outbox event (${final.SENT}/${orders})`);
  check(notifications === orders && distinctOrders === orders, `billing notified exactly once per order (${notifications}/${orders})`);
  check(duplicates > 0, `redelivered events were absorbed by the consumer (${duplicates} duplicates skipped)`);
  process.exitCode = final.SENT === orders && notifications === orders ? 0 : 1;
} finally {
  await stopAll(services);
  await admin.close();
}

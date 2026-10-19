/**
 * Demo: the same event is delivered to the consumer 8 times (3 sequential
 * redeliveries, then 5 concurrent copies racing each other). The consumer
 * acks all of them but bills the order exactly once.
 *
 * This script plays a misbehaving relay: it hosts the same Socket.IO hub and
 * transport the real relay uses, and pushes one real outbox row repeatedly.
 *
 * Requires MongoDB from docker compose:  docker compose up -d mongo
 */
import http from 'node:http';
import { connectMongo } from '../src/db/mongo.js';
import { ensureOutboxIndexes } from '../src/db/indexes.js';
import { createOrderService } from '../src/orders/order.service.js';
import { toEnvelope } from '../src/outbox/outbox-event.js';
import { createSocketHub, createSocketTransport } from '../src/relay/socket-server.js';
import { check, connectAdmin, dropDatabases, mongoUri, sampleOrder, step, waitFor } from './lib/helpers.js';
import { startService, stopAll } from './lib/processes.js';

const OUTBOX_DB = 'demo_dup_outbox';
const BILLING_DB = 'demo_dup_billing';
const PORT = 4200;

const admin = await connectAdmin();
await dropDatabases(admin, [OUTBOX_DB, BILLING_DB]);
const { client, db } = await connectMongo(mongoUri(OUTBOX_DB));
await ensureOutboxIndexes(db);

const httpServer = http.createServer();
const io = createSocketHub({ httpServer });
const transport = createSocketTransport({ io, ackTimeoutMs: 5_000 });
await new Promise((resolve) => httpServer.listen(PORT, resolve));

let consumer;
try {
  step(1, 'Start the billing consumer and create one order');
  consumer = await startService('consumer', 'src/entrypoints/consumer.js', { MONGO_URI: mongoUri(BILLING_DB), RELAY_URLS: `http://localhost:${PORT}` }, { echo: true });
  await waitFor(() => transport.subscriberCount('OrderCreated') > 0, { label: 'consumer subscription' });
  const { order, eventId } = await createOrderService({ client, db }).createOrder(sampleOrder(1));
  const envelope = toEnvelope(await db.collection('outbox').findOne({ _id: eventId }));
  console.log(`  order ${order._id} -> event ${envelope.id}`);

  step(2, 'Deliver the event, then redeliver it 2 more times (as after a relay crash)');
  for (let i = 1; i <= 3; i += 1) {
    const [ack] = await transport.deliver({ ...envelope, delivery_attempt: i });
    console.log(`  delivery #${i} ack: ${JSON.stringify(ack)}`);
  }

  step(3, 'Deliver 5 more copies concurrently (racing duplicates)');
  const racing = await Promise.all(Array.from({ length: 5 }, (_, i) => transport.deliver({ ...envelope, delivery_attempt: 4 + i })));
  racing.forEach(([ack], i) => console.log(`  concurrent #${i + 1} ack: ${JSON.stringify(ack)}`));

  step(4, 'Verify');
  const billing = admin.db(BILLING_DB);
  const notifications = await billing.collection('billing_notifications').countDocuments({ order_id: order._id.toHexString() });
  const duplicates = (await billing.collection('consumer_stats').findOne({ _id: 'billing' }))?.duplicates_skipped ?? 0;
  console.table({ deliveries: 8, billing_notifications: notifications, duplicates_skipped: duplicates });
  check(notifications === 1, 'order billed exactly once despite 8 deliveries');
  check(duplicates === 7, '7 duplicate deliveries detected and acknowledged without side effects');
  process.exitCode = notifications === 1 && duplicates === 7 ? 0 : 1;
} finally {
  await stopAll([consumer]);
  await new Promise((resolve) => io.close(() => resolve()));
  await client.close();
  await admin.close();
}

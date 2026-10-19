import http from 'node:http';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { createApp } from '../../src/app.js';
import { BillingConsumer } from '../../src/consumer/billing-consumer.js';
import { ensureConsumerIndexes } from '../../src/db/indexes.js';
import { OutboxStatus } from '../../src/outbox/event-types.js';
import { createOrderService } from '../../src/orders/order.service.js';
import { createOutboxRepository } from '../../src/outbox/outbox.repository.js';
import { OutboxRelay } from '../../src/relay/poller.js';
import { createSocketHub, createSocketTransport } from '../../src/relay/socket-server.js';
import { eventually, openTestDb, orderBody, silentLogger } from '../helpers/db.js';

/**
 * Full flow with every real component in one process:
 * HTTP POST /orders -> Mongo transaction -> relay claims the outbox row ->
 * Socket.IO -> billing consumer -> processed_events + billing_notifications
 * -> outbox row SENT.
 */
describe('outbox flow (integration)', () => {
  let ctx;
  let billingDb;
  let apiServer;
  let apiBase;
  const started = [];

  async function startRelay(options = {}) {
    const server = http.createServer();
    const io = createSocketHub({ httpServer: server, logger: silentLogger });
    await new Promise((resolve) => server.listen(0, resolve));
    const relay = new OutboxRelay({
      repo: createOutboxRepository(ctx.db),
      transport: createSocketTransport({ io, ackTimeoutMs: 2_000 }),
      logger: silentLogger,
      pollIntervalMs: 10,
      backoffBaseMs: 20,
      backoffMaxMs: 50,
      ...options,
    });
    relay.start();
    const handle = {
      relay,
      url: `http://localhost:${server.address().port}`,
      async stop() {
        await relay.stop();
        await new Promise((resolve) => io.close(() => resolve()));
      },
    };
    started.push(handle);
    return handle;
  }

  async function startConsumer(relayUrls) {
    const consumer = new BillingConsumer({ client: ctx.client, db: billingDb, relayUrls, logger: silentLogger });
    await consumer.start();
    started.push(consumer);
    return consumer;
  }

  async function createOrders(count) {
    const responses = await Promise.all(
      Array.from({ length: count }, (_, i) =>
        fetch(`${apiBase}/orders`, {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify(orderBody({ customer_id: `c-${i}` })),
        }).then((res) => res.json()),
      ),
    );
    return responses;
  }

  const sentCount = () => ctx.db.collection('outbox').countDocuments({ status: OutboxStatus.SENT });
  const notificationCount = () => billingDb.collection('billing_notifications').countDocuments();

  beforeAll(async () => {
    ctx = await openTestDb('flow');
    billingDb = ctx.client.db(`${ctx.db.databaseName}_billing`);
    apiServer = createApp({ orderService: createOrderService(ctx), logger: silentLogger }).listen(0);
    await new Promise((resolve) => apiServer.once('listening', resolve));
    apiBase = `http://localhost:${apiServer.address().port}`;
  });
  beforeEach(async () => {
    await ctx.reset();
    await billingDb.dropDatabase();
    await ensureConsumerIndexes(billingDb);
  });
  afterEach(async () => {
    for (const component of started.splice(0).reverse()) await component.stop();
  });
  afterAll(async () => {
    await new Promise((resolve) => apiServer.close(resolve));
    await billingDb.dropDatabase();
    await ctx.close();
  });

  it('delivers a created order to the consumer and marks the event SENT', async () => {
    const relay = await startRelay();
    await startConsumer([relay.url]);

    const [{ order, event_id: eventId }] = await createOrders(1);
    await eventually(async () => (await sentCount()) === 1);

    const event = await ctx.db.collection('outbox').findOne({ _id: { $exists: true } });
    expect(event._id.toHexString()).toBe(eventId);
    expect(event).toMatchObject({ status: OutboxStatus.SENT, attempts: 0, claims: 1, locked_by: null });
    expect(await billingDb.collection('processed_events').findOne({ _id: eventId })).not.toBeNull();
    expect(await billingDb.collection('billing_notifications').findOne({ order_id: order.id })).toMatchObject({
      event_id: eventId,
      amount_cents: 2999,
    });
  });

  it('shares the outbox between two relays without double-claiming', async () => {
    const relayA = await startRelay({ workerId: 'relay-a', batchSize: 10 });
    const relayB = await startRelay({ workerId: 'relay-b', batchSize: 10 });
    await startConsumer([relayA.url, relayB.url]);

    await createOrders(120);
    await eventually(async () => (await sentCount()) === 120, { timeoutMs: 20_000 });

    expect(await ctx.db.collection('outbox').countDocuments({ claims: { $ne: 1 } })).toBe(0);
    expect(await notificationCount()).toBe(120);
    expect(relayA.relay.stats.sent + relayB.relay.stats.sent).toBe(120);
  });

  it('recovers when the relay fails after the emit but before mark-as-sent', async () => {
    // The first emit of every event reaches the consumer, then the relay
    // "loses" it (as if it crashed). The retry redelivers it and the
    // consumer recognises the duplicate.
    const failedOnce = new Set();
    const relay = await startRelay({
      hooks: {
        afterDeliver(row) {
          if (failedOnce.has(String(row._id))) return;
          failedOnce.add(String(row._id));
          throw new Error('crashed before mark-as-sent');
        },
      },
    });
    const consumer = await startConsumer([relay.url]);

    await createOrders(10);
    await eventually(async () => (await sentCount()) === 10, { timeoutMs: 15_000 });

    expect(await notificationCount()).toBe(10);
    expect(consumer.stats).toMatchObject({ processed: 10, duplicates: 10 });
    expect(await ctx.db.collection('outbox').countDocuments({ attempts: 1 })).toBe(10);
  });

  it('redelivers rows stranded by a dead relay once their lease expires', async () => {
    const relay = await startRelay({ leaseMs: 150, leaseSweepIntervalMs: 20, hooks: { beforeDeliver: () => 'drop' } });
    await startConsumer([relay.url]);
    await createOrders(5);
    await eventually(async () => (await ctx.db.collection('outbox').countDocuments({ status: OutboxStatus.PROCESSING })) === 5);
    await relay.stop();

    const replacement = await startRelay({ leaseMs: 150, leaseSweepIntervalMs: 20 });
    await startConsumer([replacement.url]);
    await eventually(async () => (await sentCount()) === 5);
    expect(await notificationCount()).toBe(5);
  });

  it('keeps events PENDING while no consumer is connected, then delivers them', async () => {
    const relay = await startRelay({ maxAttempts: 1_000 });
    await createOrders(3);
    await eventually(async () => (await ctx.db.collection('outbox').countDocuments({ attempts: { $gte: 1 } })) === 3);
    expect(await sentCount()).toBe(0);

    await startConsumer([relay.url]);
    await eventually(async () => (await sentCount()) === 3, { timeoutMs: 15_000 });
    expect(await notificationCount()).toBe(3);
  });
});

describe('consumer authentication against the relay hub', () => {
  it('connects and subscribes with the shared token', async () => {
    const ctx = await openTestDb('auth');
    const server = http.createServer();
    const io = createSocketHub({ httpServer: server, logger: silentLogger, authToken: 'tok' });
    await new Promise((resolve) => server.listen(0, resolve));
    const transport = createSocketTransport({ io, ackTimeoutMs: 2_000 });

    const consumer = new BillingConsumer({
      client: ctx.client,
      db: ctx.db,
      relayUrls: [`http://localhost:${server.address().port}`],
      authToken: 'tok',
      logger: silentLogger,
    });
    await consumer.start();
    expect(transport.subscriberCount('OrderCreated')).toBe(1);

    await consumer.stop();
    await new Promise((resolve) => io.close(() => resolve()));
    await ctx.close();
  });
});

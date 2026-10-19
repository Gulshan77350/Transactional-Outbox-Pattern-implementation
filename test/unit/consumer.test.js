import { ObjectId } from 'mongodb';
import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { BillingConsumer } from '../../src/consumer/billing-consumer.js';
import { createIdempotencyStore } from '../../src/consumer/idempotency-store.js';
import { openTestDb, silentLogger } from '../helpers/db.js';

const envelope = (overrides = {}) => {
  const orderId = new ObjectId().toHexString();
  return {
    id: new ObjectId().toHexString(),
    type: 'OrderCreated',
    aggregate_id: orderId,
    payload: { order_id: orderId, customer_id: 'c-1', total_cents: 2999, currency: 'USD', item_count: 2 },
    delivery_attempt: 1,
    ...overrides,
  };
};

describe('billing consumer', () => {
  let ctx;
  let consumer;
  let notifications;

  beforeAll(async () => {
    ctx = await openTestDb('consumer');
    notifications = ctx.db.collection('billing_notifications');
  });
  beforeEach(async () => {
    await ctx.reset();
    consumer = new BillingConsumer({ client: ctx.client, db: ctx.db, relayUrls: [], logger: silentLogger });
  });
  afterAll(() => ctx.close());

  it('records one billing notification for a new event', async () => {
    const event = envelope();
    expect(await consumer.handle(event)).toEqual({ ok: true, duplicate: false });

    expect(await notifications.findOne({ event_id: event.id })).toMatchObject({ order_id: event.payload.order_id, amount_cents: 2999, currency: 'USD' });
    const marker = await ctx.db.collection('processed_events').findOne({ _id: event.id });
    expect(marker).toMatchObject({ type: 'OrderCreated', consumer: 'billing' });
    expect(marker.received_at_ms).toBeGreaterThan(0);
  });

  it('acknowledges a redelivered event without processing it again', async () => {
    const event = envelope();
    await consumer.handle(event);
    expect(await consumer.handle({ ...event, delivery_attempt: 2 })).toEqual({ ok: true, duplicate: true });
    expect(await consumer.handle({ ...event, delivery_attempt: 3 })).toEqual({ ok: true, duplicate: true });

    expect(await notifications.countDocuments({ event_id: event.id })).toBe(1);
    expect(consumer.stats).toMatchObject({ received: 3, processed: 1, duplicates: 2 });
    expect(await consumer.store.duplicatesSkipped()).toBe(2);
  });

  it('processes exactly once when duplicates arrive concurrently', async () => {
    const event = envelope();
    const results = await Promise.all(Array.from({ length: 10 }, () => consumer.handle(event)));

    expect(results.filter((r) => !r.duplicate)).toHaveLength(1);
    expect(await notifications.countDocuments({ event_id: event.id })).toBe(1);
  });

  it('ignores event types it does not handle', async () => {
    expect(await consumer.handle(envelope({ type: 'OrderShipped' }))).toEqual({ ok: true, ignored: true });
    expect(await notifications.countDocuments()).toBe(0);
  });

  it('rejects an envelope without an event id', async () => {
    await expect(consumer.handle(envelope({ id: '' }))).rejects.toThrow(/missing an event id/);
    await expect(consumer.handle(null)).rejects.toThrow(/missing an event id/);
  });
});

describe('idempotency store', () => {
  let ctx;
  let store;

  beforeAll(async () => {
    ctx = await openTestDb('idem');
    store = createIdempotencyStore({ client: ctx.client, db: ctx.db, consumerName: 'test' });
  });
  beforeEach(() => ctx.reset());
  afterAll(() => ctx.close());

  it('rolls back the processed marker when the handler fails, so a retry can succeed', async () => {
    const event = envelope();
    await expect(
      store.processOnce(event, async () => {
        throw new Error('downstream unavailable');
      }),
    ).rejects.toThrow('downstream unavailable');
    expect(await store.hasProcessed(event.id)).toBe(false);

    expect(await store.processOnce(event, async () => {})).toEqual({ duplicate: false });
    expect(await store.hasProcessed(event.id)).toBe(true);
  });

  it('rolls back handler side effects of a duplicate', async () => {
    const event = envelope();
    const sideEffects = ctx.db.collection('side_effects');
    const handler = (session) => sideEffects.insertOne({ event_id: event.id }, { session });

    await store.processOnce(event, handler);
    expect(await store.processOnce(event, handler)).toEqual({ duplicate: true });
    expect(await sideEffects.countDocuments()).toBe(1);
    expect(await store.duplicatesSkipped()).toBe(1);
  });
});

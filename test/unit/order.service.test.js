import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { connectMongo } from '../../src/db/mongo.js';
import { OutboxStatus } from '../../src/outbox/event-types.js';
import { createOrderService } from '../../src/orders/order.service.js';
import { ValidationError } from '../../src/orders/order.validation.js';
import { openTestDb, orderBody } from '../helpers/db.js';

describe('order service: order + outbox transaction', () => {
  let ctx;
  let service;

  beforeAll(async () => {
    ctx = await openTestDb('orders');
    service = createOrderService(ctx);
  });
  beforeEach(() => ctx.reset());
  afterAll(() => ctx.close());

  it('commits the order and its OrderCreated event together', async () => {
    const { order, eventId, txnMs, committedAtMs } = await service.createOrder(orderBody());

    const storedOrder = await ctx.db.collection('orders').findOne({ _id: order._id });
    const event = await ctx.db.collection('outbox').findOne({ _id: eventId });

    expect(storedOrder).toMatchObject({ customer_id: 'customer-1', total_cents: 2999, status: 'CREATED' });
    expect(event).toMatchObject({
      event_type: 'OrderCreated',
      aggregate_type: 'order',
      status: OutboxStatus.PENDING,
      attempts: 0,
      payload: { order_id: order._id.toHexString(), customer_id: 'customer-1', total_cents: 2999, currency: 'USD', item_count: 2 },
    });
    expect(event.aggregate_id).toEqual(order._id);
    expect(event.created_at).toEqual(storedOrder.created_at);
    expect(txnMs).toBeGreaterThan(0);
    expect(committedAtMs).toBeGreaterThan(Date.now() - 60_000);
  });

  it('rolls back the order when the outbox insert fails', async () => {
    // A validator that rejects every outbox document makes the second write
    // of the transaction fail after the first one already succeeded.
    await ctx.db.collection('outbox').drop();
    await ctx.db.createCollection('outbox', { validator: { event_type: { $exists: false } } });

    await expect(service.createOrder(orderBody())).rejects.toThrow(/validation/i);
    expect(await ctx.db.collection('orders').countDocuments()).toBe(0);
    expect(await ctx.db.collection('outbox').countDocuments()).toBe(0);
  });

  it('writes no outbox event when the order insert fails', async () => {
    await ctx.db.collection('orders').drop();
    await ctx.db.createCollection('orders', { validator: { total_cents: { $lt: 0 } } });

    await expect(service.createOrder(orderBody())).rejects.toThrow(/validation/i);
    expect(await ctx.db.collection('orders').countDocuments()).toBe(0);
    expect(await ctx.db.collection('outbox').countDocuments()).toBe(0);
  });

  it('writes nothing for an invalid request', async () => {
    await expect(service.createOrder({ customer_id: '' })).rejects.toBeInstanceOf(ValidationError);
    expect(await ctx.db.collection('orders').countDocuments()).toBe(0);
    expect(await ctx.db.collection('outbox').countDocuments()).toBe(0);
  });

  it('keeps orders and events one-to-one under concurrent writes', async () => {
    const results = await Promise.all(Array.from({ length: 50 }, (_, i) => service.createOrder(orderBody({ customer_id: `c-${i}` }))));

    const events = await ctx.db.collection('outbox').find().toArray();
    expect(await ctx.db.collection('orders').countDocuments()).toBe(50);
    expect(events).toHaveLength(50);
    expect(new Set(events.map((e) => e.aggregate_id.toHexString()))).toEqual(new Set(results.map((r) => r.order._id.toHexString())));
  });

  it('looks orders up by id', async () => {
    const { order } = await service.createOrder(orderBody());
    expect((await service.getOrder(order._id.toHexString()))._id).toEqual(order._id);
    expect(await service.getOrder('not-an-id')).toBeNull();
    expect(await service.getOrder('0123456789abcdef01234567')).toBeNull();
  });
});

describe('connectMongo', () => {
  it('connects to a replica set and exposes the database from the URI', async () => {
    const ctx = await openTestDb('connect');
    const url = new URL(ctx.uri);
    url.pathname = '/connect_check';
    const { client, db } = await connectMongo(url.toString());
    expect(db.databaseName).toBe('connect_check');
    await client.close();
    await ctx.close();
  });
});

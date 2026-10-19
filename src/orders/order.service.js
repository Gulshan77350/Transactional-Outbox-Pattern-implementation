import { ObjectId } from 'mongodb';
import { performance } from 'node:perf_hooks';
import { EventTypes } from '../outbox/event-types.js';
import { buildOutboxEvent } from '../outbox/outbox-event.js';
import { parseCreateOrder } from './order.validation.js';

const TXN_OPTIONS = {
  readConcern: { level: 'snapshot' },
  writeConcern: { w: 'majority' },
  readPreference: 'primary',
};

/**
 * Producer side of the outbox pattern. The order and its OrderCreated event
 * are written in one transaction, so either both exist or neither does.
 * Nothing here talks to Socket.IO: publishing is the relay's job.
 */
export function createOrderService({ client, db }) {
  const orders = db.collection('orders');
  const outbox = db.collection('outbox');

  async function createOrder(body) {
    const input = parseCreateOrder(body);
    const now = new Date();
    const order = { _id: new ObjectId(), ...input, status: 'CREATED', created_at: now };
    const event = buildOutboxEvent({
      eventType: EventTypes.ORDER_CREATED,
      aggregateType: 'order',
      aggregateId: order._id,
      payload: {
        order_id: order._id.toHexString(),
        customer_id: order.customer_id,
        total_cents: order.total_cents,
        currency: order.currency,
        item_count: order.items.length,
      },
      now,
    });

    const session = client.startSession();
    const started = performance.now();
    try {
      await session.withTransaction(async () => {
        await orders.insertOne(order, { session });
        await outbox.insertOne(event, { session });
      }, TXN_OPTIONS);
    } finally {
      await session.endSession();
    }

    return {
      order,
      eventId: event._id,
      // High-resolution wall clock of the commit; the benchmark uses it to
      // measure commit -> consumer latency across processes on one host.
      committedAtMs: performance.timeOrigin + performance.now(),
      txnMs: performance.now() - started,
    };
  }

  async function getOrder(id) {
    if (!ObjectId.isValid(id)) return null;
    return orders.findOne({ _id: new ObjectId(id) });
  }

  return { createOrder, getOrder };
}

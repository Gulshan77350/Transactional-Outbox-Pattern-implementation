import { performance } from 'node:perf_hooks';
import { io as connectSocket } from 'socket.io-client';
import { EventTypes } from '../outbox/event-types.js';
import { Protocol } from '../shared/protocol.js';
import { createSemaphore } from '../shared/semaphore.js';
import { createIdempotencyStore } from './idempotency-store.js';

/**
 * Demo downstream service. Subscribes to OrderCreated on every configured
 * relay and records a billing notification per order, exactly once, no
 * matter how many times the same event is delivered.
 */
export class BillingConsumer {
  #sockets = [];

  constructor({ client, db, relayUrls, logger, name = 'billing', maxConcurrency = 8, authToken, connect = connectSocket }) {
    Object.assign(this, { relayUrls, logger, name, authToken, connect });
    // Bounds in-flight transactions. Unbounded, a 100-event batch becomes 100
    // concurrent transactions whose write conflicts and retries cut
    // throughput in half and push acks past the relay's timeout.
    this.slots = createSemaphore(maxConcurrency);
    this.store = createIdempotencyStore({ client, db, consumerName: name });
    this.notifications = db.collection('billing_notifications');
    this.stats = { received: 0, processed: 0, duplicates: 0, errors: 0 };
  }

  /** Connects to every relay and resolves once each has confirmed the subscription. */
  async start() {
    await Promise.all(this.relayUrls.map((url) => this.#connect(url)));
  }

  async stop() {
    for (const socket of this.#sockets) socket.close();
    this.#sockets = [];
  }

  #connect(url) {
    const socket = this.connect(url, {
      transports: ['websocket'],
      reconnectionDelayMax: 2_000,
      ...(this.authToken ? { auth: { token: this.authToken } } : {}),
    });
    this.#sockets.push(socket);

    socket.on(Protocol.EVENT, async (envelope, ack) => {
      const reply = typeof ack === 'function' ? ack : () => {};
      try {
        reply(await this.slots.run(() => this.handle(envelope)));
      } catch (err) {
        this.stats.errors += 1;
        this.logger?.error({ err, eventId: envelope?.id }, 'event handling failed');
        reply({ ok: false, error: err.message });
      }
    });
    socket.on('disconnect', (reason) => this.logger?.warn({ url, reason }, 'disconnected from relay'));
    socket.on('connect_error', (err) => this.logger?.warn({ url, err: err.message }, 'cannot connect to relay, retrying'));

    // (Re)subscribe on every connect so a relay restart is transparent.
    return new Promise((resolve) => {
      socket.on('connect', () => {
        socket.emit(Protocol.SUBSCRIBE, [EventTypes.ORDER_CREATED], () => {
          this.logger?.info({ url }, 'subscribed to relay');
          resolve();
        });
      });
    });
  }

  async handle(envelope) {
    const receivedAtMs = performance.timeOrigin + performance.now();
    this.stats.received += 1;

    if (!envelope || typeof envelope.id !== 'string' || envelope.id === '') {
      throw new Error('envelope is missing an event id');
    }
    if (envelope.type !== EventTypes.ORDER_CREATED) return { ok: true, ignored: true };

    const { order_id: orderId, customer_id: customerId, total_cents: amountCents, currency } = envelope.payload;
    const { duplicate } = await this.store.processOnce(
      envelope,
      (session) =>
        this.notifications.insertOne(
          { event_id: envelope.id, order_id: orderId, customer_id: customerId, amount_cents: amountCents, currency, created_at: new Date() },
          { session },
        ),
      { receivedAtMs },
    );

    if (duplicate) {
      this.stats.duplicates += 1;
      this.logger?.info({ eventId: envelope.id, orderId, attempt: envelope.delivery_attempt }, 'duplicate event ignored');
    } else {
      this.stats.processed += 1;
      this.logger?.info({ eventId: envelope.id, orderId }, `billing notified for order ${orderId}`);
    }
    return { ok: true, duplicate };
  }
}

/**
 * BASELINE ONLY - the anti-pattern the outbox replaces.
 *
 * Naive dual-write: insert the order, then publish the event directly from
 * the request path. The two writes are not atomic, so a crash, deploy, or
 * network blip between them leaves an order with no event, and nothing ever
 * notices. Used by the benchmark to quantify that loss under the same fault
 * rate the outbox relay is subjected to.
 *
 * The publish is fire-and-forget (the cheapest possible variant), so its
 * write latency is a best case for the naive approach.
 */
import express from 'express';
import http from 'node:http';
import { ObjectId } from 'mongodb';
import { performance } from 'node:perf_hooks';
import { loadConfig } from '../config.js';
import { connectMongo } from '../db/mongo.js';
import { EventTypes } from '../outbox/event-types.js';
import { parseCreateOrder, ValidationError } from '../orders/order.validation.js';
import { createSocketHub } from '../relay/socket-server.js';
import { onShutdown, signalReady } from '../shared/lifecycle.js';
import { createLogger } from '../shared/logger.js';
import { Protocol, roomFor } from '../shared/protocol.js';

const config = loadConfig();
const logger = createLogger('naive-api', config.logLevel);
const { client, db } = await connectMongo(config.mongoUri);
const orders = db.collection('orders');

const socketServer = http.createServer();
const io = createSocketHub({ httpServer: socketServer, logger, authToken: config.relayAuthToken });
await new Promise((resolve) => socketServer.listen(config.socketPort, resolve));

const app = express();
app.use(express.json({ limit: '100kb' }));
app.get('/health', (_req, res) => res.json({ status: 'ok' }));

app.post('/orders', async (req, res) => {
  const started = performance.now();
  const order = { _id: new ObjectId(), ...parseCreateOrder(req.body), status: 'CREATED', created_at: new Date() };

  await orders.insertOne(order); // write #1: database
  const committedAtMs = performance.timeOrigin + performance.now();

  // Simulated crash between the two writes, at the same rate the outbox relay is faulted.
  if (!(config.faults.dropRate > 0 && Math.random() < config.faults.dropRate)) {
    // write #2: publish. Fire-and-forget; nobody retries if it never arrives.
    io.to(roomFor(EventTypes.ORDER_CREATED)).emit(Protocol.EVENT, {
      id: new ObjectId().toHexString(),
      type: EventTypes.ORDER_CREATED,
      aggregate_type: 'order',
      aggregate_id: order._id.toHexString(),
      payload: {
        order_id: order._id.toHexString(),
        customer_id: order.customer_id,
        total_cents: order.total_cents,
        currency: order.currency,
        item_count: order.items.length,
      },
      created_at: order.created_at.toISOString(),
      delivery_attempt: 1,
    });
  }

  res.status(201).json({
    order: { id: order._id.toHexString() },
    committed_at_ms: committedAtMs,
    txn_ms: performance.now() - started,
  });
});

app.use((err, _req, res, _next) => {
  if (err instanceof ValidationError) return res.status(400).json({ error: err.message, details: err.details });
  logger.error({ err }, 'request failed');
  res.status(500).json({ error: 'internal error' });
});

const server = app.listen(config.port, () => {
  logger.info({ port: config.port, socketPort: config.socketPort }, 'naive dual-write api listening');
  signalReady();
});

onShutdown(logger, async () => {
  await new Promise((resolve) => server.close(resolve));
  await new Promise((resolve) => io.close(() => resolve()));
  await client.close();
});

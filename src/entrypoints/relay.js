import http from 'node:http';
import { loadConfig } from '../config.js';
import { ensureOutboxIndexes } from '../db/indexes.js';
import { connectMongo } from '../db/mongo.js';
import { createOutboxRepository } from '../outbox/outbox.repository.js';
import { createFaultHooks } from '../relay/faults.js';
import { OutboxRelay } from '../relay/poller.js';
import { createSocketHub, createSocketTransport } from '../relay/socket-server.js';
import { onShutdown, signalReady } from '../shared/lifecycle.js';
import { createLogger } from '../shared/logger.js';

const config = loadConfig();
const logger = createLogger('relay', config.logLevel);

if (config.relay.leaseMs <= config.relay.ackTimeoutMs) {
  throw new Error('LEASE_MS must be larger than ACK_TIMEOUT_MS, or a slow ack could outlive its lease');
}

const { client, db } = await connectMongo(config.mongoUri);
await ensureOutboxIndexes(db);
const repo = createOutboxRepository(db);

const httpServer = http.createServer(async (req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    return res.end(JSON.stringify({ status: 'ok', running: relay?.running ?? false }));
  }
  if (req.url === '/stats') {
    try {
      const outbox = await repo.countByStatus();
      res.writeHead(200, { 'content-type': 'application/json' });
      return res.end(JSON.stringify({ workerId: relay?.workerId, relay: relay?.stats, outbox }));
    } catch (err) {
      logger.error({ err }, 'stats query failed');
      return res.writeHead(500).end();
    }
  }
  res.writeHead(404).end();
});

const io = createSocketHub({ httpServer, logger, authToken: config.relayAuthToken });
if (!config.relayAuthToken) logger.warn('RELAY_AUTH_TOKEN is not set; any client can subscribe to events');
const relay = new OutboxRelay({
  repo,
  transport: createSocketTransport({ io, ackTimeoutMs: config.relay.ackTimeoutMs }),
  logger,
  ...config.relay,
  hooks: createFaultHooks(config.faults, { logger }),
});

await new Promise((resolve) => httpServer.listen(config.socketPort, resolve));
relay.start();
logger.info({ port: config.socketPort, db: db.databaseName }, 'relay socket.io hub listening');
signalReady({ port: config.socketPort });

onShutdown(logger, async () => {
  await relay.stop();
  await new Promise((resolve) => io.close(() => resolve()));
  await client.close();
});

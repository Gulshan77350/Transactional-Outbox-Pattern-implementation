import { createApp } from '../app.js';
import { loadConfig } from '../config.js';
import { ensureOrderIndexes, ensureOutboxIndexes } from '../db/indexes.js';
import { connectMongo } from '../db/mongo.js';
import { createOrderService } from '../orders/order.service.js';
import { onShutdown, signalReady } from '../shared/lifecycle.js';
import { createLogger } from '../shared/logger.js';

const config = loadConfig();
const logger = createLogger('api', config.logLevel);

const { client, db } = await connectMongo(config.mongoUri);
await Promise.all([ensureOrderIndexes(db), ensureOutboxIndexes(db)]);

const app = createApp({ orderService: createOrderService({ client, db }), logger });
const server = app.listen(config.port, () => {
  logger.info({ port: config.port, db: db.databaseName }, 'order api listening');
  signalReady({ port: config.port });
});

onShutdown(logger, async () => {
  await new Promise((resolve) => server.close(resolve));
  await client.close();
});

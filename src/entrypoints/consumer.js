import { loadConfig } from '../config.js';
import { BillingConsumer } from '../consumer/billing-consumer.js';
import { ensureConsumerIndexes } from '../db/indexes.js';
import { connectMongo } from '../db/mongo.js';
import { onShutdown, signalReady } from '../shared/lifecycle.js';
import { createLogger } from '../shared/logger.js';

const config = loadConfig();
const logger = createLogger('consumer', config.logLevel);

const { client, db } = await connectMongo(config.mongoUri);
await ensureConsumerIndexes(db);

const consumer = new BillingConsumer({
  client,
  db,
  relayUrls: config.consumer.relayUrls,
  maxConcurrency: config.consumer.concurrency,
  logger,
});
await consumer.start();
logger.info({ relays: config.consumer.relayUrls, db: db.databaseName }, 'billing consumer ready');
signalReady();

onShutdown(logger, async () => {
  await consumer.stop();
  await client.close();
});

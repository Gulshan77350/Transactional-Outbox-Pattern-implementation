import { randomUUID } from 'node:crypto';
import { MongoClient } from 'mongodb';
import { inject } from 'vitest';
import { ensureConsumerIndexes, ensureOrderIndexes, ensureOutboxIndexes } from '../../src/db/indexes.js';

/** Connects to the shared test replica set and returns a fresh, uniquely named database. */
export async function openTestDb(prefix) {
  const uri = inject('mongoUri');
  const client = await MongoClient.connect(uri);
  const name = `${prefix}_${randomUUID().slice(0, 8)}`;
  const db = client.db(name);
  return {
    uri,
    client,
    db,
    async reset() {
      await db.dropDatabase();
      await Promise.all([ensureOutboxIndexes(db), ensureOrderIndexes(db), ensureConsumerIndexes(db)]);
    },
    async close() {
      await db.dropDatabase();
      await client.close();
    },
  };
}

export async function eventually(predicate, { timeoutMs = 10_000, intervalMs = 20 } = {}) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (await predicate()) return;
    if (Date.now() > deadline) throw new Error('condition not met in time');
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
}

export const silentLogger = {
  trace() {},
  debug() {},
  info() {},
  warn() {},
  error() {},
  fatal() {},
};

export function orderBody(overrides = {}) {
  return {
    customer_id: 'customer-1',
    currency: 'USD',
    items: [
      { sku: 'SKU-1', quantity: 2, unit_price_cents: 1250 },
      { sku: 'SKU-2', quantity: 1, unit_price_cents: 499 },
    ],
    ...overrides,
  };
}

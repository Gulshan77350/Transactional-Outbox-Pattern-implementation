import { MongoClient } from 'mongodb';
import { setTimeout as sleep } from 'node:timers/promises';

export const MONGO_BASE = process.env.MONGO_BASE_URI ?? 'mongodb://localhost:27017';

/** Host-side URI for a database (direct connection; see .env.example). */
export const mongoUri = (dbName) => `${MONGO_BASE}/${dbName}?directConnection=true`;

export async function connectAdmin() {
  return MongoClient.connect(`${MONGO_BASE}/?directConnection=true`, { maxPoolSize: 20 });
}

export async function dropDatabases(client, names) {
  await Promise.all(names.map((name) => client.db(name).dropDatabase()));
}

export async function waitFor(predicate, { timeoutMs = 60_000, intervalMs = 25, label = 'condition' } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await sleep(intervalMs);
  }
  throw new Error(`timed out after ${timeoutMs}ms waiting for ${label}`);
}

export async function outboxCounts(db) {
  const counts = { PENDING: 0, PROCESSING: 0, SENT: 0, FAILED: 0 };
  const rows = await db.collection('outbox').aggregate([{ $group: { _id: '$status', n: { $sum: 1 } } }]).toArray();
  for (const { _id, n } of rows) counts[_id] = n;
  return counts;
}

export function sampleOrder(i) {
  return {
    customer_id: `customer-${i % 500}`,
    currency: 'USD',
    items: [
      { sku: `SKU-${i % 97}`, quantity: 1 + (i % 3), unit_price_cents: 1999 },
      { sku: 'SKU-SHIPPING', quantity: 1, unit_price_cents: 499 },
    ],
  };
}

export async function postOrder(baseUrl, body) {
  const res = await fetch(`${baseUrl}/orders`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (res.status !== 201) throw new Error(`POST /orders -> ${res.status}: ${await res.text()}`);
  return res.json();
}

export const step = (n, text) => console.log(`\n\x1b[1m${n}. ${text}\x1b[0m`);
export const check = (ok, text) => console.log(`  ${ok ? '\x1b[32m✔ PASS' : '\x1b[31m✘ FAIL'}\x1b[0m ${text}`);

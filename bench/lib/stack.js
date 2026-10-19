import { dropDatabases, mongoUri, sampleOrder, waitFor } from '../../scripts/lib/helpers.js';
import { startService } from '../../scripts/lib/processes.js';
import { runLoad } from './load.js';

export const DBS = Object.freeze({
  outbox: 'bench_outbox',
  billing: 'bench_billing',
  naive: 'bench_naive',
  naiveBilling: 'bench_naive_billing',
});

export const API_PORT = 3300;
export const RELAY_PORTS = [4300, 4301];
export const API_URL = `http://localhost:${API_PORT}`;

// Per-event logging is turned off so the benchmark measures the system, not stdout.
const QUIET = { LOG_LEVEL: 'warn' };

export const startApi = () =>
  startService('api', 'src/entrypoints/api.js', { ...QUIET, MONGO_URI: mongoUri(DBS.outbox), PORT: String(API_PORT) });

export const startRelay = (port = RELAY_PORTS[0], env = {}) =>
  startService(`relay-${port}`, 'src/entrypoints/relay.js', {
    ...QUIET,
    MONGO_URI: mongoUri(DBS.outbox),
    SOCKET_PORT: String(port),
    ...env,
  });

export const startConsumer = (relayPorts = [RELAY_PORTS[0]], db = DBS.billing) =>
  startService('consumer', 'src/entrypoints/consumer.js', {
    ...QUIET,
    MONGO_URI: mongoUri(db),
    RELAY_URLS: relayPorts.map((port) => `http://localhost:${port}`).join(','),
  });

export const resetDbs = (admin, names = [DBS.outbox, DBS.billing]) => dropDatabases(admin, names);

export const loadOrders = (opts) => runLoad({ baseUrl: API_URL, makeBody: sampleOrder, ...opts });

/**
 * Waits until `expected` events are SENT in the outbox and processed by the
 * consumer. The poll has to stay cheap, or it competes with the system it is
 * timing: estimatedDocumentCount reads collection metadata instead of
 * scanning, and the SENT count is answered from the status-prefixed index.
 * The exact count is confirmed once at the end.
 */
export async function waitForDelivery(admin, expected, { timeoutMs = 180_000 } = {}) {
  const outbox = admin.db(DBS.outbox).collection('outbox');
  const processed = admin.db(DBS.billing).collection('processed_events');
  await waitFor(
    async () => (await processed.estimatedDocumentCount()) >= expected && (await outbox.countDocuments({ status: 'SENT' })) >= expected,
    { timeoutMs, intervalMs: 25, label: `${expected} events delivered` },
  );
  const exact = await processed.countDocuments();
  if (exact < expected) throw new Error(`expected ${expected} processed events, found ${exact}`);
}

/** Commit -> consumer-receipt latency for every successfully created order. */
export async function e2eLatencies(admin, okResults) {
  const committedAt = new Map(okResults.map((r) => [r.eventId, r.committedAtMs]));
  const docs = await admin
    .db(DBS.billing)
    .collection('processed_events')
    .find({ _id: { $in: [...committedAt.keys()] } }, { projection: { received_at_ms: 1 } })
    .toArray();
  return docs.map((doc) => doc.received_at_ms - committedAt.get(doc._id));
}

export async function duplicatesSkipped(admin, db = DBS.billing) {
  return (await admin.db(db).collection('consumer_stats').findOne({ _id: 'billing' }))?.duplicates_skipped ?? 0;
}

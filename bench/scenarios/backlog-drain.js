import { performance } from 'node:perf_hooks';
import { setTimeout as sleep } from 'node:timers/promises';
import { ensureOutboxIndexes } from '../../src/db/indexes.js';
import { EventTypes } from '../../src/outbox/event-types.js';
import { buildOutboxEvent } from '../../src/outbox/outbox-event.js';
import { stopAll } from '../../scripts/lib/processes.js';
import { DBS, RELAY_PORTS, duplicatesSkipped, resetDbs, startConsumer, startRelay, waitForDelivery } from '../lib/stack.js';

const SEED_CHUNK = 5_000;

async function seedBacklog(outboxDb, count, dueAt) {
  const outbox = outboxDb.collection('outbox');
  const base = Date.now();
  for (let offset = 0; offset < count; offset += SEED_CHUNK) {
    const rows = [];
    for (let i = offset; i < Math.min(count, offset + SEED_CHUNK); i += 1) {
      const row = buildOutboxEvent({
        eventType: EventTypes.ORDER_CREATED,
        aggregateType: 'order',
        aggregateId: `seed-${i}`,
        payload: { order_id: `seed-${i}`, customer_id: `customer-${i % 500}`, total_cents: 4497, currency: 'USD', item_count: 2 },
        now: new Date(base + i),
      });
      row.next_attempt_at = dueAt;
      rows.push(row);
    }
    await outbox.insertMany(rows, { ordered: false });
  }
}

/**
 * How fast the relay clears a backlog of PENDING rows, and how that scales
 * with backlog size and with a second relay instance competing for rows.
 *
 * Rows are seeded with next_attempt_at slightly in the future so every
 * relay and the consumer are connected before the first row becomes due;
 * the clock starts at that instant.
 */
export async function run({ admin, scale, log }) {
  const rows = [];
  for (const { events, relays } of scale.backlogTiers) {
    await resetDbs(admin);
    const outboxDb = admin.db(DBS.outbox);
    await ensureOutboxIndexes(outboxDb);
    const services = [];
    try {
      const ports = RELAY_PORTS.slice(0, relays);
      for (const port of ports) services.push(await startRelay(port));
      services.push(await startConsumer(ports));

      const dueAt = new Date(Date.now() + 1_500 + events / 15);
      log(`seeding ${events} PENDING events, ${relays} relay(s)`);
      await seedBacklog(outboxDb, events, dueAt);
      const slackMs = dueAt.getTime() - Date.now();
      if (slackMs < 0) throw new Error('seeding overran the start time; increase the seed head start');
      await sleep(slackMs);

      const started = performance.now();
      await waitForDelivery(admin, events);
      const drainMs = performance.now() - started;

      const [{ claims } = { claims: 0 }] = await outboxDb
        .collection('outbox')
        .aggregate([{ $group: { _id: null, claims: { $sum: '$claims' } } }])
        .toArray();

      // Rows claimed more than once were either retried after a failed
      // delivery (attempts > 0) or genuinely claimed by two relays at once.
      const retried = await outboxDb.collection('outbox').countDocuments({ attempts: { $gt: 0 } });
      const sampleError = (await outboxDb.collection('outbox').findOne({ attempts: { $gt: 0 } }))?.last_error ?? null;

      rows.push({
        events,
        relays,
        drainMs,
        eventsPerSec: events / (drainMs / 1000),
        totalClaims: claims,
        retriedRows: retried,
        doubleClaims: claims - events - retried,
        duplicates: await duplicatesSkipped(admin),
        sampleError,
      });
    } finally {
      await stopAll(services);
    }
  }
  return rows;
}

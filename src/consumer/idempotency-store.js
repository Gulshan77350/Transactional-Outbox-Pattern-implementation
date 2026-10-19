import { performance } from 'node:perf_hooks';

const TXN_OPTIONS = {
  readConcern: { level: 'snapshot' },
  writeConcern: { w: 'majority' },
  readPreference: 'primary',
};

const isDuplicateKey = (err) => err?.code === 11000;

/**
 * Exactly-once *processing* on top of at-least-once *delivery*.
 *
 * The processed-event marker (unique _id = event id) and the handler's side
 * effects commit in the same transaction. A redelivered event fails the
 * marker insert with a duplicate key, so the side effect is rolled back and
 * never applied twice. Two concurrent deliveries of the same event collide
 * as a write conflict; withTransaction retries the loser, which then sees
 * the duplicate key.
 */
export function createIdempotencyStore({ client, db, consumerName }) {
  const processed = db.collection('processed_events');
  const stats = db.collection('consumer_stats');

  async function processOnce(envelope, handler, { receivedAtMs = performance.timeOrigin + performance.now() } = {}) {
    const session = client.startSession();
    try {
      await session.withTransaction(async () => {
        await processed.insertOne(
          {
            _id: envelope.id,
            type: envelope.type,
            consumer: consumerName,
            received_at_ms: receivedAtMs,
            processed_at: new Date(),
          },
          { session },
        );
        await handler(session);
      }, TXN_OPTIONS);
      return { duplicate: false };
    } catch (err) {
      if (!isDuplicateKey(err)) throw err;
      await stats.updateOne({ _id: consumerName }, { $inc: { duplicates_skipped: 1 } }, { upsert: true });
      return { duplicate: true };
    } finally {
      await session.endSession();
    }
  }

  async function hasProcessed(eventId) {
    return (await processed.countDocuments({ _id: eventId }, { limit: 1 })) === 1;
  }

  async function duplicatesSkipped() {
    return (await stats.findOne({ _id: consumerName }))?.duplicates_skipped ?? 0;
  }

  return { processOnce, hasProcessed, duplicatesSkipped };
}

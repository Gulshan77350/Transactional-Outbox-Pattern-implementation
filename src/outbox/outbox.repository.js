import { OutboxStatus } from './event-types.js';

/**
 * Relay bookkeeping writes (claim, mark-sent, mark-failed, lease release)
 * default to w:1. Losing one of them in a failover only ever means an event
 * is delivered again, which idempotent consumers already absorb, so they do
 * not need to wait for majority durability. The producer's order + outbox
 * transaction is the write that must not be lost, and it keeps w:majority.
 */
export function createOutboxRepository(db, { writeConcern = { w: 1 } } = {}) {
  const outbox = db.collection('outbox', { writeConcern });

  /**
   * Atomically moves the oldest due PENDING row to PROCESSING and stamps a
   * lease. findOneAndUpdate is a single-document atomic operation, so two
   * relays racing for the same row can never both win it.
   */
  async function claimNext({ workerId, leaseMs, now = new Date() }) {
    return outbox.findOneAndUpdate(
      { status: OutboxStatus.PENDING, next_attempt_at: { $lte: now } },
      {
        $set: {
          status: OutboxStatus.PROCESSING,
          locked_by: workerId,
          locked_until: new Date(now.getTime() + leaseMs),
        },
        $inc: { claims: 1 },
      },
      { sort: { created_at: 1 }, returnDocument: 'after' },
    );
  }

  async function claimBatch({ workerId, leaseMs, batchSize, now = new Date() }) {
    const batch = [];
    while (batch.length < batchSize) {
      const row = await claimNext({ workerId, leaseMs, now });
      if (!row) break;
      batch.push(row);
    }
    return batch;
  }

  /** Only the current lease holder may complete a row; a stale worker's write is a no-op. */
  async function markSent(ids, workerId, now = new Date()) {
    if (ids.length === 0) return 0;
    const result = await outbox.updateMany(
      { _id: { $in: ids }, status: OutboxStatus.PROCESSING, locked_by: workerId },
      {
        $set: { status: OutboxStatus.SENT, sent_at: now, locked_by: null, locked_until: null },
      },
    );
    return result.modifiedCount;
  }

  /**
   * Records a failed delivery. Below maxAttempts the row goes back to PENDING
   * with a backoff delay; at maxAttempts it is parked as FAILED (dead letter).
   */
  async function markFailed(row, workerId, error, { maxAttempts, backoffMs, now = new Date() }) {
    const attempts = row.attempts + 1;
    const exhausted = attempts >= maxAttempts;
    const result = await outbox.updateOne(
      { _id: row._id, status: OutboxStatus.PROCESSING, locked_by: workerId },
      {
        $set: {
          status: exhausted ? OutboxStatus.FAILED : OutboxStatus.PENDING,
          attempts,
          next_attempt_at: exhausted ? null : new Date(now.getTime() + backoffMs),
          last_error: String(error?.message ?? error).slice(0, 500),
          locked_by: null,
          locked_until: null,
        },
      },
    );
    return { attempts, exhausted, updated: result.modifiedCount === 1 };
  }

  /**
   * Returns rows whose lease expired (the worker holding them died or hung)
   * to PENDING. Whether the event reached consumers before the crash is
   * unknown, so it is redelivered; consumers dedupe by event id.
   */
  async function releaseExpiredLeases(now = new Date()) {
    const result = await outbox.updateMany(
      { status: OutboxStatus.PROCESSING, locked_until: { $lt: now } },
      { $set: { status: OutboxStatus.PENDING, locked_by: null, locked_until: null, next_attempt_at: now } },
    );
    return result.modifiedCount;
  }

  /**
   * Operator action for dead letters: puts FAILED rows back to PENDING with a
   * fresh attempt budget, e.g. after the consumer bug that failed them is
   * fixed. last_error is kept for the audit trail.
   */
  async function requeueFailed({ ids, now = new Date() } = {}) {
    const filter = { status: OutboxStatus.FAILED, ...(ids ? { _id: { $in: ids } } : {}) };
    const result = await outbox.updateMany(filter, {
      $set: { status: OutboxStatus.PENDING, attempts: 0, next_attempt_at: now, locked_by: null, locked_until: null },
    });
    return result.modifiedCount;
  }

  async function countByStatus() {
    const counts = Object.fromEntries(Object.values(OutboxStatus).map((status) => [status, 0]));
    const rows = await outbox.aggregate([{ $group: { _id: '$status', n: { $sum: 1 } } }]).toArray();
    for (const { _id, n } of rows) counts[_id] = n;
    return counts;
  }

  return { claimNext, claimBatch, markSent, markFailed, releaseExpiredLeases, requeueFailed, countByStatus };
}

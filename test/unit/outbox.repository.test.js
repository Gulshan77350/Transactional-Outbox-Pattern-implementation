import { afterAll, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import { OutboxStatus } from '../../src/outbox/event-types.js';
import { buildOutboxEvent } from '../../src/outbox/outbox-event.js';
import { createOutboxRepository } from '../../src/outbox/outbox.repository.js';
import { openTestDb } from '../helpers/db.js';

const LEASE = 10_000;

describe('outbox repository', () => {
  let ctx;
  let repo;
  let outbox;

  const seed = async (count, { startMs = Date.now() - 60_000, ...overrides } = {}) => {
    const rows = Array.from({ length: count }, (_, i) => ({
      ...buildOutboxEvent({ eventType: 'OrderCreated', aggregateType: 'order', aggregateId: `o-${i}`, payload: { i }, now: new Date(startMs + i) }),
      ...overrides,
    }));
    await outbox.insertMany(rows);
    return rows;
  };

  beforeAll(async () => {
    ctx = await openTestDb('repo');
    repo = createOutboxRepository(ctx.db);
    outbox = ctx.db.collection('outbox');
  });
  beforeEach(() => ctx.reset());
  afterAll(() => ctx.close());

  describe('claiming', () => {
    it('claims the oldest due PENDING row and stamps a lease', async () => {
      const rows = await seed(3);
      const now = new Date();
      const claimed = await repo.claimNext({ workerId: 'w1', leaseMs: LEASE, now });

      expect(claimed._id).toEqual(rows[0]._id);
      expect(claimed).toMatchObject({ status: OutboxStatus.PROCESSING, locked_by: 'w1', claims: 1 });
      expect(claimed.locked_until.getTime()).toBe(now.getTime() + LEASE);
    });

    it('skips rows that are not due yet and rows that are not PENDING', async () => {
      await seed(1, { next_attempt_at: new Date(Date.now() + 60_000) });
      await seed(1, { status: OutboxStatus.SENT });
      await seed(1, { status: OutboxStatus.FAILED });
      expect(await repo.claimNext({ workerId: 'w1', leaseMs: LEASE })).toBeNull();
    });

    it('claims a batch in created_at order, bounded by batch size', async () => {
      const rows = await seed(5);
      const batch = await repo.claimBatch({ workerId: 'w1', leaseMs: LEASE, batchSize: 3 });
      expect(batch.map((r) => r._id)).toEqual(rows.slice(0, 3).map((r) => r._id));
      expect(await repo.claimBatch({ workerId: 'w1', leaseMs: LEASE, batchSize: 10 })).toHaveLength(2);
    });

    it('never hands the same row to two concurrent claimers', async () => {
      await seed(300);
      const workers = ['w1', 'w2', 'w3', 'w4'].map(async (workerId) => {
        const mine = [];
        for (;;) {
          const batch = await repo.claimBatch({ workerId, leaseMs: LEASE, batchSize: 7 });
          if (batch.length === 0) return mine;
          mine.push(...batch.map((r) => r._id.toHexString()));
        }
      });
      const claimedPerWorker = await Promise.all(workers);
      const all = claimedPerWorker.flat();

      expect(all).toHaveLength(300);
      expect(new Set(all).size).toBe(300);
      expect(await outbox.countDocuments({ claims: { $ne: 1 } })).toBe(0);
    });
  });

  describe('completing', () => {
    it('marks rows SENT only for the lease holder', async () => {
      await seed(2);
      const [a, b] = await repo.claimBatch({ workerId: 'w1', leaseMs: LEASE, batchSize: 2 });

      expect(await repo.markSent([a._id], 'someone-else')).toBe(0);
      expect(await repo.markSent([a._id, b._id], 'w1')).toBe(2);
      expect(await repo.markSent([], 'w1')).toBe(0);

      const sent = await outbox.findOne({ _id: a._id });
      expect(sent).toMatchObject({ status: OutboxStatus.SENT, locked_by: null, locked_until: null });
      expect(sent.sent_at).toBeInstanceOf(Date);
    });

    it('returns a failed row to PENDING with a backoff delay', async () => {
      await seed(1);
      const row = await repo.claimNext({ workerId: 'w1', leaseMs: LEASE });
      const now = new Date();
      const result = await repo.markFailed(row, 'w1', new Error('consumer timeout'), { maxAttempts: 3, backoffMs: 500, now });

      expect(result).toEqual({ attempts: 1, exhausted: false, updated: true });
      const stored = await outbox.findOne({ _id: row._id });
      expect(stored).toMatchObject({ status: OutboxStatus.PENDING, attempts: 1, last_error: 'consumer timeout', locked_by: null });
      expect(stored.next_attempt_at.getTime()).toBe(now.getTime() + 500);
    });

    it('parks a row as FAILED once attempts are exhausted', async () => {
      await seed(1, { attempts: 2 });
      const row = await repo.claimNext({ workerId: 'w1', leaseMs: LEASE });
      const result = await repo.markFailed(row, 'w1', 'still down', { maxAttempts: 3, backoffMs: 500 });

      expect(result).toMatchObject({ attempts: 3, exhausted: true });
      expect(await outbox.findOne({ _id: row._id })).toMatchObject({ status: OutboxStatus.FAILED, next_attempt_at: null, last_error: 'still down' });
    });

    it('ignores a failure report from a worker that no longer holds the lease', async () => {
      await seed(1);
      const row = await repo.claimNext({ workerId: 'w1', leaseMs: LEASE });
      const result = await repo.markFailed(row, 'w2', new Error('x'), { maxAttempts: 3, backoffMs: 1 });
      expect(result.updated).toBe(false);
      expect(await outbox.findOne({ _id: row._id })).toMatchObject({ status: OutboxStatus.PROCESSING, locked_by: 'w1' });
    });
  });

  describe('lease recovery', () => {
    it('releases only expired leases back to PENDING', async () => {
      await seed(2);
      const now = new Date();
      const [expired, live] = await repo.claimBatch({ workerId: 'dead', leaseMs: LEASE, batchSize: 2, now });
      await outbox.updateOne({ _id: expired._id }, { $set: { locked_until: new Date(now.getTime() - 1) } });

      expect(await repo.releaseExpiredLeases(now)).toBe(1);
      expect(await outbox.findOne({ _id: expired._id })).toMatchObject({ status: OutboxStatus.PENDING, locked_by: null });
      expect(await outbox.findOne({ _id: live._id })).toMatchObject({ status: OutboxStatus.PROCESSING, locked_by: 'dead' });

      const reclaimed = await repo.claimNext({ workerId: 'w2', leaseMs: LEASE, now });
      expect(reclaimed).toMatchObject({ _id: expired._id, locked_by: 'w2', claims: 2 });
    });
  });

  describe('dead-letter requeue', () => {
    it('returns FAILED rows to PENDING with a fresh attempt budget and keeps last_error', async () => {
      const [failedA, failedB] = await seed(2, { status: OutboxStatus.FAILED, attempts: 8, next_attempt_at: null, last_error: 'boom' });
      await seed(1, { status: OutboxStatus.SENT });

      const now = new Date();
      expect(await repo.requeueFailed({ now })).toBe(2);
      for (const { _id } of [failedA, failedB]) {
        expect(await outbox.findOne({ _id })).toMatchObject({ status: OutboxStatus.PENDING, attempts: 0, next_attempt_at: now, last_error: 'boom' });
      }
      expect(await outbox.countDocuments({ status: OutboxStatus.SENT })).toBe(1);
    });

    it('can requeue selected events only', async () => {
      const [a, b] = await seed(2, { status: OutboxStatus.FAILED });
      expect(await repo.requeueFailed({ ids: [a._id] })).toBe(1);
      expect(await outbox.findOne({ _id: b._id })).toMatchObject({ status: OutboxStatus.FAILED });
    });
  });

  it('counts rows by status', async () => {
    await seed(3);
    await seed(1, { status: OutboxStatus.SENT });
    await repo.claimNext({ workerId: 'w1', leaseMs: LEASE });
    expect(await repo.countByStatus()).toEqual({ PENDING: 2, PROCESSING: 1, SENT: 1, FAILED: 0 });
  });
});

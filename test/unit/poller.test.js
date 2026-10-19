import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { OutboxStatus } from '../../src/outbox/event-types.js';
import { buildOutboxEvent } from '../../src/outbox/outbox-event.js';
import { createOutboxRepository } from '../../src/outbox/outbox.repository.js';
import { OutboxRelay } from '../../src/relay/poller.js';
import { eventually, openTestDb, silentLogger } from '../helpers/db.js';

function fakeTransport() {
  const delivered = [];
  return {
    delivered,
    fail: null,
    async deliver(envelope) {
      if (this.fail) throw this.fail(envelope);
      delivered.push(envelope);
      return [{ ok: true }];
    },
  };
}

describe('OutboxRelay', () => {
  let ctx;
  let repo;
  let outbox;
  let transport;

  const seed = (count) =>
    outbox.insertMany(
      Array.from({ length: count }, (_, i) =>
        buildOutboxEvent({ eventType: 'OrderCreated', aggregateType: 'order', aggregateId: `o-${i}`, payload: { i }, now: new Date(Date.now() - 60_000 + i) }),
      ),
    );

  const relay = (options = {}) =>
    new OutboxRelay({ repo, transport, logger: silentLogger, workerId: 'test-relay', pollIntervalMs: 5, backoffBaseMs: 1, backoffMaxMs: 1, ...options });

  beforeAll(async () => {
    ctx = await openTestDb('poller');
    repo = createOutboxRepository(ctx.db);
    outbox = ctx.db.collection('outbox');
  });
  beforeEach(async () => {
    await ctx.reset();
    transport = fakeTransport();
  });
  afterAll(() => ctx.close());

  it('delivers a batch and marks it SENT', async () => {
    await seed(3);
    const r = relay();
    expect(await r.runOnce()).toBe(3);

    expect(transport.delivered.map((e) => e.payload.i)).toEqual([0, 1, 2]);
    expect(await outbox.countDocuments({ status: OutboxStatus.SENT })).toBe(3);
    expect(r.stats).toMatchObject({ claimed: 3, sent: 3, failed: 0 });
    expect(await r.runOnce()).toBe(0);
  });

  it('schedules a retry with backoff when delivery fails', async () => {
    await seed(1);
    transport.fail = () => new Error('consumer timed out');
    const r = relay({ backoffBaseMs: 60_000, backoffMaxMs: 60_000 });
    await r.runOnce();

    const row = await outbox.findOne();
    expect(row).toMatchObject({ status: OutboxStatus.PENDING, attempts: 1, last_error: 'consumer timed out' });
    expect(row.next_attempt_at.getTime()).toBeGreaterThan(Date.now() + 25_000);
    expect(r.stats).toMatchObject({ failed: 1, deadLettered: 0 });
  });

  it('dead-letters an event after maxAttempts failures', async () => {
    await seed(1);
    transport.fail = () => new Error('nope');
    const r = relay({ maxAttempts: 2 });
    await r.runOnce();
    await eventually(async () => (await r.runOnce()) === 1);

    expect(await outbox.findOne()).toMatchObject({ status: OutboxStatus.FAILED, attempts: 2 });
    expect(r.stats.deadLettered).toBe(1);
  });

  it('leaves a dropped row PROCESSING until its lease expires, then redelivers it', async () => {
    await seed(1);
    let drops = 1;
    const r = relay({ leaseMs: 100, leaseSweepIntervalMs: 0, hooks: { beforeDeliver: () => (drops-- > 0 ? 'drop' : undefined) } });

    await r.runOnce();
    expect(await outbox.findOne()).toMatchObject({ status: OutboxStatus.PROCESSING });
    expect(transport.delivered).toHaveLength(0);

    await eventually(async () => {
      await r.runOnce();
      return (await outbox.countDocuments({ status: OutboxStatus.SENT })) === 1;
    });
    expect(r.stats).toMatchObject({ dropped: 1, leasesReleased: 1, sent: 1 });
    expect((await outbox.findOne()).claims).toBe(2);
  });

  it('calls afterDeliver for every successful emit', async () => {
    await seed(2);
    const afterDeliver = vi.fn();
    await relay({ hooks: { afterDeliver } }).runOnce();
    expect(afterDeliver).toHaveBeenCalledTimes(2);
  });

  it('drains a backlog with pipelined batches and stops cleanly', async () => {
    await seed(250);
    const r = relay({ batchSize: 40, maxInFlightBatches: 3 });
    r.start();
    r.start(); // idempotent
    expect(r.running).toBe(true);

    await eventually(async () => (await outbox.countDocuments({ status: OutboxStatus.SENT })) === 250);
    await r.stop();
    await r.stop(); // idempotent

    expect(r.running).toBe(false);
    expect(new Set(transport.delivered.map((e) => e.id)).size).toBe(250);
  });

  it('keeps running when an iteration or a batch fails', async () => {
    await seed(5);
    const error = vi.fn();
    const claimBatch = vi.spyOn(repo, 'claimBatch').mockRejectedValueOnce(new Error('mongo blip'));
    const markSent = vi.spyOn(repo, 'markSent').mockRejectedValueOnce(new Error('write failed'));
    const r = relay({ logger: { ...silentLogger, error }, leaseMs: 50, leaseSweepIntervalMs: 0 });

    r.start();
    await eventually(async () => (await outbox.countDocuments({ status: OutboxStatus.SENT })) === 5);
    await r.stop();

    expect(error).toHaveBeenCalledWith(expect.anything(), 'relay iteration failed');
    expect(error).toHaveBeenCalledWith(expect.anything(), 'batch delivery failed');
    claimBatch.mockRestore();
    markSent.mockRestore();
  });
});

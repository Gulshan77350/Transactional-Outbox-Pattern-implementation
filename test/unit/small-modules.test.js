import { ObjectId } from 'mongodb';
import { describe, expect, it, vi } from 'vitest';
import { loadConfig } from '../../src/config.js';
import { computeBackoffMs } from '../../src/outbox/backoff.js';
import { OutboxStatus } from '../../src/outbox/event-types.js';
import { buildOutboxEvent, toEnvelope } from '../../src/outbox/outbox-event.js';
import { createFaultHooks } from '../../src/relay/faults.js';
import { createSemaphore } from '../../src/shared/semaphore.js';
import { createLogger } from '../../src/shared/logger.js';

describe('computeBackoffMs', () => {
  const opts = { baseMs: 100, maxMs: 1_000 };

  it('doubles per attempt with jitter in the upper half of the window', () => {
    expect(computeBackoffMs(1, { ...opts, random: () => 0 })).toBe(50);
    expect(computeBackoffMs(1, { ...opts, random: () => 1 })).toBe(100);
    expect(computeBackoffMs(3, { ...opts, random: () => 0 })).toBe(200);
    expect(computeBackoffMs(3, { ...opts, random: () => 1 })).toBe(400);
  });

  it('caps at maxMs', () => {
    expect(computeBackoffMs(20, { ...opts, random: () => 1 })).toBe(1_000);
  });
});

describe('outbox event', () => {
  it('builds a PENDING row that is due immediately', () => {
    const now = new Date('2026-01-01T00:00:00Z');
    const aggregateId = new ObjectId();
    const row = buildOutboxEvent({ eventType: 'OrderCreated', aggregateType: 'order', aggregateId, payload: { a: 1 }, now });
    expect(row).toMatchObject({
      event_type: 'OrderCreated',
      aggregate_id: aggregateId,
      status: OutboxStatus.PENDING,
      attempts: 0,
      claims: 0,
      next_attempt_at: now,
      created_at: now,
      sent_at: null,
    });
    expect(row._id).toBeInstanceOf(ObjectId);
  });

  it('maps a row to the wire envelope using the row id as the idempotency key', () => {
    const row = buildOutboxEvent({ eventType: 'OrderCreated', aggregateType: 'order', aggregateId: new ObjectId(), payload: { a: 1 } });
    row.attempts = 2;
    expect(toEnvelope(row)).toEqual({
      id: row._id.toHexString(),
      type: 'OrderCreated',
      aggregate_type: 'order',
      aggregate_id: row.aggregate_id.toHexString(),
      payload: { a: 1 },
      created_at: row.created_at.toISOString(),
      delivery_attempt: 3,
    });
  });
});

describe('loadConfig', () => {
  it('applies defaults', () => {
    const config = loadConfig({});
    expect(config.mongoUri).toContain('directConnection=true');
    expect(config.relay).toMatchObject({ pollIntervalMs: 50, batchSize: 100, maxInFlightBatches: 2, leaseMs: 10_000, maxAttempts: 8 });
    expect(config.consumer).toEqual({ concurrency: 8, relayUrls: ['http://localhost:4000'] });
    expect(config.faults).toEqual({ crashAfterEmits: 0, dropRate: 0 });
  });

  it('parses overrides and relay URL lists', () => {
    const config = loadConfig({ POLL_INTERVAL_MS: '10', RELAY_URLS: 'http://a:1, http://b:2,', FAULT_DROP_RATE: '0.25' });
    expect(config.relay.pollIntervalMs).toBe(10);
    expect(config.consumer.relayUrls).toEqual(['http://a:1', 'http://b:2']);
    expect(config.faults.dropRate).toBe(0.25);
  });

  it('rejects malformed numbers', () => {
    expect(() => loadConfig({ BATCH_SIZE: 'lots' })).toThrow(/BATCH_SIZE must be an integer/);
    expect(() => loadConfig({ FAULT_DROP_RATE: 'x' })).toThrow(/FAULT_DROP_RATE must be a number/);
  });
});

describe('createFaultHooks', () => {
  it('installs no hooks by default', () => {
    expect(createFaultHooks({})).toEqual({});
  });

  it('drops rows according to the drop rate', () => {
    const hooks = createFaultHooks({ dropRate: 0.5 }, { random: vi.fn().mockReturnValueOnce(0.1).mockReturnValueOnce(0.9) });
    expect(hooks.beforeDeliver()).toBe('drop');
    expect(hooks.beforeDeliver()).toBeUndefined();
  });

  it('exits on the Nth successful emit', () => {
    const exit = vi.fn();
    const hooks = createFaultHooks({ crashAfterEmits: 2 }, { exit, logger: { fatal: vi.fn() } });
    hooks.afterDeliver({ _id: 1 });
    expect(exit).not.toHaveBeenCalled();
    hooks.afterDeliver({ _id: 2 });
    expect(exit).toHaveBeenCalledWith(137);
  });
});

describe('createSemaphore', () => {
  it('never runs more than `limit` tasks at once and preserves results', async () => {
    const slots = createSemaphore(2);
    let running = 0;
    let peak = 0;
    const task = (value) =>
      slots.run(async () => {
        running += 1;
        peak = Math.max(peak, running);
        await new Promise((resolve) => setTimeout(resolve, 5));
        running -= 1;
        return value;
      });
    const results = await Promise.all([1, 2, 3, 4, 5].map(task));
    expect(results).toEqual([1, 2, 3, 4, 5]);
    expect(peak).toBe(2);
    expect(slots.active).toBe(0);
    expect(slots.queued).toBe(0);
  });

  it('releases the slot when a task throws', async () => {
    const slots = createSemaphore(1);
    await expect(slots.run(async () => Promise.reject(new Error('boom')))).rejects.toThrow('boom');
    await expect(slots.run(async () => 'ok')).resolves.toBe('ok');
  });
});

describe('createLogger', () => {
  it('creates a named pino logger at the requested level', () => {
    const logger = createLogger('test', 'warn');
    expect(logger.level).toBe('warn');
  });
});

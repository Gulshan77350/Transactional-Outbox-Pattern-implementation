import { hostname } from 'node:os';
import { computeBackoffMs } from '../outbox/backoff.js';
import { toEnvelope } from '../outbox/outbox-event.js';

/**
 * Relay loop: claim a batch of due outbox rows, push them to consumers,
 * then record the outcome. The loop drains back-to-back while full batches
 * keep coming and only sleeps for pollIntervalMs once the outbox runs dry.
 *
 * Claiming and delivering are pipelined: while one batch waits for consumer
 * acks the next one is already being claimed, up to maxInFlightBatches.
 *
 * Optional hooks exist purely for fault injection in demos and benchmarks:
 *   beforeDeliver(row) -> 'drop' simulates dying before the emit
 *   afterDeliver(row)  can kill the process between emit and mark-as-sent
 */
export class OutboxRelay {
  #running = false;
  #loop = null;
  #wake = null;
  #lastSweep = 0;

  constructor({
    repo,
    transport,
    logger,
    workerId = `relay-${hostname()}-${process.pid}`,
    pollIntervalMs = 50,
    batchSize = 100,
    maxInFlightBatches = 2,
    leaseMs = 10_000,
    leaseSweepIntervalMs = 1_000,
    maxAttempts = 8,
    backoffBaseMs = 200,
    backoffMaxMs = 30_000,
    hooks = {},
  }) {
    Object.assign(this, { repo, transport, logger, workerId, hooks });
    this.options = {
      pollIntervalMs,
      batchSize,
      maxInFlightBatches,
      leaseMs,
      leaseSweepIntervalMs,
      maxAttempts,
      backoffBaseMs,
      backoffMaxMs,
    };
    this.stats = { claimed: 0, sent: 0, failed: 0, deadLettered: 0, leasesReleased: 0, dropped: 0 };
  }

  get running() {
    return this.#running;
  }

  start() {
    if (this.#running) return;
    this.#running = true;
    this.#loop = this.#run();
    this.logger?.info({ workerId: this.workerId, ...this.options }, 'relay started');
  }

  async stop() {
    if (!this.#running) return;
    this.#running = false;
    this.#wake?.();
    await this.#loop;
    this.logger?.info({ workerId: this.workerId, stats: this.stats }, 'relay stopped');
  }

  /** One full claim -> deliver -> record cycle. Returns how many rows were claimed. */
  async runOnce(now = new Date()) {
    const batch = await this.#claim(now);
    if (batch.length > 0) await this.#deliverBatch(batch);
    return batch.length;
  }

  async #claim(now) {
    await this.#sweepExpiredLeases(now);
    const { batchSize, leaseMs } = this.options;
    const batch = await this.repo.claimBatch({ workerId: this.workerId, leaseMs, batchSize, now });
    this.stats.claimed += batch.length;
    return batch;
  }

  async #deliverBatch(batch) {
    const sentIds = [];
    const failures = [];
    await Promise.all(
      batch.map(async (row) => {
        if (this.hooks.beforeDeliver?.(row) === 'drop') {
          // Behaves like a crash before the emit: the row stays PROCESSING
          // until its lease expires and another sweep hands it out again.
          this.stats.dropped += 1;
          return;
        }
        try {
          await this.transport.deliver(toEnvelope(row));
          this.hooks.afterDeliver?.(row);
          sentIds.push(row._id);
        } catch (err) {
          failures.push({ row, err });
        }
      }),
    );

    this.stats.sent += await this.repo.markSent(sentIds, this.workerId);
    for (const { row, err } of failures) await this.#recordFailure(row, err);

    this.logger?.debug({ claimed: batch.length, sent: sentIds.length, failed: failures.length }, 'batch processed');
  }

  async #recordFailure(row, err) {
    const { maxAttempts, backoffBaseMs, backoffMaxMs } = this.options;
    const backoffMs = computeBackoffMs(row.attempts + 1, { baseMs: backoffBaseMs, maxMs: backoffMaxMs });
    const { attempts, exhausted } = await this.repo.markFailed(row, this.workerId, err, { maxAttempts, backoffMs });
    this.stats.failed += 1;
    if (exhausted) {
      this.stats.deadLettered += 1;
      this.logger?.error({ eventId: row._id, attempts, err: err.message }, 'event moved to FAILED');
    } else {
      this.logger?.warn({ eventId: row._id, attempts, retryInMs: backoffMs, err: err.message }, 'delivery failed, will retry');
    }
  }

  async #sweepExpiredLeases(now) {
    if (now.getTime() - this.#lastSweep < this.options.leaseSweepIntervalMs) return;
    this.#lastSweep = now.getTime();
    const released = await this.repo.releaseExpiredLeases(now);
    if (released > 0) {
      this.stats.leasesReleased += released;
      this.logger?.warn({ released }, 'released expired leases for redelivery');
    }
  }

  async #run() {
    const inFlight = new Set();
    while (this.#running) {
      let claimed = 0;
      try {
        const batch = await this.#claim(new Date());
        claimed = batch.length;
        if (claimed > 0) {
          const delivery = this.#deliverBatch(batch)
            .catch((err) => this.logger?.error({ err }, 'batch delivery failed'))
            .finally(() => inFlight.delete(delivery));
          inFlight.add(delivery);
        }
      } catch (err) {
        this.logger?.error({ err }, 'relay iteration failed');
      }
      if (inFlight.size >= this.options.maxInFlightBatches) await Promise.race(inFlight);
      if (claimed < this.options.batchSize) await this.#sleep(this.options.pollIntervalMs);
    }
    await Promise.all(inFlight);
  }

  #sleep(ms) {
    return new Promise((resolve) => {
      const timer = setTimeout(resolve, ms);
      this.#wake = () => {
        clearTimeout(timer);
        resolve();
      };
    });
  }
}

# Benchmark results

Generated 2026-10-19T16:16:17.166Z by `npm run bench`.

## Environment

|  |  |
| --- | --- |
| CPU | 13th Gen Intel(R) Core(TM) i7-13650HX (20 logical cores) |
| Memory | 16 GB |
| OS | Windows_NT 10.0.26200 (x64) |
| Node.js | v24.18.0 |
| MongoDB | 7.0.45 single-node replica set in Docker Desktop |
| Topology | api, relay and consumer as separate Node processes on the host; one shared MongoDB |

## Headline numbers

- Write path: **418 orders/sec** sustained at saturation (10,000 orders, 100 concurrent connections, 0 errors, relay + consumer sharing the database)
- Order + outbox transaction latency at 200 orders/sec: **p50 7.8 ms / p95 10.8 ms / p99 13 ms**
- Outbox lag (commit -> consumer, 200/s steady, 10 ms poll): **p50 34.6 ms / p95 102.1 ms / p99 151.2 ms**
- Outbox lag (commit -> consumer, 200/s steady, 50 ms poll): **p50 132.8 ms / p95 258.8 ms / p99 295.3 ms**
- Backlog drain: **502 events/sec** (50,000 events, 2 relay(s)), 0 double-claims across all runs
- Crash recovery: **9,555 stranded events delivered in 31.44 s** after relay restart, 0 lost, 100 redeliveries deduplicated
- At a 1% injected crash rate: naive dual-write lost **107 of 10,000 events (1.07%)**, the outbox lost **0**

## 1. Write throughput and transaction latency (saturation)

10,000 `POST /orders` over 100 keep-alive connections (closed loop), with the relay and consumer running against the same MongoDB. Latencies here are dominated by queueing at saturation; see section 2 for latency at a steady rate.

| Throughput | Duration | Errors | Relay caught up after load |
| --- | --- | --- | --- |
| 418 orders/sec | 23942.9 ms | 0 | 7622.9 ms |

| Metric | p50 | p95 | p99 | max | samples |
| --- | --- | --- | --- | --- | --- |
| Order + outbox transaction (server side) | 191.5 ms | 226.4 ms | 278.1 ms | 458.2 ms | 10,000 |
| HTTP request (client side) | 256.3 ms | 296.7 ms | 346.7 ms | 524.1 ms | 10,000 |
| Commit -> consumer under saturation | 2305.5 ms | 6997.7 ms | 7387 ms | 7598.7 ms | 10,000 |

## 2. End-to-end outbox lag (steady load)

Time from the order transaction committing (API process) to the consumer receiving the event (consumer process), at a fixed arrival rate below saturation.

| Poll interval | Rate | Events | p50 | p95 | p99 | max |
| --- | --- | --- | --- | --- | --- | --- |
| 10 ms | 200/s | 3,000 | 34.6 ms | 102.1 ms | 151.2 ms | 204.1 ms |
| 50 ms | 200/s | 3,000 | 132.8 ms | 258.8 ms | 295.3 ms | 323.7 ms |
| 100 ms | 200/s | 3,000 | 214.6 ms | 279.5 ms | 305.8 ms | 336.2 ms |

Write-path latency during the same steady-load runs:

| Metric | p50 | p95 | p99 | max | samples |
| --- | --- | --- | --- | --- | --- |
| Order + outbox transaction (poll 10 ms run) | 9 ms | 32.4 ms | 163.4 ms | 293.6 ms | 3,000 |
| HTTP request (poll 10 ms run) | 10.8 ms | 44.2 ms | 237.7 ms | 367.3 ms | 3,000 |
| Order + outbox transaction (poll 50 ms run) | 7.8 ms | 10.8 ms | 13 ms | 19 ms | 3,000 |
| HTTP request (poll 50 ms run) | 9.5 ms | 12.9 ms | 15.5 ms | 22.6 ms | 3,000 |
| Order + outbox transaction (poll 100 ms run) | 7.2 ms | 11.3 ms | 22 ms | 56.3 ms | 3,000 |
| HTTP request (poll 100 ms run) | 9 ms | 13.6 ms | 28.7 ms | 71.5 ms | 3,000 |

## 3. Backlog drain

N events already PENDING; time until every one is SENT and processed by the consumer.

| Backlog | Relays | Drain time | Throughput | Total claims | Double claims | Duplicates at consumer |
| --- | --- | --- | --- | --- | --- | --- |
| 1,000 | 1 | 2660.1 ms | 376 events/sec | 1,000 | 0 | 0 |
| 10,000 | 1 | 23931 ms | 418 events/sec | 10,000 | 0 | 0 |
| 50,000 | 1 | 131086.9 ms | 381 events/sec | 50,000 | 0 | 0 |
| 50,000 | 2 | 99596.4 ms | 502 events/sec | 50,000 | 0 | 0 |

## 4. Crash recovery

The relay is killed after 500 emits (after consumer ack, before mark-as-sent) while 10,000 orders are written. A new relay is then started.

| Stranded PENDING | Stranded PROCESSING | Time to deliver all after restart | Recovery rate | Events lost | Duplicates deduplicated |
| --- | --- | --- | --- | --- | --- |
| 9,414 | 141 | 31440.6 ms | 304 events/sec | 0 | 100 |

## 5. Baseline: naive dual-write vs transactional outbox

Same load and the same injected fault: for 1% of events the process "dies" between the database write and the publish.

| Design | Orders | Events delivered | Events lost | Loss rate | Throughput | Request p50 | Request p99 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| naive dual-write | 10,000 | 9,893 | 107 | 1.07% | 892 orders/sec | 70.8 ms | 93.8 ms |
| transactional outbox | 10,000 | 10,000 | 0 | 0% | 498 orders/sec | 110 ms | 154.6 ms |

The outbox costs write latency (a multi-document transaction instead of a single insert) in exchange for zero lost events.

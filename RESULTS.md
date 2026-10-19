# Benchmark results

Generated 2026-10-19T15:57:06.525Z by `npm run bench -- --quick`.

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

- Write path: **992 orders/sec** sustained (1,000 orders, 50 concurrent connections, 0 errors)
- Order + outbox transaction latency: **p50 33 ms / p95 61.3 ms / p99 89.9 ms**
- Outbox lag (commit -> consumer, 100/s steady, 50 ms poll): **p50 283.8 ms / p95 488 ms / p99 521.1 ms**
- Backlog drain: **178 events/sec** (2,000 events, 2 relay(s)), 0 double-claims across all runs
- Crash recovery: **927 stranded events delivered in 8.2 s** after relay restart, 0 lost, 100 redeliveries deduplicated
- At a 1% injected crash rate: naive dual-write lost **14 of 1,000 events (1.4%)**, the outbox lost **0**

## 1. Write throughput and transaction latency (saturation)

1,000 `POST /orders` over 50 keep-alive connections (closed loop), with the relay and consumer running.

| Throughput | Duration | Errors | Relay caught up after load |
| --- | --- | --- | --- |
| 992 orders/sec | 1008.5 ms | 0 | 6749 ms |

| Metric | p50 | p95 | p99 | max | samples |
| --- | --- | --- | --- | --- | --- |
| Order + outbox transaction (server side) | 33 ms | 61.3 ms | 89.9 ms | 97.9 ms | 1,000 |
| HTTP request (client side) | 46 ms | 78.1 ms | 104.8 ms | 121 ms | 1,000 |
| Commit -> consumer under saturation | 3589.4 ms | 6214.8 ms | 6650.2 ms | 6655.7 ms | 1,000 |

## 2. End-to-end outbox lag (steady load)

Time from the order transaction committing (API process) to the consumer receiving the event (consumer process), at a fixed arrival rate below saturation.

| Poll interval | Rate | Events | p50 | p95 | p99 | max |
| --- | --- | --- | --- | --- | --- | --- |
| 50 ms | 100/s | 500 | 283.8 ms | 488 ms | 521.1 ms | 534.5 ms |

## 3. Backlog drain

N events already PENDING; time until every one is SENT and processed by the consumer.

| Backlog | Relays | Drain time | Throughput | Total claims | Double claims | Duplicates at consumer |
| --- | --- | --- | --- | --- | --- | --- |
| 1,000 | 1 | 8857.8 ms | 113 events/sec | 1,000 | 0 | 0 |
| 2,000 | 2 | 11240.8 ms | 178 events/sec | 2,000 | 0 | 0 |

## 4. Crash recovery

The relay is killed after 100 emits (after consumer ack, before mark-as-sent) while 1,000 orders are written. A new relay is then started.

| Stranded PENDING | Stranded PROCESSING | Time to deliver all after restart | Recovery rate | Events lost | Duplicates deduplicated |
| --- | --- | --- | --- | --- | --- |
| 827 | 100 | 8197.7 ms | 113 events/sec | 0 | 100 |

## 5. Baseline: naive dual-write vs transactional outbox

Same load and the same injected fault: for 1% of events the process "dies" between the database write and the publish.

| Design | Orders | Events delivered | Events lost | Loss rate | Throughput | Request p50 | Request p99 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| naive dual-write | 1,000 | 986 | 14 | 1.4% | 1,031 orders/sec | 38.4 ms | 186.4 ms |
| transactional outbox | 1,000 | 1,000 | 0 | 0% | 734 orders/sec | 45.6 ms | 314.4 ms |

The outbox costs write latency (a multi-document transaction instead of a single insert) in exchange for zero lost events.

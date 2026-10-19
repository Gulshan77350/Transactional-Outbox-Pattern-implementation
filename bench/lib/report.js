import { round } from './stats.js';

const ms = (n) => `${round(n, 1)} ms`;
const int = (n) => Math.round(n).toLocaleString('en-US');
const table = (headers, rows) =>
  [`| ${headers.join(' | ')} |`, `| ${headers.map(() => '---').join(' | ')} |`, ...rows.map((r) => `| ${r.join(' | ')} |`)].join('\n');
const latencyRow = (label, s) => [label, ms(s.p50), ms(s.p95), ms(s.p99), ms(s.max), int(s.count)];
const LATENCY_HEADERS = ['Metric', 'p50', 'p95', 'p99', 'max', 'samples'];

export function headline(results) {
  const lines = [];
  const { throughput: t, e2e, backlog, recovery, baseline } = results;
  if (t) {
    lines.push(`Write path: **${int(t.throughput)} orders/sec** sustained at saturation (${int(t.orders)} orders, ${t.concurrency} concurrent connections, ${t.errors} errors, relay + consumer sharing the database)`);
  }
  if (e2e?.length) {
    const best = e2e.find((r) => r.pollIntervalMs === 50) ?? e2e[0];
    if (best.txnLatency) {
      lines.push(`Order + outbox transaction latency at ${best.ratePerSec} orders/sec: **p50 ${ms(best.txnLatency.p50)} / p95 ${ms(best.txnLatency.p95)} / p99 ${ms(best.txnLatency.p99)}**`);
    }
    const fastest = e2e.reduce((a, b) => (b.e2e.p50 < a.e2e.p50 ? b : a));
    if (fastest !== best) {
      lines.push(`Outbox lag (commit -> consumer, ${fastest.ratePerSec}/s steady, ${fastest.pollIntervalMs} ms poll): **p50 ${ms(fastest.e2e.p50)} / p95 ${ms(fastest.e2e.p95)} / p99 ${ms(fastest.e2e.p99)}**`);
    }
    lines.push(`Outbox lag (commit -> consumer, ${best.ratePerSec}/s steady, ${best.pollIntervalMs} ms poll): **p50 ${ms(best.e2e.p50)} / p95 ${ms(best.e2e.p95)} / p99 ${ms(best.e2e.p99)}**`);
  }
  if (backlog?.length) {
    const peak = backlog.reduce((a, b) => (b.eventsPerSec > a.eventsPerSec ? b : a));
    lines.push(`Backlog drain: **${int(peak.eventsPerSec)} events/sec** (${int(peak.events)} events, ${peak.relays} relay(s)), ${backlog.reduce((n, r) => n + r.doubleClaims, 0)} double-claims across all runs`);
  }
  if (recovery) {
    lines.push(`Crash recovery: **${int(recovery.stuckTotal)} stranded events delivered in ${round(recovery.recoveryMs / 1000, 2)} s** after relay restart, ${recovery.lost} lost, ${int(recovery.duplicatesSkipped)} redeliveries deduplicated`);
  }
  if (baseline) {
    const { naive, outbox } = baseline;
    lines.push(`At a ${baseline.faultRate * 100}% injected crash rate: naive dual-write lost **${int(naive.eventsLost)} of ${int(naive.orders)} events (${round(naive.lossRatePct, 2)}%)**, the outbox lost **${int(outbox.eventsLost)}**`);
  }
  return lines;
}

export function renderMarkdown(results, env) {
  const out = [];
  out.push('# Benchmark results', '');
  out.push(`Generated ${env.date} by \`npm run bench${env.quick ? ' -- --quick' : ''}\`.`, '');
  out.push('## Environment', '');
  out.push(
    table(
      ['', ''],
      [
        ['CPU', `${env.cpu} (${env.cores} logical cores)`],
        ['Memory', `${env.memGb} GB`],
        ['OS', env.os],
        ['Node.js', env.node],
        ['MongoDB', `${env.mongo} single-node replica set in Docker Desktop`],
        ['Topology', 'api, relay and consumer as separate Node processes on the host; one shared MongoDB'],
      ],
    ),
    '',
  );

  out.push('## Headline numbers', '', ...headline(results).map((l) => `- ${l}`), '');

  const { throughput: t, e2e, backlog, recovery, baseline } = results;
  if (t) {
    out.push('## 1. Write throughput and transaction latency (saturation)', '');
    out.push(
      `${int(t.orders)} \`POST /orders\` over ${t.concurrency} keep-alive connections (closed loop), with the relay and consumer running against the same MongoDB. ` +
        'Latencies here are dominated by queueing at saturation; see section 2 for latency at a steady rate.',
      '',
    );
    out.push(table(['Throughput', 'Duration', 'Errors', 'Relay caught up after load'], [[`${int(t.throughput)} orders/sec`, ms(t.durationMs), t.errors, ms(t.drainAfterLoadMs)]]), '');
    out.push(
      table(LATENCY_HEADERS, [
        latencyRow('Order + outbox transaction (server side)', t.txnLatency),
        latencyRow('HTTP request (client side)', t.requestLatency),
        latencyRow('Commit -> consumer under saturation', t.e2eUnderSaturation),
      ]),
      '',
    );
  }

  if (e2e?.length) {
    out.push('## 2. End-to-end outbox lag (steady load)', '');
    out.push(
      'Time from the order transaction committing (API process) to the consumer receiving the event (consumer process), at a fixed arrival rate below saturation. ' +
        `Each row pools ${e2e[0].runs ?? 1} independent runs on a fresh stack.`,
      '',
    );
    out.push(
      table(
        ['Poll interval', 'Rate', 'Events', 'p50', 'p95', 'p99', 'max', 'p50 range across runs'],
        e2e.map((r) => [
          `${r.pollIntervalMs} ms`,
          `${r.ratePerSec}/s`,
          int(r.e2e.count),
          ms(r.e2e.p50),
          ms(r.e2e.p95),
          ms(r.e2e.p99),
          ms(r.e2e.max),
          r.e2eP50Range ? `${ms(r.e2eP50Range[0])} - ${ms(r.e2eP50Range[1])}` : 'n/a',
        ]),
      ),
      '',
    );
    if (e2e[0].txnLatency) {
      out.push('Write-path latency during the same steady-load runs:', '');
      out.push(
        table(
          LATENCY_HEADERS,
          e2e.flatMap((r) => [
            latencyRow(`Order + outbox transaction (poll ${r.pollIntervalMs} ms runs)`, r.txnLatency),
            latencyRow(`HTTP request (poll ${r.pollIntervalMs} ms runs)`, r.requestLatency),
          ]),
        ),
        '',
      );
    }
  }

  if (backlog?.length) {
    out.push('## 3. Backlog drain', '');
    out.push('N events already PENDING; time until every one is SENT and processed by the consumer.', '');
    out.push(
      table(
        ['Backlog', 'Relays', 'Drain time', 'Throughput', 'Total claims', 'Double claims', 'Duplicates at consumer'],
        backlog.map((r) => [int(r.events), r.relays, ms(r.drainMs), `${int(r.eventsPerSec)} events/sec`, int(r.totalClaims), r.doubleClaims, r.duplicates]),
      ),
      '',
    );
  }

  if (recovery) {
    out.push('## 4. Crash recovery', '');
    out.push(`The relay is killed after ${recovery.crashAfterEmits} emits (after consumer ack, before mark-as-sent) while ${int(recovery.orders)} orders are written. A new relay is then started.`, '');
    out.push(
      table(
        ['Stranded PENDING', 'Stranded PROCESSING', 'Time to deliver all after restart', 'Recovery rate', 'Events lost', 'Duplicates deduplicated'],
        [[int(recovery.stuckPending), int(recovery.stuckProcessing), ms(recovery.recoveryMs), `${int(recovery.recoveryEventsPerSec)} events/sec`, recovery.lost, int(recovery.duplicatesSkipped)]],
      ),
      '',
    );
  }

  if (baseline) {
    out.push('## 5. Baseline: naive dual-write vs transactional outbox', '');
    out.push(`Same load and the same injected fault: for ${baseline.faultRate * 100}% of events the process "dies" between the database write and the publish.`, '');
    out.push(
      table(
        ['Design', 'Orders', 'Events delivered', 'Events lost', 'Loss rate', 'Throughput', 'Request p50', 'Request p99'],
        [baseline.naive, baseline.outbox].map((r) => [
          r.design,
          int(r.orders),
          int(r.eventsDelivered),
          int(r.eventsLost),
          `${round(r.lossRatePct, 2)}%`,
          `${int(r.throughput)} orders/sec`,
          ms(r.requestLatency.p50),
          ms(r.requestLatency.p99),
        ]),
      ),
      '',
    );
    out.push('The outbox costs write latency (a multi-document transaction instead of a single insert) in exchange for zero lost events.', '');
  }

  return out.join('\n');
}

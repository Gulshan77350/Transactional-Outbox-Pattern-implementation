/**
 * Runs the benchmark suite and writes RESULTS.md + bench/results.json.
 *
 *   npm run bench                     full suite (~5 minutes)
 *   npm run bench -- --quick          small sizes, for a sanity check
 *   npm run bench -- --only=backlog   one or more of: throughput,e2e,backlog,recovery,baseline
 *
 * Requires MongoDB from docker compose:  docker compose up -d mongo
 */
import { readFile, writeFile } from 'node:fs/promises';
import os from 'node:os';
import { connectAdmin } from '../scripts/lib/helpers.js';
import { renderMarkdown, headline } from './lib/report.js';
import * as backlog from './scenarios/backlog-drain.js';
import * as baseline from './scenarios/baseline.js';
import * as recovery from './scenarios/crash-recovery.js';
import * as e2e from './scenarios/e2e-latency.js';
import * as throughput from './scenarios/throughput.js';

const SCALES = {
  full: {
    warmup: 500,
    throughputOrders: 10_000,
    concurrency: 100,
    e2eRate: 200,
    e2eSeconds: 15,
    e2eRepeats: 3,
    pollIntervals: [10, 50, 100],
    backlogTiers: [
      { events: 1_000, relays: 1 },
      { events: 10_000, relays: 1 },
      { events: 50_000, relays: 1 },
      { events: 50_000, relays: 2 },
    ],
    recoveryOrders: 10_000,
    crashAfterEmits: 500,
    baselineOrders: 10_000,
    faultRate: 0.01,
  },
  quick: {
    warmup: 100,
    throughputOrders: 1_000,
    concurrency: 50,
    e2eRate: 100,
    e2eSeconds: 5,
    e2eRepeats: 1,
    pollIntervals: [50],
    backlogTiers: [
      { events: 1_000, relays: 1 },
      { events: 2_000, relays: 2 },
    ],
    recoveryOrders: 1_000,
    crashAfterEmits: 100,
    baselineOrders: 1_000,
    faultRate: 0.01,
  },
};

const SCENARIOS = { throughput, e2e, backlog, recovery, baseline };

const args = process.argv.slice(2);
const quick = args.includes('--quick');
const only = args.find((a) => a.startsWith('--only='))?.slice('--only='.length).split(',');
const scale = quick ? SCALES.quick : SCALES.full;

const admin = await connectAdmin();
const { version: mongo } = await admin.db('admin').command({ buildInfo: 1 });
const env = {
  date: new Date().toISOString(),
  quick,
  cpu: os.cpus()[0].model.trim(),
  cores: os.cpus().length,
  memGb: Math.round(os.totalmem() / 1024 ** 3),
  os: `${os.type()} ${os.release()} (${os.arch()})`,
  node: process.version,
  mongo,
};

const results = {};
try {
  for (const [name, scenario] of Object.entries(SCENARIOS)) {
    if (only && !only.includes(name)) continue;
    console.log(`\n=== ${name} ===`);
    const started = Date.now();
    results[name] = await scenario.run({ admin, scale, log: (msg) => console.log(`  ${msg}`) });
    console.log(`  done in ${((Date.now() - started) / 1000).toFixed(1)}s`);
  }
} finally {
  await admin.close();
}

// A partial run at the same scale updates its scenarios inside the previous
// full results instead of discarding the rest of the report.
const resultsFile = new URL('./results.json', import.meta.url);
let merged = results;
if (only) {
  const previous = await readFile(resultsFile, 'utf8').then(JSON.parse, () => null);
  if (previous && JSON.stringify(previous.scale) === JSON.stringify(scale)) {
    merged = { ...previous.results, ...results };
    console.log(`\nmerged ${only.join(', ')} into the previous results from ${previous.env.date}`);
  }
}

await writeFile(resultsFile, `${JSON.stringify({ env, scale, results: merged }, null, 2)}\n`);
await writeFile(new URL('../RESULTS.md', import.meta.url), renderMarkdown(merged, env));

console.log('\n=== Headline numbers ===');
for (const line of headline(merged)) console.log(`- ${line.replaceAll('**', '')}`);
console.log('\nWrote RESULTS.md and bench/results.json');

import { fork } from 'node:child_process';
import { once } from 'node:events';
import { createInterface } from 'node:readline';
import { fileURLToPath } from 'node:url';

const ROOT = fileURLToPath(new URL('../../', import.meta.url));

const COLORS = { api: 36, relay: 35, consumer: 32, naive: 33 };
const DETAIL_KEYS = ['orderId', 'eventId', 'released', 'attempts', 'emits', 'retryInMs', 'err', 'url'];

function printLine(name, line) {
  let text = line;
  try {
    const entry = JSON.parse(line);
    const details = DETAIL_KEYS.filter((k) => entry[k] !== undefined).map((k) => `${k}=${entry[k]}`);
    text = `${entry.msg}${details.length ? `  (${details.join(', ')})` : ''}`;
  } catch {
    // not a JSON log line; print as-is
  }
  const color = COLORS[name.split('-')[0]] ?? 37;
  console.log(`  \x1b[${color}m[${name}]\x1b[0m ${text}`);
}

/**
 * Forks one of the service entrypoints and resolves once it reports ready
 * over IPC. Rejects if the process exits before becoming ready.
 */
export async function startService(name, entry, env, { echo = false, readyTimeoutMs = 20_000 } = {}) {
  const child = fork(`${ROOT}${entry}`, [], {
    cwd: ROOT,
    env: { ...process.env, ...env },
    stdio: ['ignore', 'pipe', 'pipe', 'ipc'],
  });

  for (const stream of [child.stdout, child.stderr]) {
    const lines = createInterface({ input: stream });
    lines.on('line', (line) => echo && printLine(name, line));
  }

  const exited = once(child, 'exit').then(([code, signal]) => ({ code, signal }));

  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${name} did not become ready in ${readyTimeoutMs}ms`)), readyTimeoutMs);
    child.on('message', (msg) => {
      if (msg?.type === 'ready') {
        clearTimeout(timer);
        resolve();
      }
    });
    exited.then(({ code }) => {
      clearTimeout(timer);
      reject(new Error(`${name} exited with code ${code} before becoming ready`));
    });
  }).catch((err) => {
    child.kill();
    throw err;
  });

  return {
    name,
    child,
    exited,
    async stop() {
      if (child.exitCode !== null || child.signalCode !== null) return;
      child.kill('SIGTERM');
      await exited;
    },
  };
}

export async function stopAll(services) {
  await Promise.all(services.filter(Boolean).map((s) => s.stop()));
}

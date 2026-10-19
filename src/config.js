const DEFAULT_MONGO_URI = 'mongodb://localhost:27017/outbox?directConnection=true';

function int(env, name, fallback) {
  const raw = env[name];
  if (raw === undefined || raw === '') return fallback;
  const value = Number.parseInt(raw, 10);
  if (Number.isNaN(value)) throw new Error(`${name} must be an integer, got "${raw}"`);
  return value;
}

function float(env, name, fallback) {
  const raw = env[name];
  if (raw === undefined || raw === '') return fallback;
  const value = Number.parseFloat(raw);
  if (Number.isNaN(value)) throw new Error(`${name} must be a number, got "${raw}"`);
  return value;
}

export function loadConfig(env = process.env) {
  return {
    mongoUri: env.MONGO_URI || DEFAULT_MONGO_URI,
    logLevel: env.LOG_LEVEL || 'info',
    port: int(env, 'PORT', 3000),
    socketPort: int(env, 'SOCKET_PORT', 4000),
    // Shared secret consumers present to the relay; unset disables the check.
    relayAuthToken: env.RELAY_AUTH_TOKEN || undefined,
    relay: {
      workerId: env.WORKER_ID || undefined,
      pollIntervalMs: int(env, 'POLL_INTERVAL_MS', 50),
      batchSize: int(env, 'BATCH_SIZE', 100),
      maxInFlightBatches: int(env, 'MAX_IN_FLIGHT_BATCHES', 2),
      leaseMs: int(env, 'LEASE_MS', 10_000),
      ackTimeoutMs: int(env, 'ACK_TIMEOUT_MS', 5_000),
      maxAttempts: int(env, 'MAX_ATTEMPTS', 8),
      backoffBaseMs: int(env, 'BACKOFF_BASE_MS', 200),
      backoffMaxMs: int(env, 'BACKOFF_MAX_MS', 30_000),
    },
    consumer: {
      concurrency: int(env, 'CONSUMER_CONCURRENCY', 8),
      relayUrls: (env.RELAY_URLS || 'http://localhost:4000')
        .split(',')
        .map((url) => url.trim())
        .filter(Boolean),
    },
    // Fault injection, used only by the demo and benchmark scripts.
    faults: {
      crashAfterEmits: int(env, 'FAULT_CRASH_AFTER_EMITS', 0),
      dropRate: float(env, 'FAULT_DROP_RATE', 0),
    },
  };
}

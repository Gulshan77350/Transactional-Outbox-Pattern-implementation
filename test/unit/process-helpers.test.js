import { MongoMemoryServer } from 'mongodb-memory-server';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { connectMongo } from '../../src/db/mongo.js';
import { onShutdown, signalReady } from '../../src/shared/lifecycle.js';

describe('connectMongo against a standalone server', () => {
  it('refuses to start, because transactions would fail', async () => {
    const standalone = await MongoMemoryServer.create();
    try {
      await expect(connectMongo(standalone.getUri('x'))).rejects.toThrow(/not running as a replica set/);
    } finally {
      await standalone.stop();
    }
  });
});

describe('lifecycle', () => {
  afterEach(() => vi.restoreAllMocks());

  function captureSignalHandlers() {
    const handlers = {};
    vi.spyOn(process, 'on').mockImplementation((signal, handler) => {
      handlers[signal] = handler;
      return process;
    });
    const exit = vi.spyOn(process, 'exit').mockImplementation(() => {});
    return { handlers, exit };
  }

  const logger = { info: vi.fn(), error: vi.fn() };

  it('runs cleanup once and exits 0 on SIGTERM', async () => {
    const { handlers, exit } = captureSignalHandlers();
    const cleanup = vi.fn().mockResolvedValue();
    onShutdown(logger, cleanup);

    await handlers.SIGTERM('SIGTERM');
    await handlers.SIGINT('SIGINT');

    expect(cleanup).toHaveBeenCalledTimes(1);
    expect(exit).toHaveBeenCalledWith(0);
  });

  it('exits 1 when cleanup fails', async () => {
    const { handlers, exit } = captureSignalHandlers();
    onShutdown(logger, vi.fn().mockRejectedValue(new Error('stuck')));

    await handlers.SIGINT('SIGINT');
    expect(logger.error).toHaveBeenCalled();
    expect(exit).toHaveBeenCalledWith(1);
  });

  it('reports readiness to a parent process when there is one', () => {
    const original = process.send;
    process.send = vi.fn();
    try {
      signalReady({ port: 1 });
      expect(process.send).toHaveBeenCalledWith({ type: 'ready', port: 1 });
    } finally {
      process.send = original;
    }
  });
});

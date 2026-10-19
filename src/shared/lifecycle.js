/** Runs cleanup once on SIGINT/SIGTERM and tells a parent process (if any) we are up. */
export function onShutdown(logger, cleanup) {
  let closing = false;
  const shutdown = async (signal) => {
    if (closing) return;
    closing = true;
    logger.info({ signal }, 'shutting down');
    try {
      await cleanup();
      process.exit(0);
    } catch (err) {
      logger.error({ err }, 'error during shutdown');
      process.exit(1);
    }
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

export function signalReady(info = {}) {
  process.send?.({ type: 'ready', ...info });
}

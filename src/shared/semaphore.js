/** Minimal counting semaphore: run(fn) waits for a free slot before calling fn. */
export function createSemaphore(limit) {
  let active = 0;
  const waiting = [];

  const release = () => {
    active -= 1;
    waiting.shift()?.();
  };

  return {
    get active() {
      return active;
    },
    get queued() {
      return waiting.length;
    },
    async run(fn) {
      if (active >= limit) await new Promise((resolve) => waiting.push(resolve));
      active += 1;
      try {
        return await fn();
      } finally {
        release();
      }
    },
  };
}

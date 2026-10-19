/**
 * Builds relay hooks from FAULT_* settings. Both are off by default and are
 * only switched on by the demo and benchmark scripts.
 *
 * - crashAfterEmits: exit the process right after the Nth successful emit,
 *   i.e. after consumers acked but before the row is marked SENT.
 * - dropRate: probability that a claimed row is abandoned before its emit,
 *   leaving it PROCESSING until the lease expires.
 */
export function createFaultHooks({ crashAfterEmits = 0, dropRate = 0 }, { logger, random = Math.random, exit = process.exit } = {}) {
  let emits = 0;
  const hooks = {};

  if (dropRate > 0) {
    hooks.beforeDeliver = () => (random() < dropRate ? 'drop' : undefined);
  }

  if (crashAfterEmits > 0) {
    hooks.afterDeliver = (row) => {
      emits += 1;
      if (emits >= crashAfterEmits) {
        logger?.fatal({ eventId: row._id, emits }, 'FAULT: crashing after emit, before mark-as-sent');
        exit(137);
      }
    };
  }

  return hooks;
}

/**
 * Exponential backoff with "equal jitter": the delay doubles per attempt
 * (capped at maxMs) and a random half of it is jittered so that events that
 * failed together do not all retry in the same instant.
 */
export function computeBackoffMs(attempt, { baseMs, maxMs, random = Math.random }) {
  const exponential = Math.min(maxMs, baseMs * 2 ** Math.max(0, attempt - 1));
  return Math.round(exponential / 2 + random() * (exponential / 2));
}

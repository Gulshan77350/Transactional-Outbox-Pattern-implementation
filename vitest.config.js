import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['test/**/*.test.js'],
    globalSetup: ['test/setup/mongo-replset.js'],
    testTimeout: 30_000,
    hookTimeout: 180_000,
    coverage: {
      provider: 'istanbul',
      include: ['src/**/*.js'],
      // Process entrypoints only wire config to modules; they are exercised by
      // the demo scripts and benchmark, which drive them as real processes.
      // The naive baseline is benchmark-only code.
      exclude: ['src/entrypoints/**', 'src/baseline/**'],
      reporter: ['text', 'text-summary', 'json-summary'],
    },
  },
});

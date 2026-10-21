import type { E2EConfig } from 'e2e';
import { defineEngine } from 'e2e/engine';

// The product has no browser UI. e2e supports fixture-only engines;
// assertions exercise its actual subprocess CLI and stdio MCP transport.
export default {
  targets: [{ name: 'local-cli', engine: defineEngine({
    name: 'fleetq-cli', version: '1.0.0', spiVersion: 1, platform: 'cli',
  }), app: {} }],
  tests: ['tests/**/*.e2e.ts'],
  workers: 1,
  retries: 0,
} satisfies E2EConfig;

import { defineConfig } from 'tsup'

export default defineConfig({
  entry: {
    index: 'src/index.ts',
    'client/index': 'src/client/index.ts',
    'mcp/index': 'src/mcp/index.ts',
    'mcp/cli': 'src/mcp/cli.ts',
  },
  format: ['esm'],
  dts: true,
  sourcemap: true,
  clean: true,
  splitting: false,
  external: ['cloudflare:workers', 'cloudflare:sockets'],
  banner: {
    js: '#!/usr/bin/env node',
  },
})

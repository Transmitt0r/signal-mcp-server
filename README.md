# signal-mcp-server

MCP (Model Context Protocol) server for Signal messenger. Wraps a running
[signal-cli](https://github.com/AsamK/signal-cli) daemon's JSON-RPC API as MCP tools.

[![CI](https://github.com/Transmitt0r/signal-mcp-server/actions/workflows/ci.yml/badge.svg)](https://github.com/Transmitt0r/signal-mcp-server/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/signal-mcp-server)](https://www.npmjs.com/package/signal-mcp-server)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Prerequisites

- [signal-cli](https://github.com/AsamK/signal-cli) daemon running in HTTP mode
  (e.g. `signal-cli --account +491****6789 daemon --http 127.0.0.1:8080`)
- Node.js 22+

## Usage

### Stdio (for Hermes MCP, Claude Desktop, etc.)

```bash
SIGNAL_HTTP_URL=http://127.0.0.1:8080 npx signal-mcp-server
```

Configure in Hermes:

```yaml
mcp_servers:
  signal:
    command: "npx"
    args: ["-y", "signal-mcp-server"]
    env:
      SIGNAL_HTTP_URL: "http://127.0.0.1:8080"
      SIGNAL_ACCOUNT: "+491****6789"
    timeout: 30
```

### HTTP transport

```bash
signal-mcp-server --http 3100
```

Then call tools via JSON-RPC:

```bash
curl -X POST http://localhost:3100/ \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","method":"tools/list","id":"1"}'
```

### Docker

```bash
# Pull from GitHub Container Registry
docker run -e SIGNAL_HTTP_URL=http://host.docker.internal:8080 \
  ghcr.io/transmitt0r/signal-mcp-server:latest

# Or build locally
docker build -t signal-mcp-server .
docker run -e SIGNAL_HTTP_URL=http://host.docker.internal:8080 \
  signal-mcp-server
```

## Tools

| Tool | Description |
|------|-------------|
| `signal_list_contacts` | List all Signal contacts |
| `signal_list_groups` | List all Signal groups |
| `signal_list_conversations` | List recent conversations |
| `signal_read_messages` | Read recent messages (optionally filtered by sender) |
| `signal_send_message` | Send a message to a recipient or group |
| `signal_send_reaction` | React to a message with an emoji |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
- `SIGNAL_HTTP_URL` | `http://127.0.0.1:8081` | signal-cli HTTP endpoint |
| `SIGNAL_ACCOUNT` | `""` | Phone number for display (optional) |
| `SIGNAL_MCP_MAX_MSGS` | `500` | Max messages to buffer in memory |

## Architecture

The server connects to a running signal-cli daemon via its JSON-RPC HTTP API. It
uses two main components:

- **SSE consumer**: Opens a long-lived connection to the signal-cli event stream
  (`/api/v1/events`) and parses Server-Sent Events into `RpcEnvelope` objects.
  An `AbortController` allows clean shutdown on `SIGINT`/`SIGTERM`.

- **Message buffer**: An in-memory ring buffer that stores the most recent
  messages (configurable via `SIGNAL_MCP_MAX_MSGS`, default 500). Messages are
  deduplicated using a `source:timestamp` composite key. Tools like
  `signal_read_messages` and `signal_list_conversations` query this buffer
  rather than making additional RPC calls, keeping latency low.

The MCP server registers six tools (`signal_list_contacts`, `signal_list_groups`,
`signal_list_conversations`, `signal_read_messages`, `signal_send_message`,
`signal_send_reaction`) and exposes them via stdio (default) or Streamable HTTP.

## Development

```bash
# Install dependencies
pnpm install

# Build (TypeScript compilation)
pnpm build

# Run unit tests (vitest)
pnpm test:unit

# Run all tests (unit + integration)
pnpm test

# Lint (tsc — also covered by build)
pnpm build
```

The TypeScript source lives under `src/`. The main logic is in `src/index.ts`,
shared types and helpers are in `src/lib.ts`, and unit tests in
`src/tests/unit.test.ts`.
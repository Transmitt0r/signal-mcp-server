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
| `SIGNAL_HTTP_URL` | `http://127.0.0.1:8080` | signal-cli HTTP endpoint |
| `SIGNAL_ACCOUNT` | `""` | Phone number for display (optional) |
| `SIGNAL_MCP_MAX_MSGS` | `500` | Default number of messages returned per query when no explicit limit is given |
| `SIGNAL_MCP_STATE_DIR` | `$XDG_STATE_HOME/signal-mcp-server` (falls back to `~/.local/state/signal-mcp-server`) | Directory holding the persisted message database (`messages.db`) |
| `SIGNAL_MCP_NO_PERSIST` | unset | Set to `1`/`true` to disable disk persistence entirely (buffer becomes memory-only, matching pre-persistence behavior) |
| `SIGNAL_MCP_RECEIVE_LOG` | unset | Path to signal-cli's JSON receive log (see "Closing the message-loss gap" below). Strongly recommended — this is what actually prevents missed messages, not just SSE reconnect tuning. |

## Closing the message-loss gap (recommended setup)

By default this server ingests messages purely via signal-cli's SSE stream
(`/api/v1/events`), which has a fundamental limitation: **signal-cli does not
queue or replay missed messages for SSE subscribers.** Its daemon keeps a
permanent, always-on receive thread (started via `--receive-mode on-start`)
that continuously drains the server queue and deletes each envelope from its
own disk cache the instant it's handled — regardless of whether any SSE
client is connected. The SSE endpoint registers its subscription as a *weak*
listener (`HttpServerHandler#subscribeReceiveHandlers`, `isWeakListener=true`):
it only receives whatever arrives while the HTTP connection happens to be
open. If this MCP server (or its SSE connection) is down when a message
arrives, that message is gone for good — persistence doesn't help, because
the message was never captured in the first place.

**The fix**: signal-cli's *default* receive handler — the one active when you
start the daemon with `-o json` and redirect its stdout to a file — is
registered as a **strong** listener, tied to the daemon process itself
rather than to any one HTTP client. Every message gets written to that file
unconditionally, independent of whether anything is listening. Point
`SIGNAL_MCP_RECEIVE_LOG` at that file and this server tails it with a durable
byte-offset checkpoint (stored in the SQLite buffer), so a restart resumes
exactly where it left off and catches up on everything written while it was
down — actually closing the gap, not just shrinking it.

Setup (systemd user service shown; adapt for your init system):

```ini
[Service]
ExecStart=/usr/local/bin/signal-cli -o json --account +1XXXXXXXXXX daemon --http 127.0.0.1:8081
StandardOutput=append:/path/to/state/signal-cli/receive.jsonl
```

```bash
export SIGNAL_MCP_RECEIVE_LOG=/path/to/state/signal-cli/receive.jsonl
```

Without this, the server still works out of the box via SSE alone, but any
message that arrives during a disconnect (deploys, restarts, network blips)
is silently missed.

## Architecture

The server connects to a running signal-cli daemon via its JSON-RPC HTTP API. It
uses three main components:

- **Receive-log tailer** (primary ingestion path, when `SIGNAL_MCP_RECEIVE_LOG`
  is set): reads new bytes appended to signal-cli's JSON receive log, parses
  each JSON-Lines entry (non-JSON lines — signal-cli's own startup/warning
  logs share the same stdout — are skipped), and tracks its position via a
  durable byte offset. Uses `fs.watch` for low-latency pickup with a 2s
  polling backstop for filesystems where watch events aren't reliable.

- **SSE consumer**: Opens a long-lived connection to the signal-cli event stream
  (`/api/v1/events`) and parses Server-Sent Events into `RpcEnvelope` objects.
  An `AbortController` allows clean shutdown on `SIGINT`/`SIGTERM`. Reconnects
  use exponential backoff starting at 250ms (capped at 5s) rather than a flat
  5s delay, to minimize the window in which an incoming message could be
  missed during a restart or network blip. Runs unconditionally (even
  alongside the receive-log tailer) since it works with zero extra signal-cli
  configuration and message-level deduplication makes it harmless to
  double-ingest the same envelope via both paths.


- **Message buffer**: Backed by a local SQLite database
  (`$SIGNAL_MCP_STATE_DIR/messages.db`, via `better-sqlite3`) rather than an
  in-memory ring buffer. Every message received is durably written as it
  arrives; a `UNIQUE(source, ts)` constraint handles deduplication, and
  indexes on `source` and `ts` keep sender/time-range lookups fast even as
  history grows. `signal_read_messages` and `signal_list_conversations` query
  this store directly.

  This means a restart of the MCP server, or of the signal-cli daemon it
  depends on, no longer loses message history — the whole point of keeping
  history around for tools that only see it "since last restart" otherwise.
  Set `SIGNAL_MCP_NO_PERSIST=1` to opt out and run purely in-memory (data is
  lost on restart, as in earlier versions).

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
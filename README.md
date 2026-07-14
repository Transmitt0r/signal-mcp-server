# signal-mcp-server

MCP (Model Context Protocol) server for Signal messenger. Wraps a running
[signal-cli](https://github.com/AsamK/signal-cli) daemon's JSON-RPC API as MCP tools.

[![CI](https://github.com/Transmitt0r/signal-mcp-server/actions/workflows/ci.yml/badge.svg)](https://github.com/Transmitt0r/signal-mcp-server/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/%40transmitt0r%2Fsignal-mcp-server)](https://www.npmjs.com/package/@transmitt0r/signal-mcp-server)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Prerequisites

- [signal-cli](https://github.com/AsamK/signal-cli) daemon running in HTTP mode
  **with `--receive-mode=manual`** (see "Message ingestion" below for why this
  matters):
  `signal-cli --account +491****6789 daemon --http 127.0.0.1:8080 --receive-mode=manual`
- Node.js 22+

## Usage

### Stdio (for Hermes MCP, Claude Desktop, etc.)

```bash
SIGNAL_HTTP_URL=http://127.0.0.1:8080 npx @transmitt0r/signal-mcp-server
```

Configure in Hermes:

```yaml
mcp_servers:
  signal:
    command: "npx"
    args: ["-y", "@transmitt0r/signal-mcp-server"]
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

## Message ingestion: why `--receive-mode=manual` is required

Earlier versions of this server ingested messages via signal-cli's SSE
stream (`/api/v1/events`), with an optional file-tailer for durability. Both
approaches shared a structural problem, rooted in how signal-cli's **default**
receive mode works:

By default (`--receive-mode=on-start`), signal-cli's daemon runs a permanent
background thread from the moment it starts that continuously drains
Signal's server-side message queue and deletes each envelope from it —
**regardless of whether anything is listening**. Once a message is drained
this way, the only copies that exist are whatever a connected handler
happened to catch at that instant:

- The SSE endpoint (`/api/v1/events`) registers as a "weak" listener: it only
  receives messages that arrive while its HTTP connection happens to be
  open. Disconnects (deploys, restarts, network blips) mean those messages
  are gone for good — signal-cli does not queue or replay them.
- A file-tailer reading signal-cli's redirected stdout (`-o json` +
  `StandardOutput=append:...`) was intended to close this gap by acting as
  an always-on "strong" listener tied to the daemon process itself. In
  practice, on the systemd setup this was validated against, per-message
  envelope logging landed on **stderr**, not the redirected stdout stream —
  so the file only ever contained daemon startup/shutdown lines, and the
  "durable" path silently ingested nothing. This is exactly the kind of
  environment-specific log-routing assumption that's fragile to depend on.

**The actual fix**: run the signal-cli daemon with `--receive-mode=manual`.
This disables the automatic background drain thread entirely, so incoming
messages simply stay queued **on Signal's own servers** — not "buffered
somewhere on this box" — until this MCP server explicitly pulls them via the
JSON-RPC `receive` method (`POST /api/v1/rpc`, `{"method":"receive"}`),
which signal-cli's changelog documents as being available specifically "for
polling new messages." This server runs that call in a long-poll loop
(~55s per call) as its sole ingestion path. If the MCP server (or the whole
box) is down for hours, nothing is lost — the next successful `receive` call
just returns everything that queued up in the meantime.

**Constraint to be aware of**: signal-cli only allows one active receiver
per account at a time. If the daemon is still running in the default
`on-start` mode (or something else is calling `receive`/holding an SSE
connection concurrently), this server's poll calls will fail with `"Receive
command cannot be used if messages are already being received"` — logged
distinctly so it's easy to diagnose. There is no way to run this server's
poller *alongside* an SSE consumer or the default on-start thread; pick one.

Example systemd unit:

```ini
[Service]
ExecStart=/usr/local/bin/signal-cli -o json --account +1XXXXXXXXXX daemon --http 127.0.0.1:8081 --receive-mode=manual
Restart=on-failure
RestartSec=5
```

## Architecture

The server connects to a running signal-cli daemon via its JSON-RPC HTTP API. It
uses two main components:

- **Receive poller** (sole ingestion path): long-polls the JSON-RPC `receive`
  method (`POST /api/v1/rpc`, ~55s timeout per call) in a loop for as long as
  the process runs. Requires the daemon to be started with
  `--receive-mode=manual` (see "Message ingestion" above) — otherwise
  signal-cli's own background receive thread holds the account's single
  receiver slot and every poll call fails with "already being received".
  Because messages queue safely server-side until pulled, there's no
  reconnect-latency or backoff tuning to get right: a poll that returns
  empty just means nothing arrived in that window, and the very next poll
  picks up anything that landed while this process was down entirely.

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
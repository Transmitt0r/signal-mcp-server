# signal-mcp-server

MCP (Model Context Protocol) server for Signal messenger. Wraps a running
[signal-cli](https://github.com/AsamK/signal-cli) daemon's JSON-RPC API as MCP tools.

## Prerequisites

- [signal-cli](https://github.com/AsamK/signal-cli) daemon running in HTTP mode
  (e.g. `signal-cli --account +49123456789 daemon --http 127.0.0.1:8080`)
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
      SIGNAL_ACCOUNT: "+49123456789"
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
docker run -e SIGNAL_HTTP_URL=http://host.docker.internal:8080 \
  ghcr.io/transmitt0r/signal-mcp-server:latest
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
| `SIGNAL_MCP_MAX_MSGS` | `500` | Max messages to buffer in memory |
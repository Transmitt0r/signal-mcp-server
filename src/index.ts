#!/usr/bin/env node

/**
 * Signal MCP Server — wraps signal-cli daemon JSON-RPC API as MCP tools.
 *
 * Usage:
 *   signal-mcp-server              # stdio transport (default)
 *   signal-mcp-server --http 3100  # Streamable HTTP transport
 *   signal-mcp-server --http       # HTTP on default port 3100
 *
 * Environment variables:
 *   SIGNAL_HTTP_URL        — signal-cli HTTP endpoint (default: http://127.0.0.1:8080)
 *   SIGNAL_ACCOUNT         — phone number for display (optional)
 *   SIGNAL_MCP_MAX_MSGS    — max messages returned per query by default (default: 500)
 *   SIGNAL_MCP_STATE_DIR   — directory for the persisted message database
 *                            (default: $XDG_STATE_HOME/signal-mcp-server or
 *                            ~/.local/state/signal-mcp-server)
 *   SIGNAL_MCP_NO_PERSIST  — set to "1"/"true" to disable disk persistence
 *                            entirely (buffer is memory-only, legacy behavior)
 */

import * as os from "node:os";
import * as path from "node:path";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";

import { MessageBuffer, formatEnvelope, type RpcEnvelope } from "./lib.js";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const SIGNAL_HTTP_URL = process.env.SIGNAL_HTTP_URL ?? "http://127.0.0.1:8080";
const SIGNAL_ACCOUNT = process.env.SIGNAL_ACCOUNT ?? "";

const NO_PERSIST = /^(1|true)$/i.test(process.env.SIGNAL_MCP_NO_PERSIST ?? "");
const STATE_DIR =
  process.env.SIGNAL_MCP_STATE_DIR ??
  path.join(process.env.XDG_STATE_HOME ?? path.join(os.homedir(), ".local", "state"), "signal-mcp-server");
const PERSIST_PATH = NO_PERSIST ? ":memory:" : path.join(STATE_DIR, "messages.db");

// ---------------------------------------------------------------------------
// Message Buffer
// ---------------------------------------------------------------------------

const buffer = new MessageBuffer({ persistPath: PERSIST_PATH });
if (!NO_PERSIST) {
  console.error(`[signal-mcp] Persisting message history to ${PERSIST_PATH} (SQLite)`);
  console.error(`[signal-mcp] ${buffer.count()} message(s) available from prior sessions`);
} else {
  console.error(`[signal-mcp] Persistence disabled (SIGNAL_MCP_NO_PERSIST set) — buffer is memory-only`);
}

// ---------------------------------------------------------------------------
// SSE consumer
// ---------------------------------------------------------------------------
//
// IMPORTANT: signal-cli does NOT queue/replay missed messages for SSE
// clients. Its daemon keeps a permanent, always-on receive thread (started
// at daemon boot via --receive-mode on-start) that continuously drains the
// server queue and deletes each envelope from its on-disk cache the instant
// it's handed to handlers — regardless of whether any SSE client is
// connected. Our SSE subscription is registered as a "weak" handler
// (HttpServerHandler#subscribeReceiveHandlers, isWeakListener=true): it only
// receives whatever arrives while the HTTP connection is open. There is no
// backlog to drain on reconnect — a message that arrives while we're
// disconnected is gone for good, full stop.
//
// The only gap we can actually close is our OWN reconnect latency. Previously
// this used a flat 5s delay on every retry, including the very first retry
// right after our own restart — pure dead time in the exact window most
// likely to lose a message (our deploys/restarts). Now we retry immediately
// with a short exponential backoff (250ms, 500ms, 1s, 2s, capped at 5s),
// and log how long we were actually disconnected so gaps are diagnosable.

let sseAbortController: AbortController | null = null;

const SSE_RETRY_BASE_MS = 250;
const SSE_RETRY_MAX_MS = 5000;

function startSseConsumer(): void {
  const eventsUrl = `${SIGNAL_HTTP_URL}/api/v1/events`;
  console.error(`[signal-mcp] SSE consumer starting: ${eventsUrl}`);

  sseAbortController = new AbortController();
  const signal = sseAbortController.signal;
  let retryAttempt = 0;
  let disconnectedAt: number | null = null;

  const scheduleRetry = () => {
    if (signal.aborted) return;
    const delay = Math.min(SSE_RETRY_BASE_MS * 2 ** retryAttempt, SSE_RETRY_MAX_MS);
    retryAttempt += 1;
    setTimeout(poll, delay);
  };

  const poll = async () => {
    if (signal.aborted) return;

    try {
      const resp = await fetch(eventsUrl, { signal });
      if (!resp.ok || !resp.body) {
        console.error(`[signal-mcp] SSE: response status ${resp.status}, retrying`);
        scheduleRetry();
        return;
      }

      // Connected: reset backoff and report how long we were dark, if this
      // was a reconnect rather than the initial startup connection.
      retryAttempt = 0;
      if (disconnectedAt !== null) {
        const gapMs = Date.now() - disconnectedAt;
        console.error(`[signal-mcp] SSE reconnected after ${gapMs}ms disconnected`);
        disconnectedAt = null;
      }

      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      let buf = "";

      while (!signal.aborted) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });

        let idx: number;
        while ((idx = buf.indexOf("\n\n")) !== -1) {
          const raw = buf.slice(0, idx);
          buf = buf.slice(idx + 2);
          const trimmed = raw.trim();
          if (!trimmed || trimmed.startsWith(":")) continue;

          const dataLine = trimmed.split("\n").find((l) => l.startsWith("data: "));
          if (!dataLine) continue;

          try {
            const payload = JSON.parse(dataLine.slice(6));
            if (payload.envelope) {
              buffer.add({ params: { envelope: payload.envelope } });
            }
          } catch {
            // skip parse errors
          }
        }
      }
      if (!signal.aborted) {
        disconnectedAt = Date.now();
        console.error("[signal-mcp] SSE stream ended, reconnecting immediately");
      }
    } catch (err) {
      if ((err as Error)?.name === "AbortError") return;
      disconnectedAt ??= Date.now();
      console.error(`[signal-mcp] SSE connection error: ${err}, retrying`);
    }
    scheduleRetry();
  };

  poll();
}


function stopSseConsumer(): void {
  if (sseAbortController) {
    sseAbortController.abort();
    sseAbortController = null;
  }
}

// ---------------------------------------------------------------------------
// JSON-RPC client
// ---------------------------------------------------------------------------

async function rpc(method: string, params?: Record<string, unknown>, timeout = 15000): Promise<unknown> {
  const rpcUrl = `${SIGNAL_HTTP_URL}/api/v1/rpc`;
  const payload = { jsonrpc: "2.0", method, id: method, ...(params ? { params } : {}) };

  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeout);

  try {
    const resp = await fetch(rpcUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}: ${resp.statusText}`);
    const data = (await resp.json()) as { result?: unknown; error?: { message?: string } };
    if (data.error) throw new Error(`JSON-RPC error: ${data.error.message ?? JSON.stringify(data.error)}`);
    return data.result;
  } finally {
    clearTimeout(timer);
  }
}

// ---------------------------------------------------------------------------
// Tool schemas & handlers
// ---------------------------------------------------------------------------

const TOOLS = [
  {
    name: "signal_list_contacts",
    description: "List all Signal contacts with their phone numbers and names",
    inputSchema: { type: "object", properties: {} },
    handler: async () => {
      const result = (await rpc("listContacts")) as Array<Record<string, unknown>>;
      if (!result?.length) return "No contacts found.";
      const lines = ["📇 **Signal Contacts**\n"];
      for (const c of result) {
        const number = (c.number as string) ?? "?";
        const name = (c.name ?? c.profileName ?? c.givenName ?? "") as string;
        const uuid = ((c.uuid as string) ?? "").slice(0, 8);
        lines.push(`- ${name} — ${number} (uuid: ${uuid}…)`);
      }
      return lines.join("\n");
    },
  },
  {
    name: "signal_list_groups",
    description: "List all Signal groups the account is a member of",
    inputSchema: { type: "object", properties: {} },
    handler: async () => {
      const result = (await rpc("listGroups")) as Array<Record<string, unknown>>;
      if (!result?.length) return "No groups found.";
      const lines = ["👥 **Signal Groups**\n"];
      for (const g of result) {
        if (!g.isMember) continue;
        const name = (g.name as string) ?? "(unnamed)";
        const gid = ((g.id as string) ?? "").slice(0, 16);
        const members = (g.members as Array<unknown>)?.length ?? 0;
        lines.push(`- ${name} — ${members} members (id: ${gid}…)`);
      }
      return lines.join("\n");
    },
  },
  {
    name: "signal_list_conversations",
    description: "List recent conversations (contacts who have sent messages recently)",
    inputSchema: {
      type: "object",
      properties: {
        limit: { type: "integer", description: "Max conversations to show", default: 20 },
      },
    },
    handler: async (args: Record<string, unknown>) => {
      const limit = Math.min((args.limit as number) ?? 20, 100);
      const recent = buffer.getRecent(500);
      const seen = new Map<string, string>();
      for (const msg of recent) {
        const env = msg.params?.envelope;
        const src = env?.sourceNumber ?? env?.source ?? "";
        const srcName = env?.sourceName ?? "";
        if (src && !seen.has(src)) seen.set(src, srcName);
        const sync = env?.syncMessage?.sentMessage;
        if (sync) {
          const dest = sync.destinationNumber ?? sync.destination ?? "";
          if (dest && !seen.has(dest)) seen.set(dest, "(sent by you)");
        }
      }
      const entries = Array.from(seen.entries()).slice(0, limit);
      if (!entries.length) return "No conversations yet.";
      const lines = ["💬 **Recent Conversations**\n"];
      for (const [num, label] of entries) {
        const nameStr = label ? ` (${label})` : "";
        lines.push(`- ${num.slice(0, 16)}${nameStr}`);
      }
      return lines.join("\n");
    },
  },
  {
    name: "signal_read_messages",
    description: "Read recent messages. If sender is specified, filter by that sender.",
    inputSchema: {
      type: "object",
      properties: {
        sender: { type: "string", description: "Optional sender phone number (E.164 like +491****6789) or UUID to filter by" },
        limit: { type: "integer", description: "Max messages to return", default: 20 },
      },
    },
    handler: async (args: Record<string, unknown>) => {
      const sender = (args.sender as string) ?? "";
      const limit = Math.min((args.limit as number) ?? 20, 100);
      const raw = sender ? buffer.getConversation(sender, limit) : buffer.getRecent(limit);
      if (!raw.length) return "No messages in buffer.";
      const lines = ["📨 **Signal Messages**\n"];
      for (const entry of raw) {
        const env = entry.params?.envelope;
        if (env) lines.push(formatEnvelope(env));
      }
      return lines.join("\n");
    },
  },
  {
    name: "signal_send_message",
    description: "Send a Signal message to a phone number or a group",
    inputSchema: {
      type: "object",
      properties: {
        recipient: { type: "string", description: "Recipient phone number in E.164 format (e.g. +491****6789)" },
        message: { type: "string", description: "Message text to send" },
        groupId: { type: "string", description: "Base64-encoded group ID to send to instead of a recipient" },
      },
      required: ["message"],
    },
    handler: async (args: Record<string, unknown>) => {
      const message = (args.message as string) ?? "";
      if (!message) return "Error: message is required.";
      const recipient = (args.recipient as string) ?? "";
      const groupId = (args.groupId as string) ?? "";
      if (!recipient && !groupId) return "Error: either recipient or groupId is required.";

      const params: Record<string, unknown> = { message };
      if (recipient) params.recipient = [recipient];
      if (groupId) params.groupId = groupId;

      const result = (await rpc("send", params, 30000)) as { timestamp?: number };
      const ts = result?.timestamp ?? 0;
      return `✅ Message sent (timestamp: ${ts})`;
    },
  },
  {
    name: "signal_send_reaction",
    description: "React to a message with an emoji",
    inputSchema: {
      type: "object",
      properties: {
        recipient: { type: "string", description: "Recipient phone number (E.164)" },
        emoji: { type: "string", description: "Emoji reaction (e.g. 👍, ❤️, 😂)" },
        targetTimestamp: { type: "number", description: "Timestamp of the message to react to" },
        groupId: { type: "string", description: "Group ID if reacting in a group" },
        remove: { type: "boolean", description: "Remove an existing reaction", default: false },
      },
      required: ["recipient", "emoji", "targetTimestamp"],
    },
    handler: async (args: Record<string, unknown>) => {
      const recipient = (args.recipient as string) ?? "";
      const emoji = (args.emoji as string) ?? "";
      const targetTs = (args.targetTimestamp as number) ?? 0;
      const groupId = (args.groupId as string) ?? "";
      const remove = (args.remove as boolean) ?? false;

      if (!recipient || !emoji || !targetTs) return "Error: recipient, emoji, and targetTimestamp are required.";

      const params: Record<string, unknown> = { recipient: [recipient], emoji, targetTimestamp: targetTs, remove };
      if (groupId) params.groupId = groupId;

      await rpc("sendReaction", params, 15000);
      return `✅ Reaction sent: ${emoji}`;
    },
  },
];

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  const args = process.argv.slice(2);
  const httpMode = args.includes("--http");
  const httpPortArg = args.find((a) => !a.startsWith("-") && !isNaN(Number(a)));
  const httpPort = httpPortArg ? parseInt(httpPortArg, 10) : 3100;

  console.error("[signal-mcp] Starting Signal MCP Server");
  console.error(`[signal-mcp]   Transport: ${httpMode ? `HTTP (port ${httpPort})` : "stdio"}`);
  console.error(`[signal-mcp]   Signal-cli: ${SIGNAL_HTTP_URL}`);
  console.error(`[signal-mcp]   Account: ${SIGNAL_ACCOUNT ? SIGNAL_ACCOUNT.slice(0, 8) + "..." : "(not set)"}`);

  // Start background SSE consumer
  startSseConsumer();

  // Verify daemon connectivity
  try {
    const resp = await fetch(`${SIGNAL_HTTP_URL}/api/v1/check`, { signal: AbortSignal.timeout(5000) });
    console.error(`[signal-mcp] signal-cli daemon: ${resp.ok ? "✅ reachable" : "⚠️ " + resp.status}`);
  } catch (err) {
    console.error(`[signal-mcp] signal-cli daemon: ⚠️ unreachable (${err})`);
  }

  const server = new Server(
    { name: "signal-mcp-server", version: "0.3.0" },
    { capabilities: { tools: {} } },
  );

  server.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: TOOLS.map((t) => ({ name: t.name, description: t.description, inputSchema: t.inputSchema })),
  }));

  server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const tool = TOOLS.find((t) => t.name === request.params.name);
    if (!tool) {
      return { content: [{ type: "text", text: `Unknown tool: ${request.params.name}` }], isError: true };
    }
    try {
      const result = await tool.handler((request.params.arguments ?? {}) as Record<string, unknown>);
      return { content: [{ type: "text", text: result }] };
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.error(`[signal-mcp] Error in tool ${tool.name}: ${msg}`);
      return { content: [{ type: "text", text: `Error: ${msg}` }], isError: true };
    }
  });

  if (httpMode) {
    // Streamable HTTP transport (Node.js)
    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: undefined, // stateless mode
      enableJsonResponse: true,
    });

    const http = await import("node:http");

    const httpServer = http.createServer(async (req, res) => {
      // CORS headers
      res.setHeader("Access-Control-Allow-Origin", "*");
      res.setHeader("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS");
      res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");

      if (req.method === "OPTIONS") {
        res.writeHead(204);
        res.end();
        return;
      }

      // Health check
      if (req.method === "GET" && req.url === "/health") {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ status: "ok" }));
        return;
      }

      await transport.handleRequest(req, res);
    });

    // Graceful shutdown
    const shutdown = () => {
      console.error("[signal-mcp] Shutting down...");
      stopSseConsumer();
      buffer.close();
      httpServer.close();
      process.exit(0);
    };
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);

    httpServer.listen(httpPort, () => {
      console.error(`[signal-mcp] HTTP server listening on http://0.0.0.0:${httpPort}`);
      console.error(`[signal-mcp]   POST / — JSON-RPC (tools/list, tools/call, etc.)`);
      console.error(`[signal-mcp]   GET  / — SSE event stream (for GET requests)`);
    });

    await server.connect(transport);
  } else {
    // Stdio transport
    const transport = new StdioServerTransport();
    await server.connect(transport);
    console.error("[signal-mcp] Server running on stdio transport");
  }
}

main().catch((err) => {
  console.error("[signal-mcp] Fatal error:", err);
  process.exit(1);
});
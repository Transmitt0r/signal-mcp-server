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
 *   SIGNAL_HTTP_URL          — signal-cli HTTP endpoint (default: http://127.0.0.1:8080)
 *   SIGNAL_ACCOUNT           — phone number for display (optional)
 *   SIGNAL_MCP_MAX_MSGS      — max messages returned per query by default (default: 500)
 *   SIGNAL_MCP_STATE_DIR     — directory for the persisted message database
 *                              (default: $XDG_STATE_HOME/signal-mcp-server or
 *                              ~/.local/state/signal-mcp-server)
 *   SIGNAL_MCP_NO_PERSIST    — set to "1"/"true" to disable disk persistence
 *                              entirely (buffer is memory-only, legacy behavior)
 *   SIGNAL_MCP_HTTP_TOKEN    — required bearer token for the HTTP transport.
 *                              Requests must send `Authorization: Bearer <token>`.
 *                              Refuses to start in --http mode without this set.
 *   SIGNAL_MCP_HTTP_HOST     — interface to bind the HTTP transport to
 *                              (default: 127.0.0.1, i.e. loopback-only)
 *
 * IMPORTANT: the signal-cli daemon this server talks to MUST be started with
 * `--receive-mode=manual` (in addition to `--http`). Messages then stay
 * queued server-side (on Signal's own infrastructure) until this process
 * pulls them via the JSON-RPC `receive` method, instead of being drained
 * unconditionally by signal-cli's default on-start background thread. See
 * README.md's "Message ingestion" section for the full rationale.
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

import { MessageBuffer, formatEnvelope, parseReceiveResult, toSafeLimit, type RpcEnvelope } from "./lib.js";

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
// Receive poller (sole ingestion path)
// ---------------------------------------------------------------------------
//
// Architecture note (see also README.md "Message ingestion" section):
//
// Earlier versions of this server ingested messages via signal-cli's SSE
// stream (`/api/v1/events`), optionally backed by a receive-log file tailer
// for durability. Both approaches shared a structural problem: signal-cli's
// default receive mode (`--receive-mode=on-start`) runs a background thread
// that drains the server-side message queue unconditionally, the instant
// the daemon starts, regardless of whether anything is listening. Once a
// message is drained this way, it is gone from Signal's servers — the only
// copies are whatever our SSE "weak listener" happened to catch while
// connected, and (in theory) whatever landed in a redirected receive-log
// file. In practice we found the log-tailer's assumption wrong for our
// deployment: signal-cli's own startup/shutdown lines went to the
// redirected file but per-message envelope logging came out on stderr
// instead, so the "durable" ingestion path never actually saw any messages
// — SSE alone was silently doing all the work, with its dropped-while-
// disconnected gap intact. Fixing this against a real log-routing quirk
// isn't a stable foundation; the class of bug can resurface with any
// signal-cli or systemd change.
//
// The actual fix is running the daemon with `--receive-mode=manual`
// (systemd ExecStart, not a code change here) so the background drain
// thread never runs. Messages then sit safely in Signal's *server-side*
// queue — genuinely undeleted, not just "buffered somewhere on this box" —
// until explicitly pulled via the JSON-RPC `receive` method, which
// signal-cli's changelog documents as being "available in JSON-RPC daemon
// mode, for polling new messages." We long-poll it in a loop below. If this
// process is down for an hour, nothing is lost: the next `receive` call
// simply returns everything that queued up meanwhile.
//
// IMPORTANT constraint (verified empirically against signal-cli 0.14.6):
// only one "receiver" may be active against a given account at a time.
// Both the SSE endpoint and a manual `receive` call count as a receiver,
// and calling `receive` while another is outstanding returns "Receive
// command cannot be used if messages are already being received." This is
// why this poller is the *only* ingestion path now — running it alongside
// an SSE consumer would just make them fight over the single-receiver slot
// (whichever connects first blocks the other, non-deterministically).
//
// Requires the signal-cli daemon to be started with `--receive-mode=manual`
// (see README.md). If it's still running in the default `on-start` mode,
// every `receive` call here will fail with that same "already being
// received" error, because the daemon's own background thread holds the
// slot. We log that condition distinctly so it's diagnosable rather than
// silently retried forever.

const RECEIVE_POLL_TIMEOUT_SECONDS = 55; // long-poll duration per request
const RECEIVE_POLL_RETRY_MS = 2000; // pause between polls on error/empty-but-fast-return

let receivePollAbort: AbortController | null = null;
let receivePollStopped = false;

function ingestReceiveResult(result: unknown): number {
  const envelopes = parseReceiveResult(result);
  let ingested = 0;
  for (const envelope of envelopes) {
    try {
      buffer.add({ params: { envelope } });
      ingested++;
    } catch (err) {
      const msg = (err as Error)?.message ?? String(err);
      console.error(
        `[signal-mcp] Failed to persist an envelope (source=${envelope.source ?? envelope.sourceNumber ?? "?"}, ` +
          `ts=${envelope.timestamp ?? "?"}): ${msg}`,
      );
    }
  }
  return ingested;
}

async function receiveOnce(): Promise<"ok" | "already-receiving" | "error"> {
  receivePollAbort = new AbortController();
  try {
    const result = await rpc(
      "receive",
      { timeout: RECEIVE_POLL_TIMEOUT_SECONDS },
      (RECEIVE_POLL_TIMEOUT_SECONDS + 15) * 1000,
      receivePollAbort.signal,
    );
    const n = ingestReceiveResult(result);
    if (n > 0) console.error(`[signal-mcp] Ingested ${n} message(s) via receive poll`);
    return "ok";
  } catch (err) {
    const msg = (err as Error)?.message ?? String(err);
    if (/already being received/i.test(msg)) {
      console.error(
        "[signal-mcp] receive poll blocked: signal-cli daemon already has an active receiver. " +
          "This means the daemon is NOT running with --receive-mode=manual (or something else is " +
          "polling it). Fix the daemon's systemd unit; see README.md.",
      );
      return "already-receiving";
    }
    console.error(`[signal-mcp] receive poll error: ${msg}`);
    return "error";
  } finally {
    receivePollAbort = null;
  }
}

async function startReceivePoller(): Promise<void> {
  console.error(
    `[signal-mcp] Starting receive poller (long-poll ${RECEIVE_POLL_TIMEOUT_SECONDS}s against ${SIGNAL_HTTP_URL}/api/v1/rpc)`,
  );
  receivePollStopped = false;
  while (!receivePollStopped) {
    const outcome = await receiveOnce();
    if (receivePollStopped) break;
    if (outcome !== "ok") {
      // Back off before retrying so a persistently misconfigured daemon
      // doesn't spin a tight error loop.
      await new Promise((r) => setTimeout(r, RECEIVE_POLL_RETRY_MS));
    }
    // On "ok" (including an empty result — a normal long-poll timeout),
    // loop immediately into the next poll; there's no gap to sleep during
    // since messages queue safely server-side.
  }
}

function stopReceivePoller(): void {
  receivePollStopped = true;
  receivePollAbort?.abort();
}

// ---------------------------------------------------------------------------
// JSON-RPC client
// ---------------------------------------------------------------------------

async function rpc(
  method: string,
  params?: Record<string, unknown>,
  timeout = 15000,
  externalSignal?: AbortSignal,
): Promise<unknown> {
  const rpcUrl = `${SIGNAL_HTTP_URL}/api/v1/rpc`;
  const payload = { jsonrpc: "2.0", method, id: method, ...(params ? { params } : {}) };

  const timeoutController = new AbortController();
  const timer = setTimeout(() => timeoutController.abort(), timeout);
  const signal = externalSignal
    ? AbortSignal.any([timeoutController.signal, externalSignal])
    : timeoutController.signal;

  try {
    const resp = await fetch(rpcUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
      signal,
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
      const limit = Math.min(toSafeLimit(args.limit, 20), 100);
      const recent = buffer.getRecent(500); // oldest-first; walk newest-first below
      const seen = new Map<string, string>();
      for (let i = recent.length - 1; i >= 0; i--) {
        const env = recent[i].params?.envelope;
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
      const limit = Math.min(toSafeLimit(args.limit, 20), 100);
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

  // Start the receive poller (sole ingestion path; see its doc comment for
  // why SSE + file-tailing were both removed). Runs in the background —
  // don't await it, it loops until stopReceivePoller() is called on
  // shutdown.
  void startReceivePoller();

  // Verify daemon connectivity
  try {
    const resp = await fetch(`${SIGNAL_HTTP_URL}/api/v1/check`, { signal: AbortSignal.timeout(5000) });
    console.error(`[signal-mcp] signal-cli daemon: ${resp.ok ? "✅ reachable" : "⚠️ " + resp.status}`);
  } catch (err) {
    console.error(`[signal-mcp] signal-cli daemon: ⚠️ unreachable (${err})`);
  }

  const server = new Server(
    { name: "signal-mcp-server", version: "0.4.0" },
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
    const httpToken = process.env.SIGNAL_MCP_HTTP_TOKEN ?? "";
    if (!httpToken) {
      console.error(
        "[signal-mcp] Refusing to start HTTP transport: SIGNAL_MCP_HTTP_TOKEN is not set. " +
          "The HTTP transport can send/read Signal messages, so it requires a bearer token. " +
          "Set SIGNAL_MCP_HTTP_TOKEN to a random secret and send it as `Authorization: Bearer <token>`.",
      );
      process.exit(1);
    }
    const httpHost = process.env.SIGNAL_MCP_HTTP_HOST ?? "127.0.0.1";

    // Streamable HTTP transport (Node.js)
    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: undefined, // stateless mode
      enableJsonResponse: true,
    });

    const http = await import("node:http");

    const httpServer = http.createServer(async (req, res) => {
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

      const authHeader = req.headers.authorization ?? "";
      if (authHeader !== `Bearer ${httpToken}`) {
        res.writeHead(401, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Unauthorized" }));
        return;
      }

      await transport.handleRequest(req, res);
    });

    // Graceful shutdown
    const shutdown = () => {
      console.error("[signal-mcp] Shutting down...");
      stopReceivePoller();
      buffer.close();
      httpServer.close();
      process.exit(0);
    };
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);

    httpServer.listen(httpPort, httpHost, () => {
      console.error(`[signal-mcp] HTTP server listening on http://${httpHost}:${httpPort}`);
      console.error(`[signal-mcp]   POST / — JSON-RPC (tools/list, tools/call, etc.)`);
      console.error(`[signal-mcp]   GET  / — SSE event stream (for GET requests)`);
    });

    await server.connect(transport);
  } else {
    // Stdio transport
    const transport = new StdioServerTransport();

    const shutdown = () => {
      console.error("[signal-mcp] Shutting down...");
      stopReceivePoller();
      buffer.close();
      process.exit(0);
    };
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);

    await server.connect(transport);
    console.error("[signal-mcp] Server running on stdio transport");
  }
}

main().catch((err) => {
  console.error("[signal-mcp] Fatal error:", err);
  process.exit(1);
});
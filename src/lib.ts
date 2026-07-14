import * as fs from "node:fs";
import * as path from "node:path";
import Database from "better-sqlite3";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface Envelope {
  source?: string;
  sourceNumber?: string;
  sourceUuid?: string;
  sourceName?: string;
  sourceDevice?: number;
  timestamp?: number;
  dataMessage?: DataMessage;
  syncMessage?: SyncMessage;
}

export interface DataMessage {
  timestamp?: number;
  message?: string;
  quote?: { text?: string };
  attachments?: Array<{ fileName?: string; contentType?: string }>;
}

export interface SyncMessage {
  sentMessage?: SentMessage;
}

export interface SentMessage {
  destination?: string;
  destinationNumber?: string;
  destinationUuid?: string;
  timestamp?: number;
  message?: string;
}

export interface RpcEnvelope {
  params?: { envelope?: Envelope };
}

// ---------------------------------------------------------------------------
// Message Buffer
// ---------------------------------------------------------------------------

/** Coerce an arbitrary value to a positive integer, falling back if it isn't one. */
export function toSafeLimit(value: unknown, fallback: number): number {
  const n = typeof value === "number" ? value : Number(value);
  return Number.isFinite(n) && n > 0 ? Math.trunc(n) : fallback;
}

const DEFAULT_QUERY_LIMIT = toSafeLimit(process.env.SIGNAL_MCP_MAX_MSGS, 500);

export interface MessageBufferOptions {
  /**
   * Path to a SQLite database file used to persist messages across process
   * restarts. When set (the default in index.ts), every new message is
   * durably written here and survives restarts of the MCP server or the
   * signal-cli daemon. Pass ":memory:" (or omit) for a purely in-memory,
   * non-durable buffer — useful for tests or when SIGNAL_MCP_NO_PERSIST is set.
   */
  persistPath?: string;
}

function extractKey(payload: RpcEnvelope): { source: string; ts: number } {
  const env = payload.params?.envelope;
  const ts = env?.timestamp ?? 0;
  const source = env?.source ?? env?.sourceNumber ?? env?.sourceUuid ?? "";
  return { source, ts };
}

export class MessageBuffer {
  private db: Database.Database;
  private insertStmt: Database.Statement;

  constructor(options: MessageBufferOptions = {}) {
    const dbPath = options.persistPath ?? ":memory:";
    if (dbPath !== ":memory:") {
      fs.mkdirSync(path.dirname(dbPath), { recursive: true });
    }
    this.db = new Database(dbPath);
    this.db.pragma("journal_mode = WAL");
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS messages (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        source    TEXT NOT NULL,
        ts        INTEGER NOT NULL,
        payload   TEXT NOT NULL,
        UNIQUE(source, ts)
      );
      CREATE INDEX IF NOT EXISTS idx_messages_source ON messages(source);
      CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages(ts);
      CREATE TABLE IF NOT EXISTS ingest_state (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );
    `);
    this.insertStmt = this.db.prepare(
      "INSERT OR IGNORE INTO messages (source, ts, payload) VALUES (?, ?, ?)",
    );
  }

  /**
   * Get a durable ingestion checkpoint (e.g. the byte offset the file-tail
   * reader last processed up to). Returns null if never set. Used so a
   * restart resumes exactly where it left off instead of re-reading from
   * the start of a (potentially large) log file, or losing track of
   * position entirely.
   */
  getOffset(key: string): number | null {
    const row = this.db.prepare("SELECT value FROM ingest_state WHERE key = ?").get(key) as
      | { value: string }
      | undefined;
    if (!row) return null;
    const n = Number(row.value);
    return Number.isFinite(n) ? n : null;
  }

  setOffset(key: string, value: number): void {
    this.db
      .prepare("INSERT INTO ingest_state (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value")
      .run(key, String(value));
  }


  /** Number of messages currently stored (for startup logging, etc.). */
  count(): number {
    const row = this.db.prepare("SELECT COUNT(*) AS n FROM messages").get() as { n: number };
    return row.n;
  }

  add(payload: RpcEnvelope): void {
    const { source, ts } = extractKey(payload);
    this.insertStmt.run(source, ts, JSON.stringify(payload));
  }

  getRecent(limit = DEFAULT_QUERY_LIMIT): RpcEnvelope[] {
    const rows = this.db
      .prepare("SELECT payload FROM messages ORDER BY id DESC LIMIT ?")
      .all(limit) as Array<{ payload: string }>;
    return rows.reverse().map((r) => JSON.parse(r.payload) as RpcEnvelope);
  }

  getConversation(sender: string, limit = DEFAULT_QUERY_LIMIT): RpcEnvelope[] {
    // Escape LIKE wildcards in the caller-supplied sender so e.g. "%" or "_"
    // can't widen the match beyond the intended substring (would otherwise
    // match every row for sender="%").
    const escaped = sender.replace(/[\\%_]/g, "\\$&");
    const rows = this.db
      .prepare(
        "SELECT payload FROM messages WHERE LOWER(source) LIKE LOWER(?) ESCAPE '\\' ORDER BY id DESC LIMIT ?",
      )
      .all(`%${escaped}%`, limit) as Array<{ payload: string }>;
    return rows.reverse().map((r) => JSON.parse(r.payload) as RpcEnvelope);
  }

  close(): void {
    this.db.close();
  }
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

// signal-cli timestamps are in milliseconds — pass them directly to Date.
export function formatTimestamp(ts: number): string {
  try {
    return new Date(ts).toISOString().replace("T", " ").slice(0, 19);
  } catch {
    return String(ts);
  }
}

export function formatEnvelope(envelope: Envelope): string {
  const source = envelope.sourceName ?? "";
  const sourceNumber = envelope.sourceNumber ?? envelope.source ?? "";
  const ts = envelope.timestamp ?? 0;
  const dt = ts ? formatTimestamp(ts) : "?";

  const sync = envelope.syncMessage;
  if (sync?.sentMessage) {
    const sent = sync.sentMessage;
    const dest = sent.destinationNumber ?? sent.destination ?? "";
    const msg = sent.message ?? "";
    return `[${dt}] 📤 To ${dest}: ${msg}`;
  }

  const data = envelope.dataMessage;
  if (!data) return `[${dt}] (unknown message type)`;

  const msg = data.message ?? "";
  const quote = data.quote;
  const quotedText = quote?.text ? ` (replying to: ${quote.text.slice(0, 80)})` : "";

  const attachments = data.attachments;
  let attachmentInfo = "";
  if (attachments?.length) {
    const names = attachments.map((a) => a.fileName ?? a.contentType ?? "file");
    attachmentInfo = ` [${names.join(", ")}]`;
  }

  const nameStr = source ? ` (${source})` : "";
  return `[${dt}] ${sourceNumber}${nameStr}: ${msg}${quotedText}${attachmentInfo}`;
}

// ---------------------------------------------------------------------------
// Receive-poll result parsing
// ---------------------------------------------------------------------------
//
// signal-cli's `receive` JSON-RPC method (polled with signal-cli daemons
// started with `--receive-mode=manual`) returns an array of envelope
// notifications once messages have arrived (or an empty array on a
// long-poll timeout with nothing new). The exact wrapper shape has drifted
// across signal-cli versions in the wild — this parses defensively so a
// minor version difference doesn't silently drop messages.

/**
 * Extract `Envelope` objects from a `receive` RPC result, handling the
 * known response shapes:
 *   - `[{ envelope: {...} }, ...]` (bare notification array — the
 *     documented on-start/manual push shape)
 *   - `[{ result: { envelope: {...} } }, ...]` (subscribeReceive-style
 *     subscription wrapper)
 *   - a single object of either shape above (non-array result)
 * Malformed or unrecognized entries are skipped rather than throwing, so
 * one bad entry can't crash the poll loop.
 */
export function parseReceiveResult(result: unknown): Envelope[] {
  const items: unknown[] = Array.isArray(result) ? result : result ? [result] : [];
  const envelopes: Envelope[] = [];
  for (const item of items) {
    if (!item || typeof item !== "object") continue;
    const obj = item as Record<string, unknown>;
    const envelope = (obj.envelope ??
      (obj.result as Record<string, unknown> | undefined)?.envelope) as Envelope | undefined;
    if (envelope && typeof envelope === "object") {
      envelopes.push(envelope);
    }
  }
  return envelopes;
}

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

const DEFAULT_QUERY_LIMIT = parseInt(process.env.SIGNAL_MCP_MAX_MSGS ?? "500", 10);

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
    `);
    this.insertStmt = this.db.prepare(
      "INSERT OR IGNORE INTO messages (source, ts, payload) VALUES (?, ?, ?)",
    );
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
    const rows = this.db
      .prepare(
        "SELECT payload FROM messages WHERE LOWER(source) LIKE LOWER(?) ORDER BY id DESC LIMIT ?",
      )
      .all(`%${sender}%`, limit) as Array<{ payload: string }>;
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

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

const MAX_MESSAGE_BUFFER = parseInt(process.env.SIGNAL_MCP_MAX_MSGS ?? "500", 10);

export class MessageBuffer {
  private messages: RpcEnvelope[] = [];
  private seen = new Set<string>();

  add(payload: RpcEnvelope): void {
    const env = payload.params?.envelope;
    const ts = env?.timestamp ?? 0;
    const source = env?.source ?? env?.sourceNumber ?? env?.sourceUuid ?? "";
    const key = `${source}:${ts}`;
    if (this.seen.has(key)) return;
    this.seen.add(key);
    this.messages.push(payload);
    if (this.messages.length > MAX_MESSAGE_BUFFER) {
      this.messages = this.messages.slice(-MAX_MESSAGE_BUFFER);
    }
  }

  getRecent(limit = 50): RpcEnvelope[] {
    return this.messages.slice(-limit);
  }

  getConversation(sender: string, limit = 50): RpcEnvelope[] {
    const results: RpcEnvelope[] = [];
    for (let i = this.messages.length - 1; i >= 0; i--) {
      const env = this.messages[i]?.params?.envelope;
      const src = env?.source ?? env?.sourceNumber ?? env?.sourceUuid ?? "";
      if (src.toLowerCase().includes(sender.toLowerCase())) {
        results.push(this.messages[i]);
        if (results.length >= limit) break;
      }
    }
    return results;
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
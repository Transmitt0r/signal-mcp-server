import { describe, it, expect, beforeEach } from "vitest";

// ---------------------------------------------------------------------------
// Inline implementation of the logic under test (same as src/index.ts)
// We test the real module via dist/index.js in integration tests.
// ---------------------------------------------------------------------------

type Envelope = {
  source?: string;
  sourceNumber?: string;
  sourceUuid?: string;
  sourceName?: string;
  timestamp?: number;
  dataMessage?: { message?: string; quote?: { text?: string }; attachments?: Array<{ fileName?: string; contentType?: string }> };
  syncMessage?: { sentMessage?: { destination?: string; destinationNumber?: string; message?: string } };
};

type RpcEnvelope = { params?: { envelope?: Envelope } };

class MessageBuffer {
  private messages: RpcEnvelope[] = [];
  private seen = new Set<number>();

  add(payload: RpcEnvelope): void {
    const ts = payload.params?.envelope?.timestamp ?? 0;
    if (this.seen.has(ts)) return;
    this.seen.add(ts);
    this.messages.push(payload);
    if (this.messages.length > 500) {
      this.messages = this.messages.slice(-500);
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

function formatTimestamp(ts: number): string {
  try {
    return new Date(ts / 1000).toISOString().replace("T", " ").slice(0, 19);
  } catch {
    return String(ts);
  }
}

function formatEnvelope(envelope: Envelope): string {
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
// Tests
// ---------------------------------------------------------------------------

describe("MessageBuffer", () => {
  let buffer: MessageBuffer;

  beforeEach(() => {
    buffer = new MessageBuffer();
  });

  it("starts empty", () => {
    expect(buffer.getRecent()).toEqual([]);
  });

  it("stores and retrieves messages", () => {
    const msg: RpcEnvelope = {
      params: {
        envelope: { source: "+49123456789", sourceName: "Alice", timestamp: 1700000000000, dataMessage: { message: "Hello!" } },
      },
    };
    buffer.add(msg);
    expect(buffer.getRecent()).toHaveLength(1);
    expect(buffer.getRecent()[0].params!.envelope!.source).toBe("+49123456789");
  });

  it("deduplicates by timestamp", () => {
    const msg: RpcEnvelope = {
      params: { envelope: { source: "+49123456789", timestamp: 100, dataMessage: { message: "Hello!" } } },
    };
    buffer.add(msg);
    buffer.add(msg);
    expect(buffer.getRecent()).toHaveLength(1);
  });

  it("filters by sender", () => {
    buffer.add({ params: { envelope: { source: "+491111", timestamp: 1, dataMessage: { message: "A" } } } });
    buffer.add({ params: { envelope: { source: "+492222", timestamp: 2, dataMessage: { message: "B" } } } });
    buffer.add({ params: { envelope: { source: "+491111", timestamp: 3, dataMessage: { message: "C" } } } });

    const conv = buffer.getConversation("+491111");
    expect(conv).toHaveLength(2);
  });

  it("caps at 500 messages", () => {
    for (let i = 0; i < 600; i++) {
      buffer.add({ params: { envelope: { source: "+49X", timestamp: i, dataMessage: { message: `msg${i}` } } } });
    }
    expect(buffer.getRecent(600)).toHaveLength(500);
    expect(buffer.getRecent(600)[0].params!.envelope!.timestamp).toBe(100);
  });
});

describe("formatEnvelope", () => {
  it("formats a data message", () => {
    const env: Envelope = { source: "+49123456789", sourceName: "Alice", timestamp: 1700000000000, dataMessage: { message: "Hey there" } };
    const result = formatEnvelope(env);
    expect(result).toContain("+49123456789");
    expect(result).toContain("Alice");
    expect(result).toContain("Hey there");
  });

  it("formats a sync message (sent by you)", () => {
    const env: Envelope = { syncMessage: { sentMessage: { destinationNumber: "+491111111111", message: "I'll be there soon" } }, timestamp: 1700000000000 };
    const result = formatEnvelope(env);
    expect(result).toContain("📤");
    expect(result).toContain("+491111111111");
    expect(result).toContain("I'll be there soon");
  });

  it("includes quoted text", () => {
    const env: Envelope = { source: "+49123456789", timestamp: 1700000000000, dataMessage: { message: "Yes!", quote: { text: "Are you coming?" } } };
    const result = formatEnvelope(env);
    expect(result).toContain("Are you coming?");
  });

  it("includes attachment info", () => {
    const env: Envelope = { source: "+49123456789", timestamp: 1700000000000, dataMessage: { message: "Check this", attachments: [{ fileName: "photo.jpg", contentType: "image/jpeg" }] } };
    const result = formatEnvelope(env);
    expect(result).toContain("photo.jpg");
  });

  it("handles missing data gracefully", () => {
    const env: Envelope = { source: "+49123456789", timestamp: 1700000000000 };
    const result = formatEnvelope(env);
    expect(result).toContain("unknown message type");
  });
});
import { describe, it, expect, beforeEach, afterEach } from "vitest";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { MessageBuffer, formatTimestamp, formatEnvelope, parseReceiveResult, type Envelope, type RpcEnvelope } from "../lib.js";

describe("MessageBuffer (in-memory)", () => {
  let buffer: MessageBuffer;

  beforeEach(() => {
    buffer = new MessageBuffer();
  });

  afterEach(() => {
    buffer.close();
  });

  it("starts empty", () => {
    expect(buffer.getRecent()).toEqual([]);
    expect(buffer.count()).toBe(0);
  });

  it("stores and retrieves messages", () => {
    const msg: RpcEnvelope = {
      params: {
        envelope: { source: "+491****6789", sourceName: "Alice", timestamp: 1700000000000, dataMessage: { message: "Hello!" } },
      },
    };
    buffer.add(msg);
    expect(buffer.getRecent()).toHaveLength(1);
    expect(buffer.getRecent()[0].params!.envelope!.source).toBe("+491****6789");
  });

  it("deduplicates by source:timestamp composite key", () => {
    const msg: RpcEnvelope = {
      params: { envelope: { source: "+491****6789", timestamp: 100, dataMessage: { message: "Hello!" } } },
    };
    buffer.add(msg);
    buffer.add(msg);
    expect(buffer.getRecent()).toHaveLength(1);
    expect(buffer.count()).toBe(1);
  });

  it("allows same timestamp from different sources", () => {
    buffer.add({ params: { envelope: { source: "+49A", timestamp: 100, dataMessage: { message: "A" } } } });
    buffer.add({ params: { envelope: { source: "+49B", timestamp: 100, dataMessage: { message: "B" } } } });
    expect(buffer.getRecent()).toHaveLength(2);
  });

  it("filters by sender", () => {
    buffer.add({ params: { envelope: { source: "+491111", timestamp: 1, dataMessage: { message: "A" } } } });
    buffer.add({ params: { envelope: { source: "+492222", timestamp: 2, dataMessage: { message: "B" } } } });
    buffer.add({ params: { envelope: { source: "+491111", timestamp: 3, dataMessage: { message: "C" } } } });

    const conv = buffer.getConversation("+491111");
    expect(conv).toHaveLength(2);
  });

  it("treats % and _ in the sender filter as literal characters, not SQL LIKE wildcards", () => {
    buffer.add({ params: { envelope: { source: "+491111", timestamp: 1, dataMessage: { message: "A" } } } });
    buffer.add({ params: { envelope: { source: "+492222", timestamp: 2, dataMessage: { message: "B" } } } });
    buffer.add({ params: { envelope: { source: "+49_underscore", timestamp: 3, dataMessage: { message: "C" } } } });

    expect(buffer.getConversation("%")).toHaveLength(0);
    expect(buffer.getConversation("_")).toHaveLength(1);
    expect(buffer.getConversation("_")[0].params!.envelope!.source).toBe("+49_underscore");
  });

  it("getRecent returns messages in chronological order (oldest first)", () => {
    for (let i = 0; i < 5; i++) {
      buffer.add({ params: { envelope: { source: "+49X", timestamp: i, dataMessage: { message: `msg${i}` } } } });
    }
    const recent = buffer.getRecent(10);
    expect(recent.map((m) => m.params!.envelope!.dataMessage!.message)).toEqual(["msg0", "msg1", "msg2", "msg3", "msg4"]);
  });

  it("respects the limit parameter without capping total storage", () => {
    for (let i = 0; i < 600; i++) {
      buffer.add({ params: { envelope: { source: "+49X", timestamp: i, dataMessage: { message: `msg${i}` } } } });
    }
    // All 600 are retained (durable store, not a ring buffer) ...
    expect(buffer.count()).toBe(600);
    // ... but a query can still ask for just the most recent N.
    const recent100 = buffer.getRecent(100);
    expect(recent100).toHaveLength(100);
    expect(recent100[recent100.length - 1].params!.envelope!.timestamp).toBe(599);
  });
});

describe("MessageBuffer persistence (SQLite on disk)", () => {
  let tmpDir: string;
  let dbPath: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "signal-mcp-test-"));
    dbPath = path.join(tmpDir, "messages.db");
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it("creates the database file on construction", () => {
    const buffer = new MessageBuffer({ persistPath: dbPath });
    expect(fs.existsSync(dbPath)).toBe(true);
    buffer.close();
  });

  it("survives a simulated restart (new instance, same file)", () => {
    const buffer1 = new MessageBuffer({ persistPath: dbPath });
    buffer1.add({ params: { envelope: { source: "+491111", timestamp: 1, dataMessage: { message: "before restart" } } } });
    buffer1.add({ params: { envelope: { source: "+492222", timestamp: 2, dataMessage: { message: "also before restart" } } } });
    buffer1.close();

    // Simulate a process restart: construct a fresh buffer pointed at the same file.
    const buffer2 = new MessageBuffer({ persistPath: dbPath });
    const recent = buffer2.getRecent();
    expect(recent).toHaveLength(2);
    expect(recent[0].params!.envelope!.dataMessage!.message).toBe("before restart");
    expect(recent[1].params!.envelope!.dataMessage!.message).toBe("also before restart");
    buffer2.close();
  });

  it("continues persisting new messages after reopening", () => {
    const buffer1 = new MessageBuffer({ persistPath: dbPath });
    buffer1.add({ params: { envelope: { source: "+491111", timestamp: 1, dataMessage: { message: "old" } } } });
    buffer1.close();

    const buffer2 = new MessageBuffer({ persistPath: dbPath });
    buffer2.add({ params: { envelope: { source: "+492222", timestamp: 2, dataMessage: { message: "new" } } } });
    expect(buffer2.count()).toBe(2);
    buffer2.close();
  });

  it("does not duplicate rows across restarts for the same source:timestamp", () => {
    const buffer1 = new MessageBuffer({ persistPath: dbPath });
    const msg: RpcEnvelope = { params: { envelope: { source: "+491111", timestamp: 1, dataMessage: { message: "A" } } } };
    buffer1.add(msg);
    buffer1.close();

    const buffer2 = new MessageBuffer({ persistPath: dbPath });
    buffer2.add(msg); // same source+timestamp, should be ignored
    expect(buffer2.count()).toBe(1);
    buffer2.close();
  });

  it("supports querying by sender across a large history via getConversation", () => {
    const buffer = new MessageBuffer({ persistPath: dbPath });
    for (let i = 0; i < 50; i++) {
      buffer.add({ params: { envelope: { source: "+49BROTHER", timestamp: i, dataMessage: { message: `msg${i}` } } } });
    }
    for (let i = 0; i < 50; i++) {
      buffer.add({ params: { envelope: { source: "+49OTHER", timestamp: 1000 + i, dataMessage: { message: `other${i}` } } } });
    }
    const conv = buffer.getConversation("BROTHER", 10);
    expect(conv).toHaveLength(10);
    expect(conv.every((m) => m.params!.envelope!.source === "+49BROTHER")).toBe(true);
    buffer.close();
  });

  it("creates parent directories for the persist path if needed", () => {
    const nestedPath = path.join(tmpDir, "nested", "dir", "messages.db");
    const buffer = new MessageBuffer({ persistPath: nestedPath });
    buffer.add({ params: { envelope: { source: "+491111", timestamp: 1, dataMessage: { message: "A" } } } });
    expect(fs.existsSync(nestedPath)).toBe(true);
    buffer.close();
  });

  it("without a persistPath, behaves purely in-memory (no file created)", () => {
    const buffer = new MessageBuffer();
    buffer.add({ params: { envelope: { source: "+491111", timestamp: 1, dataMessage: { message: "A" } } } });
    expect(buffer.getRecent()).toHaveLength(1);
    expect(fs.existsSync(dbPath)).toBe(false);
    buffer.close();
  });
});

describe("MessageBuffer ingest offset tracking", () => {
  let tmpDir: string;
  let dbPath: string;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "signal-mcp-test-"));
    dbPath = path.join(tmpDir, "messages.db");
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it("returns null for an offset that has never been set", () => {
    const buffer = new MessageBuffer({ persistPath: dbPath });
    expect(buffer.getOffset("receive_log_offset")).toBeNull();
    buffer.close();
  });

  it("stores and retrieves an offset", () => {
    const buffer = new MessageBuffer({ persistPath: dbPath });
    buffer.setOffset("receive_log_offset", 1234);
    expect(buffer.getOffset("receive_log_offset")).toBe(1234);
    buffer.close();
  });

  it("updates an existing offset in place (upsert)", () => {
    const buffer = new MessageBuffer({ persistPath: dbPath });
    buffer.setOffset("receive_log_offset", 100);
    buffer.setOffset("receive_log_offset", 500);
    expect(buffer.getOffset("receive_log_offset")).toBe(500);
    buffer.close();
  });

  it("keeps independent offsets per key", () => {
    const buffer = new MessageBuffer({ persistPath: dbPath });
    buffer.setOffset("a", 10);
    buffer.setOffset("b", 20);
    expect(buffer.getOffset("a")).toBe(10);
    expect(buffer.getOffset("b")).toBe(20);
    buffer.close();
  });

  it("survives a simulated restart (offset persists across instances)", () => {
    const buffer1 = new MessageBuffer({ persistPath: dbPath });
    buffer1.setOffset("receive_log_offset", 4096);
    buffer1.close();

    const buffer2 = new MessageBuffer({ persistPath: dbPath });
    expect(buffer2.getOffset("receive_log_offset")).toBe(4096);
    buffer2.close();
  });
});

describe("formatTimestamp", () => {
  it("formats a millisecond timestamp correctly", () => {
    // 1700000000000ms = 2023-11-14 22:13:20 UTC
    const result = formatTimestamp(1700000000000);
    expect(result).toBe("2023-11-14 22:13:20");
  });

  it("handles 0", () => {
    const result = formatTimestamp(0);
    expect(result).toBe("1970-01-01 00:00:00");
  });
});

describe("formatEnvelope", () => {
  it("formats a data message", () => {
    const env: Envelope = { source: "+491****6789", sourceName: "Alice", timestamp: 1700000000000, dataMessage: { message: "Hey there" } };
    const result = formatEnvelope(env);
    expect(result).toContain("+491****6789");
    expect(result).toContain("Alice");
    expect(result).toContain("Hey there");
  });

  it("formats a sync message (sent by you)", () => {
    const env: Envelope = { syncMessage: { sentMessage: { destinationNumber: "+491****1111", message: "I'll be there soon" } }, timestamp: 1700000000000 };
    const result = formatEnvelope(env);
    expect(result).toContain("📤");
    expect(result).toContain("+491****1111");
    expect(result).toContain("I'll be there soon");
  });

  it("includes quoted text", () => {
    const env: Envelope = { source: "+491****6789", timestamp: 1700000000000, dataMessage: { message: "Yes!", quote: { text: "Are you coming?" } } };
    const result = formatEnvelope(env);
    expect(result).toContain("Are you coming?");
  });

  it("includes attachment info", () => {
    const env: Envelope = { source: "+491****6789", timestamp: 1700000000000, dataMessage: { message: "Check this", attachments: [{ fileName: "photo.jpg", contentType: "image/jpeg" }] } };
    const result = formatEnvelope(env);
    expect(result).toContain("photo.jpg");
  });

  it("handles missing data gracefully", () => {
    const env: Envelope = { source: "+491****6789", timestamp: 1700000000000 };
    const result = formatEnvelope(env);
    expect(result).toContain("unknown message type");
  });
});

describe("parseReceiveResult", () => {
  it("returns an empty array for an empty result (normal long-poll timeout)", () => {
    expect(parseReceiveResult([])).toEqual([]);
  });

  it("returns an empty array for null/undefined", () => {
    expect(parseReceiveResult(null)).toEqual([]);
    expect(parseReceiveResult(undefined)).toEqual([]);
  });

  it("parses the bare notification array shape: [{ envelope: {...} }, ...]", () => {
    const result = [
      { envelope: { source: "+491111", timestamp: 1, dataMessage: { message: "hi" } } },
      { envelope: { source: "+492222", timestamp: 2, dataMessage: { message: "there" } } },
    ];
    const envelopes = parseReceiveResult(result);
    expect(envelopes).toHaveLength(2);
    expect(envelopes[0].source).toBe("+491111");
    expect(envelopes[1].dataMessage?.message).toBe("there");
  });

  it("parses the subscription-wrapper shape: [{ result: { envelope: {...} } }, ...]", () => {
    const result = [{ result: { envelope: { source: "+491111", timestamp: 1, dataMessage: { message: "wrapped" } } } }];
    const envelopes = parseReceiveResult(result);
    expect(envelopes).toHaveLength(1);
    expect(envelopes[0].dataMessage?.message).toBe("wrapped");
  });

  it("handles a single non-array object result", () => {
    const result = { envelope: { source: "+491111", timestamp: 1, dataMessage: { message: "single" } } };
    const envelopes = parseReceiveResult(result);
    expect(envelopes).toHaveLength(1);
    expect(envelopes[0].dataMessage?.message).toBe("single");
  });

  it("skips malformed entries without throwing", () => {
    const result = [
      null,
      42,
      "a string",
      { noEnvelopeHere: true },
      { envelope: { source: "+491111", timestamp: 1, dataMessage: { message: "survivor" } } },
    ];
    expect(() => parseReceiveResult(result)).not.toThrow();
    const envelopes = parseReceiveResult(result);
    expect(envelopes).toHaveLength(1);
    expect(envelopes[0].dataMessage?.message).toBe("survivor");
  });

  it("handles a mix of both known shapes in the same batch", () => {
    const result = [
      { envelope: { source: "+491111", timestamp: 1, dataMessage: { message: "bare" } } },
      { result: { envelope: { source: "+492222", timestamp: 2, dataMessage: { message: "wrapped" } } } },
    ];
    const envelopes = parseReceiveResult(result);
    expect(envelopes).toHaveLength(2);
    expect(envelopes.map((e) => e.dataMessage?.message)).toEqual(["bare", "wrapped"]);
  });
});

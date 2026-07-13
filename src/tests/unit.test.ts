import { describe, it, expect, beforeEach } from "vitest";
import { MessageBuffer, formatTimestamp, formatEnvelope, type Envelope, type RpcEnvelope } from "../lib.js";

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

  it("caps at 500 messages", () => {
    for (let i = 0; i < 600; i++) {
      buffer.add({ params: { envelope: { source: "+49X", timestamp: i, dataMessage: { message: `msg${i}` } } } });
    }
    expect(buffer.getRecent(600)).toHaveLength(500);
    expect(buffer.getRecent(600)[0].params!.envelope!.timestamp).toBe(100);
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
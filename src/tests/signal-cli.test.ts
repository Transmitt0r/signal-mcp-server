import { describe, it, expect, beforeAll, afterAll } from "vitest";
import { GenericContainer, StartedTestContainer } from "testcontainers";
import { spawn } from "child_process";

const SERVER_BIN = new URL("../../dist/index.js", import.meta.url).pathname;

describe("signal-mcp-server integration", () => {
  let container: StartedTestContainer | undefined;

  beforeAll(async () => {
    container = await new GenericContainer("alpine:3.19")
      .withCommand([
        "sh", "-c",
        "apk add --no-cache openjdk17-jre-headless curl >/dev/null 2>&1 && " +
        "curl -sL https://github.com/AsamK/signal-cli/releases/download/v0.13.9/signal-cli-0.13.9.tar.gz | " +
        "tar xz -C /opt && " +
        "ln -s /opt/signal-cli-0.13.9/bin/signal-cli /usr/local/bin/signal-cli && " +
        "tail -f /dev/null",
      ])
      .withStartupTimeout(120_000)
      .start();
  }, 120_000);

  afterAll(async () => {
    if (container) await container.stop();
  });

  it("MCP server starts and exposes 6 tools via stdio", async () => {
    const result = await new Promise<{ stdout: string }>((resolve, reject) => {
      const child = spawn("node", [SERVER_BIN], {
        stdio: ["pipe", "pipe", "pipe"],
        timeout: 15_000,
      });

      let stdout = "";
      child.stdout?.on("data", (data: Buffer) => {
        stdout += data.toString();
      });

      child.on("error", reject);

      // Give it time to start, then send tools/list request
      setTimeout(() => {
        child.stdin?.write(JSON.stringify({
          jsonrpc: "2.0", method: "tools/list", id: "1",
        }) + "\n");
      }, 1000);

      // After another second, kill and resolve with whatever we got
      setTimeout(() => {
        child.kill();
        resolve({ stdout });
      }, 3000);
    });

    expect(result.stdout).toContain("signal_list_contacts");
    expect(result.stdout).toContain("signal_list_groups");
    expect(result.stdout).toContain("signal_list_conversations");
    expect(result.stdout).toContain("signal_read_messages");
    expect(result.stdout).toContain("signal_send_message");
    expect(result.stdout).toContain("signal_send_reaction");

    const parsed = JSON.parse(result.stdout);
    const tools = (parsed.result as { tools?: Array<{ name: string }> })?.tools ?? [];
    expect(tools).toHaveLength(6);
  });
});
# Known Issues

## Receive poller can get stuck in "already being received" after an abort (2026-07-14)

**Status:** Open — signal MCP server disabled (`enabled: false` in
`~/.hermes/config.yaml`) pending a fix. signal-cli daemon also stopped.

**Symptom:** After the client-side `AbortController` fires on a `receive`
long-poll call (e.g. because the request exceeded the ~70s abort timeout in
`receiveOnce()`), signal-cli's daemon can get stuck reporting `"Receive
command cannot be used if messages are already being received"` on *every*
subsequent `receive` call — including fresh ones from other clients (verified
with a plain `curl` against the daemon while the poller was in this state).

**Impact:** Real inbound messages arriving during this stuck window are
drained by signal-cli's own receive thread (they show up in
`journalctl -u signal-cli.service`), logged, and then **silently lost** —
never delivered to any RPC response, so they never reach the SQLite buffer.
Confirmed reproduced live: three real messages (from Andrea Grotz and Heinz
Grotz in a group chat, 2026-07-14 14:52–14:56 UTC) arrived and were logged by
signal-cli, but never made it into `messages.db` because the poller was stuck
in this exact failure mode for several minutes around that window.

**Self-healing:** The stuck state does eventually clear on its own after
several minutes (unclear exact trigger — possibly an internal signal-cli
timeout, or the next successful poll attempt clearing whatever
server-side book-keeping was left dangling). A full `systemctl --user restart
signal-cli.service` also clears it immediately.

**Root cause (working theory, not yet confirmed against signal-cli source):**
Aborting the client HTTP request when `receiveOnce()`'s outer timeout fires
does NOT propagate a cancellation to signal-cli's own `receive` handler
server-side — the daemon still believes a receiver is registered/active even
though the client that requested it has already given up and moved on. The
next `receive` call (from the same poller, on a fresh connection) then hits
the "already being received" guard, because as far as signal-cli's internal
state is concerned, the old (abandoned) receive registration was never
released.

**Next steps to investigate:**
- Reduce or remove the client-side abort timeout wrapper entirely — let the
  signal-cli-side `params.timeout` (currently 55s) be the sole timeout
  authority, since signal-cli's own long-poll should return on its own after
  that many seconds. The `(timeout+15)*1000` outer abort in `rpc()` may be
  actively causing this by cutting off a request signal-cli hasn't finished
  processing yet.
- Check whether signal-cli exposes an explicit "cancel receive" or
  "unsubscribeReceive" call that should be invoked before abandoning an
  in-flight `receive` request, to cleanly release the server-side
  registration instead of just dropping the TCP connection.
- Consider whether `--receive-mode=manual` + `receive` RPC polling is even
  the right approach given this failure mode, versus e.g. investigating why
  the `-o json` + `StandardOutput=append:` approach's envelope logging goes
  to stderr instead of stdout on this signal-cli build (0.14.6) — fixing
  *that* routing issue might be a more robust foundation than long-polling,
  since it doesn't depend on a fragile single-receiver-slot RPC.

**To re-enable when resuming this work:**
```bash
systemctl --user start signal-cli.service   # already configured with --receive-mode=manual
hermes config set mcp_servers.signal.enabled true
```
Then re-verify with the reproduction steps above before trusting it in
production again — do NOT just flip it back on and assume the earlier fix
(commit 86f0238, "Replace SSE+file-tailer ingestion with receive-mode=manual
long-poll") is safe as-is. It closed one real gap (the stdout/stderr
receive-log routing bug and the SSE weak-listener gap) but introduced this
new one.

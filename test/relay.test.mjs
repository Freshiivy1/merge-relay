import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import http from "http";
import os from "os";
import path from "path";
import fs from "fs";
import WebSocket from "ws";
import { createRelay, deriveEventsUrl, PHASE_LOUD, PHASE_PROMPT_LIGHT } from "../lib/relay.js";
import { MergeToneDetector, LightToneDetector, PromptFingerprintMatcher } from "../lib/detectors.js";
import {
  loadAnchor, toFrames, promptLightFrames, promptOnlyFrames, lightOnlyFrames,
  loudToneFrames, silenceFrames, noiseFrames, dualTone,
} from "./audio.mjs";

const SECRET = "test-secret-not-real";

/* ------------------------------- harness ------------------------------- */

async function startHarness(overrides = {}) {
  const verdicts = [];
  const events = [];
  const app = http.createServer((req, res) => {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      const rec = {
        url: req.url,
        secret: req.headers["x-verify-secret"],
        body: body ? JSON.parse(body) : null,
      };
      if (req.url.startsWith("/stream-detected")) verdicts.push(rec);
      else if (req.url.startsWith("/stream-event")) events.push(rec);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end('{"ok":true}');
    });
  });
  await new Promise((r) => app.listen(0, "127.0.0.1", r));
  const appPort = app.address().port;

  const stateFile = path.join(os.tmpdir(), `relay-test-${process.pid}-${Math.random().toString(36).slice(2)}.json`);
  const { fingerprint } = loadAnchor();
  const relay = createRelay({
    callbackUrl: `http://127.0.0.1:${appPort}/stream-detected`,
    secret: SECRET,
    stateFile,
    fingerprint,
    heartbeatMs: 100000, // heartbeat off unless a test opts in
    callbackAttempts: 2,
    callbackRetryDelayMs: 20,
    log: () => {},
    ...overrides,
  });
  await new Promise((r) => relay.server.listen(0, "127.0.0.1", r));
  const port = relay.server.address().port;

  const clients = new Set();
  return {
    relay, app, port, appPort, verdicts, events, stateFile, clients,
    async close() {
      for (const ws of clients) { try { ws.terminate(); } catch {} }
      relay.close();
      app.close();
      for (const f of [stateFile, stateFile + ".tmp"]) { try { fs.unlinkSync(f); } catch {} }
      await new Promise((r) => setTimeout(r, 30));
    },
  };
}

function connectStream(h, { sid, token = `tok-${sid}`, leg = "legB", mode = "merge-detection", useQuery = false, customParams = true } = {}) {
  return new Promise((resolve, reject) => {
    const url = useQuery ? `ws://127.0.0.1:${h.port}/?sid=${sid}` : `ws://127.0.0.1:${h.port}/`;
    const ws = new WebSocket(url);
    h.clients.add(ws);
    ws.on("open", () => {
      ws.send(JSON.stringify({ event: "connected", protocol: "Call", version: "1.0.0" }));
      const start = { event: "start", start: { streamSid: `MZ${sid}` } };
      if (customParams) {
        start.start.customParameters = { sid, leg, mode, challengeToken: token };
      }
      ws.send(JSON.stringify(start));
      resolve(ws);
    });
    ws.on("error", reject);
  });
}

async function sendFrames(ws, frames, gapMs = 2) {
  for (const payload of frames) {
    if (ws.readyState !== WebSocket.OPEN) break;
    ws.send(JSON.stringify({ event: "media", media: { track: "inbound", chunk: "1", timestamp: "1", payload } }));
    if (gapMs) await new Promise((r) => setTimeout(r, gapMs));
  }
}

async function waitFor(fn, timeoutMs = 4000, what = "condition") {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    const v = fn();
    if (v) return v;
    await new Promise((r) => setTimeout(r, 20));
  }
  throw new Error(`timeout waiting for ${what}`);
}

function challengeStart(h, body, secret = SECRET) {
  return fetch(`http://127.0.0.1:${h.port}/challenge-start`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-verify-secret": secret },
    body: JSON.stringify(body),
  });
}

/* --------------------------- detector unit tests ----------------------- */

test("loud MergeToneDetector: unchanged characterization", () => {
  // Fires on the 6th consecutive 50 ms window (15 frames = 6 windows), not before.
  const loud = toFrames(dualTone({ amplitude: 12000, ms: 2000 }));
  const d = new MergeToneDetector();
  let firedAt = -1;
  for (let i = 0; i < 15; i++) if (d.push(loud[i])) { firedAt = i; break; }
  assert.equal(firedAt, 14, "must fire exactly when the 6th window completes");
  assert.equal(d.push(loud[15]), false, "idempotent after fire");
  // Quiet tone: same frequencies but below the 1e6 energy floor — no fire.
  const quiet = new MergeToneDetector();
  for (const f of toFrames(dualTone({ amplitude: 600, ms: 1500 }))) assert.equal(quiet.push(f), false);
  // Single frequency only — dual-frequency requirement — no fire.
  const single = new MergeToneDetector();
  for (const f of toFrames(dualTone({ amplitude: 12000, ms: 1500, freqs: [852] }))) assert.equal(single.push(f), false);
  // Silence / noise — no fire.
  const sil = new MergeToneDetector();
  for (const f of silenceFrames(800)) assert.equal(sil.push(f), false);
  const noi = new MergeToneDetector();
  for (const f of noiseFrames(800)) assert.equal(noi.push(f), false);
});

test("light detector and prompt fingerprint matcher calibration", () => {
  const { fingerprint } = loadAnchor();
  // Light watermark: quiet tone and prompt+light fire; plain noise does not.
  const l1 = new LightToneDetector({ ratioFloor: 1e-3, needWindows: 6 });
  assert.ok(lightOnlyFrames().some((f) => l1.push(f)), "quiet DTMF-8 watermark fires");
  const l2 = new LightToneDetector({ ratioFloor: 1e-3, needWindows: 6 });
  assert.ok(promptLightFrames().some((f) => l2.push(f)), "watermark under prompt fires");
  const l3 = new LightToneDetector({ ratioFloor: 1e-3, needWindows: 6 });
  assert.ok(!promptOnlyFrames().some((f) => l3.push(f)), "notched prompt does not fire light");
  const l4 = new LightToneDetector({ ratioFloor: 1e-3, needWindows: 6 });
  assert.ok(!noiseFrames(1500).some((f) => l4.push(f)), "noise does not fire light");
  // Prompt fingerprint: anchor replay matches, unrelated audio does not.
  const p1 = new PromptFingerprintMatcher(fingerprint);
  assert.ok(promptLightFrames().some((f) => p1.push(f)), "anchor replay matches fingerprint");
  const p2 = new PromptFingerprintMatcher(fingerprint);
  assert.ok(!lightOnlyFrames().some((f) => p2.push(f)), "tone alone does not match fingerprint");
  const p3 = new PromptFingerprintMatcher(fingerprint);
  assert.ok(!noiseFrames(2500).some((f) => p3.push(f)), "noise does not match fingerprint");
});

/* ------------------------------ HTTP tests ----------------------------- */

test("health / ready / stats / body limit / auth", async () => {
  const h = await startHarness();
  try {
    const health = await fetch(`http://127.0.0.1:${h.port}/health`);
    assert.equal(health.status, 200);
    assert.equal((await health.json()).ok, true);

    const ready = await fetch(`http://127.0.0.1:${h.port}/ready`);
    assert.equal(ready.status, 200);
    const readyBody = await ready.json();
    assert.equal(readyBody.ready, true);
    assert.equal(readyBody.fingerprint.durationMs, 18984);

    assert.equal((await fetch(`http://127.0.0.1:${h.port}/stats`)).status, 403);
    const stats = await fetch(`http://127.0.0.1:${h.port}/stats`, { headers: { "x-verify-secret": SECRET } });
    assert.equal(stats.status, 200);
    const statsBody = await stats.json();
    assert.ok("connections" in statsBody);
    assert.ok(!JSON.stringify(statsBody).includes("tok-"), "stats must not leak tokens");

    const noAuth = await fetch(`http://127.0.0.1:${h.port}/challenge-start`, { method: "POST", body: "{}" });
    assert.equal(noAuth.status, 403);
  } finally {
    await h.close();
  }
});

test("ready reports 503 when misconfigured", async () => {
  const relay = createRelay({ callbackUrl: "", secret: "", stateFile: path.join(os.tmpdir(), "x.json"), fingerprint: null, log: () => {} });
  await new Promise((r) => relay.server.listen(0, "127.0.0.1", r));
  const port = relay.server.address().port;
  try {
    const res = await fetch(`http://127.0.0.1:${port}/ready`);
    assert.equal(res.status, 503);
    const body = await res.json();
    assert.ok(body.reasons.length >= 2);
  } finally {
    relay.close();
  }
});

test("deriveEventsUrl replaces trailing /stream-detected", () => {
  assert.equal(deriveEventsUrl("https://app/api/verify/stream-detected"), "https://app/api/verify/stream-event");
  assert.equal(deriveEventsUrl("https://app/api/verify/stream-detected/"), "https://app/api/verify/stream-event");
  assert.equal(deriveEventsUrl(""), "");
});

/* ------------------------- stream identification ----------------------- */

test("customParameters-only identification -> stream_ready event; no query sid", async () => {
  const h = await startHarness();
  try {
    const ws = await connectStream(h, { sid: "s-cp" });
    await sendFrames(ws, silenceFrames(100));
    const ev = await waitFor(() => h.events.find((e) => e.body?.event === "stream_ready" && e.body?.sid === "s-cp"), 3000, "stream_ready event");
    assert.equal(ev.secret, SECRET);
    assert.equal(ev.body.streamSid, "MZs-cp");
    assert.ok(ev.body.at);
    ws.close();
  } finally {
    await h.close();
  }
});

test("legacy query-sid path still works (backwards-compatible test path)", async () => {
  const h = await startHarness();
  try {
    const ws = await connectStream(h, { sid: "s-legacy", useQuery: true, customParams: false });
    await sendFrames(ws, silenceFrames(100));
    await waitFor(() => h.events.find((e) => e.body?.event === "stream_ready" && e.body?.sid === "s-legacy"), 3000, "legacy stream_ready");
    ws.close();
  } finally {
    await h.close();
  }
});

test("invalid start parameters are rejected and never become ready", async () => {
  const h = await startHarness();
  try {
    const ws = await connectStream(h, { sid: "s-bad", mode: "duplex" });
    await waitFor(() => ws.readyState === WebSocket.CLOSED, 3000, "socket closed");
    await sendFrames(ws, silenceFrames(100)).catch(() => {});
    await new Promise((r) => setTimeout(r, 150));
    assert.equal(h.events.filter((e) => e.body?.sid === "s-bad").length, 0);
    assert.ok(h.relay.stats.invalidStarts >= 1);
  } finally {
    await h.close();
  }
});

/* ---------------------------- challenge-start -------------------------- */

test("challenge-start: 409 until streamReady, token mismatch 403, success starts PROMPT_LIGHT_MODE", async () => {
  const h = await startHarness();
  try {
    // Stream not ready (never connected) -> 409
    let res = await challengeStart(h, { sid: "s-ch", challengeToken: "tok-s-ch", challengeStartedAt: Date.now(), promptEndsAt: Date.now() + 30000 });
    assert.equal(res.status, 409);

    const ws = await connectStream(h, { sid: "s-ch" });
    // Connected but no media frame yet -> still not ready -> 409
    res = await challengeStart(h, { sid: "s-ch", challengeToken: "tok-s-ch", promptEndsAt: Date.now() + 30000 });
    assert.equal(res.status, 409);

    await sendFrames(ws, silenceFrames(100));
    await waitFor(() => h.events.find((e) => e.body?.event === "stream_ready"), 3000, "stream_ready");

    res = await challengeStart(h, { sid: "s-ch", challengeToken: "wrong-token", promptEndsAt: Date.now() + 30000 });
    assert.equal(res.status, 403);

    const promptEndsAt = Date.now() + 30000;
    res = await challengeStart(h, { sid: "s-ch", challengeToken: "tok-s-ch", challengeStartedAt: Date.now(), promptLightDurationMs: 30000, promptEndsAt, phaseToleranceMs: 250 });
    assert.equal(res.status, 200);
    const body = await res.json();
    assert.equal(body.phase, PHASE_PROMPT_LIGHT);
    assert.equal(body.promptEndsAt, promptEndsAt);
    ws.close();
  } finally {
    await h.close();
  }
});

test("challenge-start with expired promptEndsAt starts in LOUD_DTMF_MODE", async () => {
  const h = await startHarness();
  try {
    const ws = await connectStream(h, { sid: "s-exp" });
    await sendFrames(ws, silenceFrames(100));
    await waitFor(() => h.events.find((e) => e.body?.event === "stream_ready"), 3000, "stream_ready");
    const res = await challengeStart(h, { sid: "s-exp", challengeToken: "tok-s-exp", promptEndsAt: Date.now() - 1000, phaseToleranceMs: 100 });
    assert.equal(res.status, 200);
    assert.equal((await res.json()).phase, PHASE_LOUD);
    ws.close();
  } finally {
    await h.close();
  }
});

test("challenge-start body limit returns 413", async () => {
  const h = await startHarness({ maxBodyBytes: 256 });
  try {
    const res = await fetch(`http://127.0.0.1:${h.port}/challenge-start`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "x-verify-secret": SECRET },
      body: JSON.stringify({ sid: "x".repeat(1000) }),
    });
    assert.equal(res.status, 413);
  } finally {
    await h.close();
  }
});

/* ------------------------------ Phase 1 -------------------------------- */

async function readySession(h, sid, { promptEndsAt = Date.now() + 30000, phaseToleranceMs = 250 } = {}) {
  const ws = await connectStream(h, { sid });
  await sendFrames(ws, silenceFrames(100));
  await waitFor(() => h.events.find((e) => e.body?.event === "stream_ready" && e.body?.sid === sid), 3000, `stream_ready ${sid}`);
  const res = await challengeStart(h, {
    sid, challengeToken: `tok-${sid}`, challengeStartedAt: Date.now(),
    promptLightDurationMs: promptEndsAt - Date.now(), promptEndsAt, phaseToleranceMs,
  });
  assert.equal(res.status, 200);
  return ws;
}

test("Phase 1: prompt fingerprint + overlapping light watermark => immediate MERGE_DETECTED, exactly once", async () => {
  const h = await startHarness();
  try {
    const ws = await readySession(h, "s-p1");
    await sendFrames(ws, promptLightFrames());
    const v = await waitFor(() => h.verdicts.find((x) => x.body?.sid === "s-p1"), 4000, "MERGE_DETECTED verdict");
    assert.equal(v.secret, SECRET);
    assert.equal(v.body.result, "MERGE_DETECTED");
    assert.equal(v.body.phase, PHASE_PROMPT_LIGHT);
    assert.equal(v.body.reason, "prompt+light");
    assert.ok(v.body.detectedAt > 0);
    assert.ok(v.url.includes("sid=s-p1"), "backwards-compatible ?sid= kept");
    // Duplicate evidence must not produce a second verdict (idempotent finalize).
    await sendFrames(ws, promptLightFrames());
    await sendFrames(ws, loudToneFrames());
    await new Promise((r) => setTimeout(r, 300));
    assert.equal(h.verdicts.filter((x) => x.body?.sid === "s-p1").length, 1);
    ws.close();
  } finally {
    await h.close();
  }
});

test("Phase 1: prompt-only does not trigger; light-only does not trigger", async () => {
  const h = await startHarness();
  try {
    const ws1 = await readySession(h, "s-prompt-only");
    await sendFrames(ws1, promptOnlyFrames());
    const ws2 = await readySession(h, "s-light-only");
    await sendFrames(ws2, lightOnlyFrames());
    await new Promise((r) => setTimeout(r, 500));
    assert.equal(h.verdicts.length, 0, "neither partial signal may finalize");
    // Both sessions did accumulate their one-sided evidence internally.
    const d1 = h.relay.sessions.get("s-prompt-only").detectors;
    assert.ok(d1.promptMatchedAt > 0 && d1.lightMatchedAt === 0);
    const d2 = h.relay.sessions.get("s-light-only").detectors;
    assert.ok(d2.lightMatchedAt > 0 && d2.promptMatchedAt === 0);
    ws1.close();
    ws2.close();
  } finally {
    await h.close();
  }
});

test("boundary: partial Phase 1 evidence cleared at promptEndsAt; Phase 2 loud alone triggers", async () => {
  const h = await startHarness();
  try {
    const promptEndsAt = Date.now() + 500;
    const ws = await readySession(h, "s-boundary", { promptEndsAt, phaseToleranceMs: 100 });
    // Partial evidence: light watermark latches, prompt missing -> no verdict.
    await sendFrames(ws, lightOnlyFrames());
    await new Promise((r) => setTimeout(r, 150));
    assert.equal(h.verdicts.length, 0);
    // Cross the persisted boundary.
    await new Promise((r) => setTimeout(r, Math.max(promptEndsAt + 100 - Date.now(), 0) + 100));
    // Prompt+light audio after the boundary must NOT trigger (Phase 1 over,
    // partial evidence cleared; anchor replay is not a loud tone).
    await sendFrames(ws, promptLightFrames());
    await new Promise((r) => setTimeout(r, 300));
    assert.equal(h.verdicts.length, 0, "prompt+light after boundary must not finalize");
    assert.equal(h.relay.sessions.get("s-boundary").phase, PHASE_LOUD);
    // Loud tone alone => immediate final MERGE_DETECTED in Phase 2.
    await sendFrames(ws, loudToneFrames());
    const v = await waitFor(() => h.verdicts.find((x) => x.body?.sid === "s-boundary"), 4000, "Phase 2 verdict");
    assert.equal(v.body.result, "MERGE_DETECTED");
    assert.equal(v.body.phase, PHASE_LOUD);
    assert.equal(v.body.reason, "loud-tone");
    ws.close();
  } finally {
    await h.close();
  }
});

test("Phase 2 loud alone fires without any prior prompt evidence", async () => {
  const h = await startHarness();
  try {
    const ws = await readySession(h, "s-p2", { promptEndsAt: Date.now() - 500, phaseToleranceMs: 100 });
    assert.equal(h.relay.sessions.get("s-p2").phase, PHASE_LOUD);
    await sendFrames(ws, loudToneFrames());
    const v = await waitFor(() => h.verdicts.find((x) => x.body?.sid === "s-p2"), 4000, "Phase 2 verdict");
    assert.equal(v.body.result, "MERGE_DETECTED");
    assert.equal(v.body.phase, PHASE_LOUD);
    ws.close();
  } finally {
    await h.close();
  }
});

/* --------------------------- restart / state --------------------------- */

test("restart reconstructs expired phase as LOUD_DTMF_MODE and keeps final verdicts idempotent", async () => {
  const sid = "s-restart";
  const h = await startHarness();
  // Snapshot the state file before teardown (cleanup deletes the original).
  const stateCopy = h.stateFile + ".copy";
  let ws1;
  {
    ws1 = await readySession(h, sid, { promptEndsAt: Date.now() + 300, phaseToleranceMs: 100 });
    await new Promise((r) => setTimeout(r, 500)); // let promptEndsAt expire
    fs.copyFileSync(h.stateFile, stateCopy);
  }
  // Simulate a process restart: close the relay WITHOUT terminating the
  // stream client (a crash would not emit orderly close handlers either,
  // and no final verdict may be synthesized by the restart itself).
  h.relay.close();
  await new Promise((r) => setTimeout(r, 50));

  // "Restarted" relay over the same STATE_FILE + same fake app.
  const h2 = await startHarness({ stateFile: stateCopy });
  h2.clients.add(ws1);
  try {
    const session = h2.relay.sessions.get(sid);
    assert.ok(session, "persisted session reloaded");
    assert.equal(session.phase, PHASE_LOUD, "expired phase reconstructed as LOUD_DTMF_MODE");
    assert.equal(session.streamReady, false, "stream must reconnect before ready");

    const ws2 = await connectStream(h2, { sid });
    await sendFrames(ws2, silenceFrames(100));
    await waitFor(() => h2.events.find((e) => e.body?.event === "stream_ready" && e.body?.sid === sid), 3000, "reconnect stream_ready");
    await sendFrames(ws2, loudToneFrames());
    const v = await waitFor(() => h2.verdicts.find((x) => x.body?.sid === sid), 4000, "verdict after restart");
    assert.equal(v.body.result, "MERGE_DETECTED");
    assert.equal(v.body.phase, PHASE_LOUD);
    // Re-finalization is a no-op.
    assert.equal(h2.relay.finalize(session, "MERGE_DETECTED", "duplicate"), false);
    await new Promise((r) => setTimeout(r, 200));
    assert.equal(h2.verdicts.filter((x) => x.body?.sid === sid).length, 1);
    ws2.close();
  } finally {
    try { ws1.terminate(); } catch {}
    h.app.close();
    for (const f of [h.stateFile, h.stateFile + ".tmp"]) { try { fs.unlinkSync(f); } catch {} }
    await h2.close();
  }
});

/* --------------------------- failure semantics ------------------------- */

test("stream stop before verdict => stream_stopped event + DETECTION_INCONCLUSIVE, never success", async () => {
  const h = await startHarness();
  try {
    const ws = await readySession(h, "s-stop");
    ws.send(JSON.stringify({ event: "stop", streamSid: "MZs-stop" }));
    const ev = await waitFor(() => h.events.find((e) => e.body?.event === "stream_stopped" && e.body?.sid === "s-stop"), 3000, "stream_stopped");
    assert.equal(ev.secret, SECRET);
    const v = await waitFor(() => h.verdicts.find((x) => x.body?.sid === "s-stop"), 3000, "inconclusive verdict");
    assert.equal(v.body.result, "DETECTION_INCONCLUSIVE");
    assert.equal(v.body.reason, "stop_message");
    // Even loud tones after stop change nothing.
    await new Promise((r) => setTimeout(r, 200));
    assert.equal(h.verdicts.filter((x) => x.body?.sid === "s-stop" && x.body.result === "MERGE_DETECTED").length, 0);
  } finally {
    await h.close();
  }
});

test("socket close before verdict => DETECTION_INCONCLUSIVE", async () => {
  const h = await startHarness();
  try {
    const ws = await readySession(h, "s-close");
    ws.close();
    const v = await waitFor(() => h.verdicts.find((x) => x.body?.sid === "s-close"), 3000, "inconclusive verdict");
    assert.equal(v.body.result, "DETECTION_INCONCLUSIVE");
    assert.equal(v.body.reason, "socket_closed");
  } finally {
    await h.close();
  }
});

test("silence timeout => stream_timeout event + DETECTION_INCONCLUSIVE", async () => {
  const h = await startHarness({ silenceTimeoutMs: 300 });
  try {
    const ws = await readySession(h, "s-silence");
    // Send nothing more: no media for 300 ms.
    await waitFor(() => h.events.find((e) => e.body?.event === "stream_timeout" && e.body?.sid === "s-silence"), 3000, "stream_timeout");
    const v = await waitFor(() => h.verdicts.find((x) => x.body?.sid === "s-silence"), 3000, "timeout verdict");
    assert.equal(v.body.result, "DETECTION_INCONCLUSIVE");
    assert.equal(v.body.reason, "silence_timeout");
    ws.close();
  } finally {
    await h.close();
  }
});

test("absolute session timeout => stream_timeout + DETECTION_INCONCLUSIVE", async () => {
  const h = await startHarness({ sessionTimeoutMs: 400 });
  try {
    const ws = await connectStream(h, { sid: "s-abs" });
    await sendFrames(ws, silenceFrames(600)); // media keeps flowing past the deadline
    const v = await waitFor(() => h.verdicts.find((x) => x.body?.sid === "s-abs"), 4000, "session timeout verdict");
    assert.equal(v.body.result, "DETECTION_INCONCLUSIVE");
    assert.equal(v.body.reason, "session_timeout");
    assert.ok(h.events.find((e) => e.body?.event === "stream_timeout" && e.body?.reason === "session_timeout"));
    ws.close();
  } finally {
    await h.close();
  }
});

test("malformed message flood => DETECTION_FAILED, malformed counter increments", async () => {
  const h = await startHarness();
  try {
    const ws = await readySession(h, "s-malformed");
    for (let i = 0; i < 12; i++) ws.send("this is not json{");
    const v = await waitFor(() => h.verdicts.find((x) => x.body?.sid === "s-malformed"), 3000, "failure verdict");
    assert.equal(v.body.result, "DETECTION_FAILED");
    assert.equal(v.body.reason, "malformed_stream");
    assert.ok(h.relay.stats.malformed >= 12);
    assert.ok(h.events.find((e) => e.body?.event === "stream_error" && e.body?.sid === "s-malformed"));
  } finally {
    await h.close();
  }
});

test("detector exception => DETECTION_FAILED, never success", async () => {
  const h = await startHarness();
  try {
    const ws = await readySession(h, "s-exc");
    const session = h.relay.sessions.get("s-exc");
    // Force a detector fault: corrupt the light detector so push() throws.
    session.detectors = {
      loud: { push() { throw new Error("boom"); } },
      light: { push() { return false; } },
      prompt: null,
      promptMatchedAt: 0,
      lightMatchedAt: 0,
    };
    session.phase = PHASE_LOUD;
    await sendFrames(ws, silenceFrames(100));
    const v = await waitFor(() => h.verdicts.find((x) => x.body?.sid === "s-exc"), 3000, "failure verdict");
    assert.equal(v.body.result, "DETECTION_FAILED");
    assert.equal(v.body.reason, "detector_error");
    ws.close();
  } finally {
    await h.close();
  }
});

/* ------------------------------ heartbeat ------------------------------ */

test("heartbeat: server pings connected clients", async () => {
  const h = await startHarness({ heartbeatMs: 100 });
  try {
    const ws = await connectStream(h, { sid: "s-hb" });
    let pings = 0;
    ws.on("ping", () => pings++);
    await waitFor(() => pings >= 1, 2000, "server ping");
    ws.close();
  } finally {
    await h.close();
  }
});

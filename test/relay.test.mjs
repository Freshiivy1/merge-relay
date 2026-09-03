import { test } from "node:test";
import assert from "node:assert/strict";
import http from "http";
import os from "os";
import path from "path";
import fs from "fs";
import WebSocket from "ws";
import {
  createRelay, deriveCallbackUrl, streamToken,
  PHASE_LOUD, PHASE_PROMPT_LIGHT,
} from "../lib/relay.js";
import { MergeToneDetector, LightToneDetector, PromptFingerprintMatcher } from "../lib/detectors.js";
import {
  loadFingerprint, toFrames, promptLightFrames, promptOnlyFrames, lightOnlyFrames,
  loudToneFrames, silenceFrames, noiseFrames, dualTone,
} from "./audio.mjs";

const SECRET = "test-secret-not-real";

/* ------------------------------- harness ------------------------------- */

async function startHarness(overrides = {}) {
  const ready = [];
  const detected = [];
  const failed = [];
  const events = [];
  const failPaths = overrides.failPaths || new Set();
  const app = http.createServer((req, res) => {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      const rec = { url: req.url, secret: req.headers["x-verify-secret"], body: body ? JSON.parse(body) : null };
      if (req.url.startsWith("/api/verify/stream-ready")) ready.push(rec);
      else if (req.url.startsWith("/api/verify/stream-detected")) detected.push(rec);
      else if (req.url.startsWith("/api/verify/stream-failed")) failed.push(rec);
      else if (req.url.startsWith("/api/verify/stream-event")) events.push(rec);
      const fail = [...failPaths].some((p) => req.url.startsWith(p));
      res.writeHead(fail ? 500 : 200, { "Content-Type": "application/json" });
      res.end('{"ok":true}');
    });
  });
  await new Promise((r) => app.listen(0, "127.0.0.1", r));
  const appPort = app.address().port;

  const stateFile = path.join(os.tmpdir(), `relay-test-${process.pid}-${Math.random().toString(36).slice(2)}.json`);
  const relay = createRelay({
    callbackUrl: `http://127.0.0.1:${appPort}/api/verify/stream-detected`,
    secret: SECRET,
    stateFile,
    fingerprint: loadFingerprint(),
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
    relay, app, port, appPort, ready, detected, failed, events, stateFile, clients,
    async close() {
      for (const ws of clients) { try { ws.terminate(); } catch {} }
      relay.close();
      app.close();
      for (const f of [stateFile, stateFile + ".tmp"]) { try { fs.unlinkSync(f); } catch {} }
      await new Promise((r) => setTimeout(r, 30));
    },
  };
}

function connectStream(h, { sid, token = streamToken(SECRET, sid), leg = "legB", mode = "merge-detection", useQuery = false, customParams = true } = {}) {
  return new Promise((resolve, reject) => {
    const url = useQuery ? `ws://127.0.0.1:${h.port}/?sid=${sid}` : `ws://127.0.0.1:${h.port}/`;
    const ws = new WebSocket(url);
    h.clients.add(ws);
    ws.on("open", () => {
      ws.send(JSON.stringify({ event: "connected", protocol: "Call", version: "1.0.0" }));
      const start = { event: "start", start: { streamSid: `MZ${sid}` } };
      if (customParams) {
        start.start.customParameters = { sid, leg, mode, token };
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

async function waitFor(fn, timeoutMs = 8000, what = "condition") {
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

function arm(h, body, secret = SECRET) {
  return fetch(`http://127.0.0.1:${h.port}/arm`, {
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

test("prompt fingerprint + light watermark calibration on the real asset", () => {
  const fp = loadFingerprint();
  // Real prompt+watermark (Twilio μ-law): both Phase 1 signals fire.
  const p1 = new PromptFingerprintMatcher(fp);
  assert.ok(promptLightFrames().some((f) => p1.push(f)), "real prompt+light matches fingerprint");
  assert.ok(p1.bestScore >= 0.9, `strong match (got ${p1.bestScore})`);
  const l1 = new LightToneDetector();
  assert.ok(promptLightFrames().some((f) => l1.push(f)), "watermark under prompt fires light detector");
  // Prompt only (watermark notched out of the real asset): prompt matches, light never.
  const p2 = new PromptFingerprintMatcher(fp);
  assert.ok(promptOnlyFrames().some((f) => p2.push(f)), "notched prompt still matches fingerprint");
  const l2 = new LightToneDetector();
  assert.ok(!promptOnlyFrames().some((f) => l2.push(f)), "notched prompt never fires light detector");
  // Light watermark only: light fires, prompt never (temporal-variation guard).
  const l3 = new LightToneDetector();
  assert.ok(lightOnlyFrames().some((f) => l3.push(f)), "quiet DTMF-8 watermark fires");
  const p3 = new PromptFingerprintMatcher(fp);
  assert.ok(!lightOnlyFrames().some((f) => p3.push(f)), "pure tone never matches fingerprint");
  // Loud tone: loud detector fires; fingerprint must NOT match the tone.
  const d4 = new MergeToneDetector();
  assert.ok(loudToneFrames(2000).some((f) => d4.push(f)), "loud tone fires loud detector");
  const p4 = new PromptFingerprintMatcher(fp);
  assert.ok(!loudToneFrames(19000).some((f) => p4.push(f)), "loud tone never matches fingerprint");
  // Noise / silence: nothing fires.
  const p5 = new PromptFingerprintMatcher(fp);
  assert.ok(!noiseFrames(4000).some((f) => p5.push(f)), "noise never matches fingerprint");
  const l5 = new LightToneDetector();
  assert.ok(!noiseFrames(4000).some((f) => l5.push(f)), "noise never fires light detector");
  const p6 = new PromptFingerprintMatcher(fp);
  assert.ok(!silenceFrames(4000).some((f) => p6.push(f)), "silence never matches fingerprint");
});

test("fingerprint asset format + measured duration contract", () => {
  const fp = loadFingerprint();
  assert.equal(fp.algorithm, "normalized-log-band-spectral-contour-v1");
  assert.equal(fp.sampleRate, 8000);
  // The measured duration of the rendered prompt+watermark WAV (SPEC §6).
  assert.equal(fp.durationMs, 18840);
  assert.equal(fp.frames.length, 752);
  assert.equal(fp.bandEdgesHz.length - 1, 16);
  assert.deepEqual(fp.lightTone.frequenciesHz, [852, 1336]);
  assert.ok(fp.frames.every((row) => row.length === 16));
  // Rejects a foreign/legacy fingerprint format.
  assert.throws(() => new PromptFingerprintMatcher({ algorithm: "normalized-cross-correlation-pcm16" }));
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
    assert.equal(readyBody.fingerprint.durationMs, 18840);

    assert.equal((await fetch(`http://127.0.0.1:${h.port}/stats`)).status, 403);
    const stats = await fetch(`http://127.0.0.1:${h.port}/stats`, { headers: { "x-verify-secret": SECRET } });
    assert.equal(stats.status, 200);
    const statsBody = await stats.json();
    assert.ok("connections" in statsBody);

    const noAuth = await fetch(`http://127.0.0.1:${h.port}/challenge-start`, { method: "POST", body: "{}" });
    assert.equal(noAuth.status, 403);
    const noAuthArm = await fetch(`http://127.0.0.1:${h.port}/arm`, { method: "POST", body: "{}" });
    assert.equal(noAuthArm.status, 403);
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

test("deriveCallbackUrl replaces the trailing /stream-* segment", () => {
  assert.equal(deriveCallbackUrl("https://app/api/verify/stream-detected", "ready"), "https://app/api/verify/stream-ready");
  assert.equal(deriveCallbackUrl("https://app/api/verify/stream-detected/", "failed"), "https://app/api/verify/stream-failed");
  assert.equal(deriveCallbackUrl("", "ready"), "");
});

/* ------------------------- stream identification ----------------------- */

test("customParameters identity -> authenticated stream-ready callback", async () => {
  const h = await startHarness();
  try {
    const ws = await connectStream(h, { sid: "s-cp" });
    await sendFrames(ws, silenceFrames(100));
    const ev = await waitFor(() => h.ready.find((e) => e.body?.sid === "s-cp"), 4000, "stream-ready callback");
    assert.equal(ev.secret, SECRET);
    assert.equal(ev.body.streamSid, "MZs-cp");
    assert.ok(ev.body.readyAt > 0);
    ws.close();
  } finally {
    await h.close();
  }
});

test("stream-ready is sent exactly once per sid across reconnects", async () => {
  const h = await startHarness();
  try {
    const ws1 = await connectStream(h, { sid: "s-once" });
    await sendFrames(ws1, silenceFrames(100));
    await waitFor(() => h.ready.find((e) => e.body?.sid === "s-once"), 4000, "first stream-ready");
    // Second connection for the same sid (network blip) — no duplicate ready.
    const ws2 = await connectStream(h, { sid: "s-once" });
    await sendFrames(ws2, silenceFrames(100));
    await new Promise((r) => setTimeout(r, 300));
    assert.equal(h.ready.filter((e) => e.body?.sid === "s-once").length, 1);
    ws1.close();
    ws2.close();
  } finally {
    await h.close();
  }
});

test("query-string sid is NOT relied upon: start without customParameters is rejected", async () => {
  const h = await startHarness();
  try {
    const ws = await connectStream(h, { sid: "s-legacy", useQuery: true, customParams: false });
    await waitFor(() => ws.readyState === WebSocket.CLOSED, 4000, "socket closed");
    await sendFrames(ws, silenceFrames(100)).catch(() => {});
    await new Promise((r) => setTimeout(r, 150));
    assert.equal(h.relay.sessions.has("s-legacy"), false, "no session may be created from query sid");
    assert.equal(h.ready.length + h.detected.length + h.failed.length, 0);
    assert.ok(h.relay.stats.invalidStarts >= 1);
  } finally {
    await h.close();
  }
});

test("invalid start parameters are rejected and never become ready", async () => {
  const h = await startHarness();
  try {
    const ws = await connectStream(h, { sid: "s-bad", mode: "duplex" });
    await waitFor(() => ws.readyState === WebSocket.CLOSED, 4000, "socket closed");
    await sendFrames(ws, silenceFrames(100)).catch(() => {});
    await new Promise((r) => setTimeout(r, 150));
    assert.equal(h.ready.filter((e) => e.body?.sid === "s-bad").length, 0);
    assert.ok(h.relay.stats.invalidStarts >= 1);
  } finally {
    await h.close();
  }
});

test("stream token is validated (HMAC of sid with shared secret)", async () => {
  const h = await startHarness();
  try {
    // Wrong token -> close 4403, no session, no callbacks.
    const ws = await connectStream(h, { sid: "s-badtok", token: "bogus" });
    await waitFor(() => ws.readyState === WebSocket.CLOSED, 4000, "socket closed");
    await new Promise((r) => setTimeout(r, 150));
    assert.equal(h.relay.sessions.has("s-badtok"), false);
    // Token for a different sid -> rejected too.
    const ws2 = await connectStream(h, { sid: "s-badtok2", token: streamToken(SECRET, "other-sid") });
    await waitFor(() => ws2.readyState === WebSocket.CLOSED, 4000, "socket closed");
    // Correct token works.
    const ws3 = await connectStream(h, { sid: "s-goodtok" });
    await sendFrames(ws3, silenceFrames(100));
    await waitFor(() => h.ready.find((e) => e.body?.sid === "s-goodtok"), 4000, "stream-ready");
    ws3.close();
  } finally {
    await h.close();
  }
});

/* -------------------------------- /arm --------------------------------- */

test("/arm pre-registers the session and validates the tone pair", async () => {
  const h = await startHarness();
  try {
    let res = await arm(h, {});
    assert.equal(res.status, 400);
    res = await arm(h, { sid: "s-arm", tone: { low: 770, high: 1336 } });
    assert.equal(res.status, 400);
    res = await arm(h, {
      sid: "s-arm", legA: "CA_A", legB: "CA_B", mode: "merge-detection",
      tone: { low: 852, high: 1336 }, promptLightDurationMs: 18840, promptEndsAt: Date.now() + 60000,
    });
    assert.equal(res.status, 200);
    const body = await res.json();
    assert.equal(body.ok, true);
    assert.equal(body.streamReady, false);
    // Arming does NOT bypass readiness gating for challenge-start.
    res = await challengeStart(h, { sid: "s-arm", promptEndsAt: Date.now() + 60000 });
    assert.equal(res.status, 409);
    // Idempotent re-arm.
    res = await arm(h, { sid: "s-arm", legA: "CA_A", legB: "CA_B", mode: "merge-detection", tone: { low: 852, high: 1336 } });
    assert.equal(res.status, 200);
  } finally {
    await h.close();
  }
});

/* ---------------------------- challenge-start -------------------------- */

test("challenge-start: 409 until streamReady, success starts PROMPT_LIGHT_MODE (transitionToleranceMs)", async () => {
  const h = await startHarness();
  try {
    // Stream not ready (never connected) -> 409
    let res = await challengeStart(h, { sid: "s-ch", challengeStartedAt: Date.now(), promptEndsAt: Date.now() + 30000 });
    assert.equal(res.status, 409);

    const ws = await connectStream(h, { sid: "s-ch" });
    // Connected but no media frame yet -> still not ready -> 409
    res = await challengeStart(h, { sid: "s-ch", promptEndsAt: Date.now() + 30000 });
    assert.equal(res.status, 409);

    await sendFrames(ws, silenceFrames(100));
    await waitFor(() => h.ready.find((e) => e.body?.sid === "s-ch"), 4000, "stream-ready");

    const promptEndsAt = Date.now() + 30000;
    res = await challengeStart(h, {
      sid: "s-ch", challengeStartedAt: Date.now(),
      promptLightDurationMs: 30000, promptEndsAt, transitionToleranceMs: 300,
    });
    assert.equal(res.status, 200);
    const body = await res.json();
    assert.equal(body.phase, PHASE_PROMPT_LIGHT);
    assert.equal(body.promptEndsAt, promptEndsAt);
    assert.equal(body.transitionToleranceMs, 300);
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
    await waitFor(() => h.ready.find((e) => e.body?.sid === "s-exp"), 4000, "stream-ready");
    const res = await challengeStart(h, { sid: "s-exp", promptEndsAt: Date.now() - 1000, transitionToleranceMs: 100 });
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

async function readySession(h, sid, { promptEndsAt = Date.now() + 30000, transitionToleranceMs = 250 } = {}) {
  const ws = await connectStream(h, { sid });
  await sendFrames(ws, silenceFrames(100));
  await waitFor(() => h.ready.find((e) => e.body?.sid === sid), 4000, `stream-ready ${sid}`);
  const res = await challengeStart(h, {
    sid, challengeStartedAt: Date.now(),
    promptLightDurationMs: promptEndsAt - Date.now(), promptEndsAt, transitionToleranceMs,
  });
  assert.equal(res.status, 200);
  return ws;
}

test("Phase 1: prompt fingerprint + overlapping light watermark => immediate final MERGE_DETECTED, exactly once", async () => {
  const h = await startHarness();
  try {
    const ws = await readySession(h, "s-p1");
    await sendFrames(ws, promptLightFrames());
    const v = await waitFor(() => h.detected.find((x) => x.body?.sid === "s-p1"), 10000, "MERGE_DETECTED verdict");
    assert.equal(v.secret, SECRET);
    assert.equal(v.body.verdict, "MERGE_DETECTED");
    assert.equal(v.body.phase, "PROMPT_LIGHT");
    assert.ok(v.body.detectedAt > 0);
    assert.ok(v.body.evidence.promptScore >= 0.75);
    assert.ok(v.body.evidence.promptMatchedAt > 0 && v.body.evidence.lightMatchedAt > 0);
    assert.deepEqual(v.body.evidence.toneFrequenciesHz, [852, 1336]);
    // Duplicate evidence must not produce a second verdict (idempotent finalize).
    await sendFrames(ws, promptLightFrames());
    await sendFrames(ws, loudToneFrames());
    await new Promise((r) => setTimeout(r, 300));
    assert.equal(h.detected.filter((x) => x.body?.sid === "s-p1").length, 1);
    assert.equal(h.failed.length, 0);
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
    assert.equal(h.detected.length, 0, "neither partial signal may finalize");
    assert.equal(h.failed.length, 0);
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

test("boundary: partial Phase 1 evidence cleared at promptEndsAt+tolerance; Phase 2 loud alone triggers", async () => {
  const h = await startHarness();
  try {
    const promptEndsAt = Date.now() + 500;
    const ws = await readySession(h, "s-boundary", { promptEndsAt, transitionToleranceMs: 100 });
    // Partial evidence: light watermark latches, prompt missing -> no verdict.
    await sendFrames(ws, lightOnlyFrames(1200));
    await new Promise((r) => setTimeout(r, 150));
    assert.equal(h.detected.length, 0);
    // Cross the persisted boundary.
    await new Promise((r) => setTimeout(r, Math.max(promptEndsAt + 100 - Date.now(), 0) + 100));
    // Prompt+light audio after the boundary must NOT trigger (Phase 1 over,
    // partial evidence cleared; the prompt is not a loud tone).
    await sendFrames(ws, promptLightFrames());
    await new Promise((r) => setTimeout(r, 300));
    assert.equal(h.detected.length, 0, "prompt+light after boundary must not finalize");
    assert.equal(h.relay.sessions.get("s-boundary").phase, PHASE_LOUD);
    // Loud tone alone => immediate final MERGE_DETECTED in Phase 2.
    await sendFrames(ws, loudToneFrames());
    const v = await waitFor(() => h.detected.find((x) => x.body?.sid === "s-boundary"), 8000, "Phase 2 verdict");
    assert.equal(v.body.verdict, "MERGE_DETECTED");
    assert.equal(v.body.phase, "LOUD_DTMF");
    ws.close();
  } finally {
    await h.close();
  }
});

test("Phase 2 loud alone fires without any prior prompt evidence", async () => {
  const h = await startHarness();
  try {
    const ws = await readySession(h, "s-p2", { promptEndsAt: Date.now() - 500, transitionToleranceMs: 100 });
    assert.equal(h.relay.sessions.get("s-p2").phase, PHASE_LOUD);
    await sendFrames(ws, loudToneFrames());
    const v = await waitFor(() => h.detected.find((x) => x.body?.sid === "s-p2"), 8000, "Phase 2 verdict");
    assert.equal(v.body.verdict, "MERGE_DETECTED");
    assert.equal(v.body.phase, "LOUD_DTMF");
    ws.close();
  } finally {
    await h.close();
  }
});

/* --------------------------- restart / state --------------------------- */

test("restart reconstructs expired phase as LOUD_DTMF_MODE; verdicts and stream-ready stay idempotent", async () => {
  const sid = "s-restart";
  const h = await startHarness();
  // Snapshot the state file before teardown (cleanup deletes the original).
  const stateCopy = h.stateFile + ".copy";
  let ws1;
  {
    ws1 = await readySession(h, sid, { promptEndsAt: Date.now() + 300, transitionToleranceMs: 100 });
    await waitFor(() => h.relay.sessions.get(sid)?.readySent, 4000, "ready delivered + persisted");
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
    assert.equal(session.readySent, true, "delivered stream-ready survives restart");

    const ws2 = await connectStream(h2, { sid });
    await sendFrames(ws2, silenceFrames(100));
    await waitFor(() => h2.relay.sessions.get(sid)?.streamReady, 4000, "reconnect readiness");
    // stream-ready must NOT be re-sent for the same sid.
    await new Promise((r) => setTimeout(r, 300));
    assert.equal(h2.ready.filter((e) => e.body?.sid === sid).length, 0);
    await sendFrames(ws2, loudToneFrames());
    const v = await waitFor(() => h2.detected.find((x) => x.body?.sid === sid), 8000, "verdict after restart");
    assert.equal(v.body.verdict, "MERGE_DETECTED");
    assert.equal(v.body.phase, "LOUD_DTMF");
    // Re-finalization is a no-op.
    assert.equal(h2.relay.finalize(session, "MERGE_DETECTED", "duplicate"), false);
    await new Promise((r) => setTimeout(r, 200));
    assert.equal(h2.detected.filter((x) => x.body?.sid === sid).length, 1);
    ws2.close();
  } finally {
    try { ws1.terminate(); } catch {}
    h.app.close();
    for (const f of [h.stateFile, h.stateFile + ".tmp"]) { try { fs.unlinkSync(f); } catch {} }
    await h2.close();
  }
});

/* --------------------------- failure semantics ------------------------- */

test("stream stop before verdict => explicit DETECTION_INCONCLUSIVE on /stream-failed, never success", async () => {
  const h = await startHarness({ appEventsUrl: undefined });
  try {
    const ws = await readySession(h, "s-stop");
    ws.send(JSON.stringify({ event: "stop", streamSid: "MZs-stop" }));
    const v = await waitFor(() => h.failed.find((x) => x.body?.sid === "s-stop"), 4000, "inconclusive verdict");
    assert.equal(v.secret, SECRET);
    assert.equal(v.body.verdict, "DETECTION_INCONCLUSIVE");
    assert.equal(v.body.reason, "stop_message");
    assert.ok(v.body.failedAt > 0);
    // Even loud tones after stop change nothing.
    await new Promise((r) => setTimeout(r, 200));
    assert.equal(h.detected.filter((x) => x.body?.sid === "s-stop").length, 0);
  } finally {
    await h.close();
  }
});

test("socket close before verdict => DETECTION_INCONCLUSIVE", async () => {
  const h = await startHarness();
  try {
    const ws = await readySession(h, "s-close");
    ws.close();
    const v = await waitFor(() => h.failed.find((x) => x.body?.sid === "s-close"), 4000, "inconclusive verdict");
    assert.equal(v.body.verdict, "DETECTION_INCONCLUSIVE");
    assert.equal(v.body.reason, "socket_closed");
  } finally {
    await h.close();
  }
});

test("silence timeout => DETECTION_INCONCLUSIVE", async () => {
  const h = await startHarness({ silenceTimeoutMs: 300 });
  try {
    const ws = await readySession(h, "s-silence");
    // Send nothing more: no media for 300 ms.
    const v = await waitFor(() => h.failed.find((x) => x.body?.sid === "s-silence"), 4000, "timeout verdict");
    assert.equal(v.body.verdict, "DETECTION_INCONCLUSIVE");
    assert.equal(v.body.reason, "silence_timeout");
    ws.close();
  } finally {
    await h.close();
  }
});

test("absolute session timeout => DETECTION_INCONCLUSIVE", async () => {
  const h = await startHarness({ sessionTimeoutMs: 400 });
  try {
    const ws = await connectStream(h, { sid: "s-abs" });
    await sendFrames(ws, silenceFrames(600)); // media keeps flowing past the deadline
    const v = await waitFor(() => h.failed.find((x) => x.body?.sid === "s-abs"), 4000, "session timeout verdict");
    assert.equal(v.body.verdict, "DETECTION_INCONCLUSIVE");
    assert.equal(v.body.reason, "session_timeout");
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
    const v = await waitFor(() => h.failed.find((x) => x.body?.sid === "s-malformed"), 4000, "failure verdict");
    assert.equal(v.body.verdict, "DETECTION_FAILED");
    assert.equal(v.body.reason, "malformed_stream");
    assert.ok(h.relay.stats.malformed >= 12);
  } finally {
    await h.close();
  }
});

test("detector exception => DETECTION_FAILED, never success", async () => {
  const h = await startHarness();
  try {
    const ws = await readySession(h, "s-exc");
    const session = h.relay.sessions.get("s-exc");
    // Force a detector fault: corrupt the loud detector so push() throws.
    session.detectors = {
      loud: { push() { throw new Error("boom"); } },
      light: { push() { return false; } },
      prompt: null,
      promptMatchedAt: 0,
      lightMatchedAt: 0,
    };
    session.phase = PHASE_LOUD;
    await sendFrames(ws, silenceFrames(100));
    const v = await waitFor(() => h.failed.find((x) => x.body?.sid === "s-exc"), 4000, "failure verdict");
    assert.equal(v.body.verdict, "DETECTION_FAILED");
    assert.equal(v.body.reason, "detector_error");
    ws.close();
  } finally {
    await h.close();
  }
});

/* ------------------------- callback exhaustion ------------------------- */

test("MERGE callback exhaustion escalates to explicit inconclusive stream-failed", async () => {
  const h = await startHarness({ failPaths: new Set(["/api/verify/stream-detected"]) });
  try {
    const ws = await readySession(h, "s-exhaust", { promptEndsAt: Date.now() - 500, transitionToleranceMs: 100 });
    await sendFrames(ws, loudToneFrames());
    const v = await waitFor(() => h.failed.find((x) => x.body?.sid === "s-exhaust"), 8000, "escalated stream-failed");
    assert.equal(v.body.verdict, "DETECTION_INCONCLUSIVE");
    assert.equal(v.body.reason, "callback_exhausted");
    assert.ok(h.relay.stats.callbackExhausted >= 1);
    assert.ok(h.relay.stats.callbackRetries >= 1, "bounded retries with backoff happened");
    // Every detected attempt failed (500): exactly `callbackAttempts` tries, no success.
    assert.ok(h.detected.filter((x) => x.body?.sid === "s-exhaust").length >= 2);
    ws.close();
  } finally {
    await h.close();
  }
});

test("stream-ready callback exhaustion => explicit DETECTION_FAILED (never silent)", async () => {
  const h = await startHarness({ failPaths: new Set(["/api/verify/stream-ready"]) });
  try {
    const ws = await connectStream(h, { sid: "s-ready-fail" });
    await sendFrames(ws, silenceFrames(100));
    const v = await waitFor(() => h.failed.find((x) => x.body?.sid === "s-ready-fail"), 8000, "stream-failed verdict");
    assert.equal(v.body.verdict, "DETECTION_FAILED");
    assert.equal(v.body.reason, "stream_ready_callback_exhausted");
    assert.ok(h.relay.stats.callbackExhausted >= 1);
    ws.close();
  } finally {
    await h.close();
  }
});

/* ------------------------------ demo TwiML ----------------------------- */

test("demo TwiML endpoints are restricted by default and correct when enabled", async () => {
  const h = await startHarness();
  try {
    const res = await fetch(`http://127.0.0.1:${h.port}/twiml/legb?sid=demo1`);
    assert.equal(res.status, 404, "demo TwiML must be disabled by default");
  } finally {
    await h.close();
  }
  const h2 = await startHarness({ demoTwiml: true });
  try {
    const res = await fetch(`http://127.0.0.1:${h2.port}/twiml/legb?sid=demo1`);
    assert.equal(res.status, 200);
    const body = await res.text();
    assert.match(body, /<Start><Stream url="wss:[^"]*" track="inbound_track">/);
    assert.ok(!body.includes("?sid="), "no sid in the stream URL query string");
    assert.match(body, /<Parameter name="sid" value="demo1"\/>/);
    assert.match(body, /<Parameter name="leg" value="legB"\/>/);
    assert.match(body, /<Parameter name="mode" value="merge-detection"\/>/);
    assert.match(body, new RegExp(`<Parameter name="token" value="${streamToken(SECRET, "demo1")}"/>`));
    assert.match(body, /<Dial><Conference/);
    assert.ok(!body.includes("<Connect>") && !body.includes("<Gather>") && !body.includes("<Say>") && !body.includes("<Record>"));
  } finally {
    await h2.close();
  }
});

/* ------------------------------ heartbeat ------------------------------ */

test("heartbeat: server pings connected clients", async () => {
  const h = await startHarness({ heartbeatMs: 100 });
  try {
    const ws = await connectStream(h, { sid: "s-hb" });
    let pings = 0;
    ws.on("ping", () => pings++);
    await waitFor(() => pings >= 1, 4000, "server ping");
    ws.close();
  } finally {
    await h.close();
  }
});

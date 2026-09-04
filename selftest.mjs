// Self-test: spawn the real server.js, then drive fake Twilio Media Streams
// through the full two-phase flow against a fake app receiver:
//   session A: /arm -> stream-ready -> challenge-start -> real prompt+light
//              fixture audio -> immediate MERGE_DETECTED (PROMPT_LIGHT)
//   session B: challenge-start with an already-expired promptEndsAt ->
//              loud tone alone -> MERGE_DETECTED (LOUD_DTMF)
//   session C: prompt-only audio (watermark notched out) -> NO verdict,
//              then stop -> explicit DETECTION_INCONCLUSIVE on stream-failed
//   also asserts: customParameters-only identity with HMAC token, bad token
//   rejected, challenge-start is 409 before stream-ready, verdicts carry the
//   shared secret, /health + /ready respond, /stats requires auth, demo
//   TwiML is disabled by default.
import { spawn } from "child_process";
import http from "http";
import crypto from "crypto";
import WebSocket from "ws";
import { promptLightFrames, promptOnlyFrames, loudToneFrames, silenceFrames } from "./test/audio.mjs";

const PORT = 18099;
const APP_PORT = 19098;
const SECRET = "selftest-secret";
const STATE_FILE = `/tmp/relay-selftest-${process.pid}.json`;

const token = (sid) => crypto.createHmac("sha256", SECRET).update(`merge-relay-stream:${sid}`).digest("hex");

const ready = [];
const detected = [];
const failed = [];
const app = http.createServer((req, res) => {
  let body = "";
  req.on("data", (c) => (body += c));
  req.on("end", () => {
    const rec = { url: req.url, secret: req.headers["x-verify-secret"], body: body ? JSON.parse(body) : null };
    if (req.url.startsWith("/api/verify/stream-ready")) ready.push(rec);
    else if (req.url.startsWith("/api/verify/stream-detected")) detected.push(rec);
    else if (req.url.startsWith("/api/verify/stream-failed")) failed.push(rec);
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end('{"ok":true}');
  });
});
await new Promise((r) => app.listen(APP_PORT, "127.0.0.1", r));

const relay = spawn("node", ["server.js"], {
  env: {
    ...process.env,
    PORT: String(PORT),
    CALLBACK_URL: `http://127.0.0.1:${APP_PORT}/api/verify/stream-detected`,
    STREAM_SECRET: SECRET,
    STATE_FILE,
  },
  stdio: "pipe",
});
relay.stdout.on("data", (d) => process.stdout.write("[relay] " + d));
relay.stderr.on("data", (d) => process.stdout.write("[relay:err] " + d));
await new Promise((r) => setTimeout(r, 900));

let failures = 0;
const check = (name, ok, detail = "") => {
  console.log(`${ok ? "PASS" : "FAIL"}: ${name}${detail ? " — " + detail : ""}`);
  if (!ok) failures++;
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function waitFor(fn, what, timeoutMs = 10000) {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    const v = fn();
    if (v) return v;
    await sleep(25);
  }
  throw new Error("timeout waiting for " + what);
}

async function stream(sid, tok = token(sid)) {
  const ws = new WebSocket(`ws://127.0.0.1:${PORT}/`); // NO query sid
  await new Promise((res, rej) => { ws.on("open", res); ws.on("error", rej); });
  ws.send(JSON.stringify({ event: "connected", protocol: "Call", version: "1.0.0" }));
  ws.send(JSON.stringify({
    event: "start",
    start: { streamSid: `MZ${sid}`, customParameters: { sid, leg: "legB", mode: "merge-detection", token: tok } },
  }));
  return ws;
}
async function sendFrames(ws, frames) {
  for (const payload of frames) {
    if (ws.readyState !== WebSocket.OPEN) break;
    ws.send(JSON.stringify({ event: "media", media: { track: "inbound", payload } }));
    await sleep(2);
  }
}
const post = (path, body, secret = SECRET) =>
  fetch(`http://127.0.0.1:${PORT}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-verify-secret": secret },
    body: JSON.stringify(body),
  });

try {
  const health = await fetch(`http://127.0.0.1:${PORT}/health`);
  check("GET /health", health.status === 200);
  const readyRes = await fetch(`http://127.0.0.1:${PORT}/ready`);
  const readyBody = await readyRes.json();
  check("GET /ready", readyRes.status === 200 && readyBody.ready === true && readyBody.fingerprint?.durationMs === 21360);
  check("GET /stats protected", (await fetch(`http://127.0.0.1:${PORT}/stats`)).status === 403);
  check("demo TwiML disabled by default", (await fetch(`http://127.0.0.1:${PORT}/twiml/legb?sid=x`)).status === 404);

  // ---- /arm + bad token rejection --------------------------------------
  const armRes = await post("/arm", {
    sid: "selfA", legA: "CA_A", legB: "CA_B", mode: "merge-detection",
    tone: { low: 852, high: 1336 }, promptLightDurationMs: 60000, promptEndsAt: Date.now() + 60000,
  });
  check("POST /arm", armRes.status === 200 && (await armRes.json()).armed === true);
  const badWs = await stream("selfBad", "not-a-valid-token");
  await waitFor(() => badWs.readyState === WebSocket.CLOSED, "bad-token socket close", 5000);
  check("invalid HMAC token rejected", true);

  // ---- Session A: Phase 1 prompt+light --------------------------------
  const resA0 = await post("/challenge-start", { sid: "selfA", promptEndsAt: Date.now() + 60000 });
  check("challenge-start 409 before stream-ready", resA0.status === 409, `got ${resA0.status}`);

  const wsA = await stream("selfA");
  await sendFrames(wsA, silenceFrames(100));
  const readyA = await waitFor(() => ready.find((e) => e.body?.sid === "selfA"), "stream-ready A");
  check("stream-ready callback posted", readyA.secret === SECRET && readyA.body.streamSid === "MZselfA" && readyA.body.readyAt > 0);

  const t0 = Date.now();
  const resA = await post("/challenge-start", {
    sid: "selfA", challengeStartedAt: t0,
    promptLightDurationMs: 60000, promptEndsAt: t0 + 60000, transitionToleranceMs: 250,
  });
  check("challenge-start 200 after ready", resA.status === 200);
  check("challenge-start starts PROMPT_LIGHT_MODE", (await resA.json()).phase === "PROMPT_LIGHT_MODE");

  await sendFrames(wsA, promptLightFrames());
  const vA = await waitFor(() => detected.find((v) => v.body?.sid === "selfA"), "Phase 1 verdict", 15000);
  check(
    "Phase 1 prompt+light => MERGE_DETECTED (PROMPT_LIGHT)",
    vA.body.verdict === "MERGE_DETECTED" && vA.body.phase === "PROMPT_LIGHT" && vA.secret === SECRET && vA.body.evidence?.promptScore >= 0.75,
    `score=${vA.body.evidence?.promptScore} ~${vA.body.detectedAt - t0}ms after challenge start`,
  );
  wsA.close();

  // ---- Session B: expired prompt window => Phase 2 loud alone ---------
  const wsB = await stream("selfB");
  await sendFrames(wsB, silenceFrames(100));
  await waitFor(() => ready.find((e) => e.body?.sid === "selfB"), "stream-ready B");
  const resB = await post("/challenge-start", { sid: "selfB", promptEndsAt: Date.now() - 1000, transitionToleranceMs: 100 });
  check("expired promptEndsAt starts LOUD_DTMF_MODE", (await resB.json()).phase === "LOUD_DTMF_MODE");
  await sendFrames(wsB, loudToneFrames());
  const vB = await waitFor(() => detected.find((v) => v.body?.sid === "selfB"), "Phase 2 verdict");
  check(
    "Phase 2 loud alone => MERGE_DETECTED (LOUD_DTMF)",
    vB.body.verdict === "MERGE_DETECTED" && vB.body.phase === "LOUD_DTMF",
  );
  wsB.close();

  // ---- Session C: prompt-only never fires; stop => inconclusive -------
  const wsC = await stream("selfC");
  await sendFrames(wsC, silenceFrames(100));
  await waitFor(() => ready.find((e) => e.body?.sid === "selfC"), "stream-ready C");
  await post("/challenge-start", { sid: "selfC", challengeStartedAt: Date.now(), promptEndsAt: Date.now() + 60000 });
  await sendFrames(wsC, promptOnlyFrames());
  await sleep(500);
  check("prompt-only never fires MERGE_DETECTED", detected.filter((v) => v.body?.sid === "selfC").length === 0);
  wsC.send(JSON.stringify({ event: "stop", streamSid: "MZselfC" }));
  const vC = await waitFor(() => failed.find((v) => v.body?.sid === "selfC"), "stream-failed C");
  check(
    "stop before verdict => DETECTION_INCONCLUSIVE",
    vC.body.verdict === "DETECTION_INCONCLUSIVE" && vC.body.reason === "stop_message" && vC.secret === SECRET,
  );

  // ---- idempotency: one terminal callback per sid ----------------------
  await sleep(300);
  check(
    "exactly one terminal callback per sid",
    detected.filter((v) => v.body?.sid === "selfA").length === 1 &&
    detected.filter((v) => v.body?.sid === "selfB").length === 1 &&
    failed.filter((v) => v.body?.sid === "selfC").length === 1,
  );
} catch (err) {
  console.log("FAIL: " + err.message);
  failures++;
}

relay.kill();
app.close();
for (const f of [STATE_FILE, STATE_FILE + ".tmp"]) { try { (await import("fs")).unlinkSync(f); } catch {} }
console.log(failures ? `\n${failures} FAILURE(S)` : "\nALL SELFTEST CHECKS PASSED");
process.exitCode = failures ? 1 : 0;
setTimeout(() => process.exit(), 300);

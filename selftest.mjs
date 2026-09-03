// Self-test: spawn the real server.js, then drive a fake Twilio Media
// Stream through the full two-phase flow against a fake app receiver:
//   session A: stream_ready -> challenge-start -> prompt+light audio ->
//              immediate MERGE_DETECTED (PROMPT_LIGHT_MODE)
//   session B: challenge-start with an already-expired promptEndsAt ->
//              loud tone alone -> MERGE_DETECTED (LOUD_DTMF_MODE)
//   also asserts: challenge-start is 409 before streamReady, verdicts carry
//   the shared secret, and /health + /ready respond.
import { spawn } from "child_process";
import http from "http";
import WebSocket from "ws";
import { promptLightFrames, loudToneFrames, silenceFrames } from "./test/audio.mjs";

const PORT = 18099;
const APP_PORT = 19098;
const SECRET = "selftest-secret";
const STATE_FILE = `/tmp/relay-selftest-${process.pid}.json`;

const events = [];
const verdicts = [];
const app = http.createServer((req, res) => {
  let body = "";
  req.on("data", (c) => (body += c));
  req.on("end", () => {
    const rec = { url: req.url, secret: req.headers["x-verify-secret"], body: body ? JSON.parse(body) : null };
    if (req.url.startsWith("/stream-detected")) verdicts.push(rec);
    else if (req.url.startsWith("/stream-event")) events.push(rec);
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end('{"ok":true}');
  });
});
await new Promise((r) => app.listen(APP_PORT, "127.0.0.1", r));

const relay = spawn("node", ["server.js"], {
  env: {
    ...process.env,
    PORT: String(PORT),
    CALLBACK_URL: `http://127.0.0.1:${APP_PORT}/stream-detected`,
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
async function waitFor(fn, what, timeoutMs = 5000) {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    const v = fn();
    if (v) return v;
    await sleep(25);
  }
  throw new Error("timeout waiting for " + what);
}

async function stream(sid, token) {
  const ws = new WebSocket(`ws://127.0.0.1:${PORT}/`); // NO query sid
  await new Promise((res, rej) => { ws.on("open", res); ws.on("error", rej); });
  ws.send(JSON.stringify({ event: "connected", protocol: "Call", version: "1.0.0" }));
  ws.send(JSON.stringify({
    event: "start",
    start: { streamSid: `MZ${sid}`, customParameters: { sid, leg: "legB", mode: "merge-detection", challengeToken: token } },
  }));
  return ws;
}
async function sendFrames(ws, frames) {
  for (const payload of frames) {
    ws.send(JSON.stringify({ event: "media", media: { track: "inbound", payload } }));
    await sleep(2);
  }
}
const challenge = (body) =>
  fetch(`http://127.0.0.1:${PORT}/challenge-start`, {
    method: "POST",
    headers: { "Content-Type": "application/json", "x-verify-secret": SECRET },
    body: JSON.stringify(body),
  });

try {
  const health = await fetch(`http://127.0.0.1:${PORT}/health`);
  check("GET /health", health.status === 200);
  const ready = await fetch(`http://127.0.0.1:${PORT}/ready`);
  check("GET /ready", ready.status === 200 && (await ready.json()).ready === true);
  check("GET /stats protected", (await fetch(`http://127.0.0.1:${PORT}/stats`)).status === 403);

  // ---- Session A: Phase 1 prompt+light --------------------------------
  const resA0 = await challenge({ sid: "selfA", challengeToken: "tokA", promptEndsAt: Date.now() + 60000 });
  check("challenge-start 409 before streamReady", resA0.status === 409, `got ${resA0.status}`);

  const wsA = await stream("selfA", "tokA");
  await sendFrames(wsA, silenceFrames(100));
  await waitFor(() => events.find((e) => e.body?.event === "stream_ready" && e.body?.sid === "selfA"), "stream_ready A");
  check("stream_ready event posted", true);

  const t0 = Date.now();
  const resA = await challenge({
    sid: "selfA", challengeToken: "tokA", challengeStartedAt: t0,
    promptLightDurationMs: 60000, promptEndsAt: t0 + 60000, phaseToleranceMs: 250,
  });
  check("challenge-start 200 after ready", resA.status === 200);
  check("challenge-start starts PROMPT_LIGHT_MODE", (await resA.json()).phase === "PROMPT_LIGHT_MODE");

  await sendFrames(wsA, promptLightFrames());
  const vA = await waitFor(() => verdicts.find((v) => v.body?.sid === "selfA"), "Phase 1 verdict");
  check(
    "Phase 1 prompt+light => MERGE_DETECTED",
    vA.body.result === "MERGE_DETECTED" && vA.body.phase === "PROMPT_LIGHT_MODE" && vA.secret === SECRET,
    `${vA.body.result}/${vA.body.phase} ~${vA.body.detectedAt - t0}ms after challenge start`,
  );
  wsA.close();

  // ---- Session B: expired prompt window => Phase 2 loud alone ---------
  const wsB = await stream("selfB", "tokB");
  await sendFrames(wsB, silenceFrames(100));
  await waitFor(() => events.find((e) => e.body?.event === "stream_ready" && e.body?.sid === "selfB"), "stream_ready B");
  const resB = await challenge({ sid: "selfB", challengeToken: "tokB", promptEndsAt: Date.now() - 1000, phaseToleranceMs: 100 });
  check("expired promptEndsAt starts LOUD_DTMF_MODE", (await resB.json()).phase === "LOUD_DTMF_MODE");
  await sendFrames(wsB, loudToneFrames());
  const vB = await waitFor(() => verdicts.find((v) => v.body?.sid === "selfB"), "Phase 2 verdict");
  check(
    "Phase 2 loud alone => MERGE_DETECTED",
    vB.body.result === "MERGE_DETECTED" && vB.body.phase === "LOUD_DTMF_MODE" && vB.body.reason === "loud-tone",
  );
  wsB.close();
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

// Self-test: start the relay, connect a fake Twilio stream, push the merge
// tone as μ-law frames, and assert the callback fires.
import { spawn } from "child_process";
import http from "http";
import WebSocket from "ws";

const PORT = 18099;
let callbackHit = null;

// fake main-app callback receiver
const cb = http.createServer((req, res) => {
  if (req.method === "POST" && req.url.startsWith("/cb") && req.headers["x-verify-secret"] === "s3cret") {
    callbackHit = req.url;
    res.writeHead(200); res.end("ok");
  } else { res.writeHead(403); res.end(); }
});
await new Promise((r) => cb.listen(19098, r));

const relay = spawn("node", ["server.js"], {
  env: { ...process.env, PORT: String(PORT), CALLBACK_URL: "http://127.0.0.1:19098/cb", STREAM_SECRET: "s3cret" },
  stdio: "pipe",
});
relay.stdout.on("data", (d) => process.stdout.write("[relay] " + d));
await new Promise((r) => setTimeout(r, 800));

function encodeMulaw(s) {
  const BIAS = 0x84, CLIP = 32604;
  const sign = s < 0 ? 0x80 : 0;
  let mag = Math.min(Math.abs(Math.round(s)), CLIP) + BIAS;
  let exp = 7;
  for (let mask = 0x4000; (mag & mask) === 0 && exp > 0; mask >>= 1) exp--;
  return ~(sign | (exp << 4) | ((mag >> (exp + 3)) & 0x0f)) & 0xff;
}
function toneFrame() {
  const b = Buffer.alloc(160);
  for (let j = 0; j < 160; j++) {
    const i = frameIdx * 160 + j;
    b[j] = encodeMulaw(12000 * Math.sin(2 * Math.PI * 852 * i / 8000) + 12000 * Math.sin(2 * Math.PI * 1336 * i / 8000));
  }
  frameIdx++;
  return b.toString("base64");
}
let frameIdx = 0;

const ws = new WebSocket(`ws://127.0.0.1:${PORT}/?sid=selftest123`);
await new Promise((res, rej) => { ws.on("open", res); ws.on("error", rej); });
ws.send(JSON.stringify({ event: "connected", protocol: "Call", version: "1.0.0" }));
ws.send(JSON.stringify({ event: "start", start: { streamSid: "MZself" } }));

const t0 = Date.now();
// stream 20ms frames as fast as Twilio would over ~1s of audio
for (let f = 0; f < 50; f++) {
  ws.send(JSON.stringify({ event: "media", media: { track: "inbound", payload: toneFrame() } }));
  await new Promise((r) => setTimeout(r, 10));
}
await new Promise((r) => setTimeout(r, 600));

if (callbackHit && callbackHit.includes("sid=selftest123")) {
  console.log(`PASS: callback fired (${callbackHit}) ~${Date.now() - t0}ms after stream start`);
  process.exitCode = 0;
} else {
  console.log("FAIL: callback never fired");
  process.exitCode = 1;
}
ws.close(); relay.kill(); cb.close();
setTimeout(() => process.exit(), 300);

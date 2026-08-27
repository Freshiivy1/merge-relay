/**
 * CallVerify merge-detection relay.
 *
 * Why this exists: the main app's hosting platform blocks WebSocket upgrades,
 * and Twilio's <Gather> cannot hear in-band audio tones — so real-time
 * (<0.5s) merge detection requires a WebSocket-capable host. This tiny
 * standalone service is that host:
 *
 *   1. Leg B's TwiML opens a Twilio Media Stream to  wss://<this-host>/?sid=…
 *   2. We run a Goertzel detector for the continuous DTMF-'9' merge tone
 *      (852 Hz + 1336 Hz) over the inbound μ-law audio frames
 *   3. The instant the tone leaks across a merged call, we POST
 *      CALLBACK_URL?sid=… with the shared secret header — the main app then
 *      plays the verdict and terminates both calls
 *
 * Config (environment variables):
 *   PORT            listen port (default 8080)
 *   CALLBACK_URL    e.g. https://your-app/api/verify/stream-detected
 *   STREAM_SECRET   shared secret — must match the app's VERIFY_STREAM_SECRET
 *
 * Any Node 18+ host with WebSocket support works (Render, Railway, Fly.io,
 * a VPS, etc.). See README.md for step-by-step deployment.
 */
import http from "http";
import fs from "fs";
import { WebSocketServer } from "ws";

// Minimal .env loader (no dependency): lets platforms that can't set env
// vars via a dashboard (or where that step was skipped) run from a
// committed .env. Real environment variables always take precedence.
try {
  for (const line of fs.readFileSync(new URL("./.env", import.meta.url), "utf8").split("\n")) {
    const i = line.indexOf("=");
    if (i > 0) {
      const k = line.slice(0, i).trim();
      if (k && !(k in process.env)) process.env[k] = line.slice(i + 1).trim();
    }
  }
} catch { /* no .env — rely on real env vars */ }

const PORT = parseInt(process.env.PORT || "8080", 10);
const CALLBACK_URL = (process.env.CALLBACK_URL || "").replace(/\/+$/, "");
const SECRET = process.env.STREAM_SECRET || "";

if (!CALLBACK_URL || !SECRET) {
  console.error("FATAL: set CALLBACK_URL and STREAM_SECRET env vars");
  process.exit(1);
}

/* ------------------------------------------------------------------ */
/* DSP — μ-law decode + Goertzel (identical to the main app's, tested) */
/* ------------------------------------------------------------------ */

const SAMPLE_RATE = 8000;

function decodeMulaw(u) {
  u = ~u & 0xff;
  let t = ((u & 0x0f) << 3) + 0x84;
  t <<= (u & 0x70) >> 4;
  return u & 0x80 ? 0x84 - t : t - 0x84;
}

function goertzelPower(samples, freq) {
  const w = (2 * Math.PI * freq) / SAMPLE_RATE;
  const cw = 2 * Math.cos(w);
  let s1 = 0;
  let s2 = 0;
  for (let i = 0; i < samples.length; i++) {
    const s0 = samples[i] + cw * s1 - s2;
    s2 = s1;
    s1 = s0;
  }
  return s1 * s1 + s2 * s2 - cw * s1 * s2;
}

const WIN = 400;          // 50 ms windows
const TONE_RATIO = 0.05;  // p/(E·N²): dual-tone ≈ 0.25, noise ≈ 0.0025
const ENERGY_FLOOR = 1e6;
const NEED_WINDOWS = 6;   // 300 ms continuous tone → fire

class MergeToneDetector {
  constructor() {
    this.buf = [];
    this.streak = 0;
    this.fired = false;
  }
  /** Feed one base64 μ-law frame (20 ms); returns true exactly once on fire. */
  push(payloadB64) {
    if (this.fired) return false;
    const bytes = Buffer.from(payloadB64, "base64");
    for (const b of bytes) this.buf.push(decodeMulaw(b));
    while (this.buf.length >= WIN) {
      const window = this.buf.slice(0, WIN);
      this.buf = this.buf.slice(WIN);
      let e = 0;
      for (let i = 0; i < window.length; i++) e += window[i] * window[i];
      e /= window.length;
      const norm = e * WIN * WIN;
      const hit =
        e > ENERGY_FLOOR &&
        goertzelPower(window, 852) / norm > TONE_RATIO &&
        goertzelPower(window, 1336) / norm > TONE_RATIO;
      this.streak = hit ? this.streak + 1 : 0;
      if (this.streak >= NEED_WINDOWS) {
        this.fired = true;
        return true;
      }
    }
    return false;
  }
}

/* ------------------------------------------------------------------ */
/* Server                                                              */
/* ------------------------------------------------------------------ */

const server = http.createServer((req, res) => {
  // Health check (Render/Railway/Fly hit this)
  if (req.method === "GET" && (req.url === "/" || req.url === "/health")) {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ ok: true, service: "callverify-merge-relay" }));
    return;
  }
  res.writeHead(404);
  res.end();
});

const wss = new WebSocketServer({ server });

wss.on("connection", (ws, req) => {
  const sid = new URL(req.url, "http://localhost").searchParams.get("sid") || "";
  if (!sid) {
    ws.close();
    return;
  }
  const detector = new MergeToneDetector();
  const t0 = Date.now();
  let frames = 0;
  console.log(`[relay] stream connected sid=${sid}`);

  ws.on("message", (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch {
      return;
    }
    if (msg.event !== "media" || !msg.media?.payload) return;
    if (msg.media.track && msg.media.track !== "inbound") return;
    frames++;
    if (detector.push(msg.media.payload)) {
      const ms = Date.now() - t0;
      console.log(`[relay] MERGE TONE DETECTED sid=${sid} (${ms}ms after connect, frame ${frames})`);
      fetch(`${CALLBACK_URL}?sid=${encodeURIComponent(sid)}`, {
        method: "POST",
        headers: { "x-verify-secret": SECRET },
      }).catch((err) => console.error("[relay] callback failed:", err.message));
    }
  });

  ws.on("close", () => console.log(`[relay] stream closed sid=${sid} frames=${frames}`));
  ws.on("error", (err) => console.error(`[relay] ws error sid=${sid}:`, err.message));
});

server.listen(PORT, () => {
  console.log(`[relay] listening on :${PORT} — wss://<host>/?sid=<sessionId>`);
});

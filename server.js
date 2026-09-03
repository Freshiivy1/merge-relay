/**
 * CallVerify merge-detection relay — two-phase call architecture.
 *
 * Why this exists: the main app's hosting platform blocks WebSocket upgrades,
 * and Twilio's <Gather> cannot hear in-band audio tones — so real-time
 * merge detection requires a WebSocket-capable host. This tiny standalone
 * service is that host:
 *
 *   1. Leg B's TwiML opens a background inbound-only Twilio Media Stream
 *      (<Start><Stream track="inbound_track">) to wss://<this-host>/ and
 *      identifies itself via start.customParameters {sid, leg, mode, token}
 *      — never via the URL query string. The token is
 *      hex(HMAC-SHA256(STREAM_SECRET, "merge-relay-stream:" + sid)).
 *   2. After a valid start + first media frame the relay POSTs an
 *      authenticated stream-ready callback ({CALLBACK_BASE}/stream-ready).
 *   3. Phase 1 PROMPT_LIGHT_MODE: the prompt fingerprint (spectral contour
 *      match) AND the overlapping light 852+1336 Hz DTMF-8 watermark
 *      together finalize MERGE_DETECTED immediately.
 *   4. At the persisted promptEndsAt (+ transitionToleranceMs) the session
 *      clears partial Phase 1 evidence and switches to LOUD_DTMF_MODE, where
 *      the existing loud DTMF-8 detector ALONE finalizes MERGE_DETECTED.
 *   5. Verdicts are POSTed to {CALLBACK_BASE}/stream-detected (merge) or
 *      /stream-failed (explicit failure/inconclusive) with the shared-secret
 *      header. The relay never writes media into the Leg B stream.
 *
 * Config (environment variables; a local untracked .env is honored in dev):
 *   PORT               listen port (default 8080)
 *   CALLBACK_URL       app stream-detected endpoint,
 *                      e.g. https://your-app/api/verify/stream-detected
 *   STREAM_READY_URL   optional override (derived from CALLBACK_URL)
 *   STREAM_FAILED_URL  optional override (derived from CALLBACK_URL)
 *   APP_EVENTS_URL     optional lifecycle-event sink (not derived)
 *   STREAM_SECRET      shared secret — must match the app's VERIFY_STREAM_SECRET
 *   STATE_FILE         atomic JSON session state (default ./relay-state.json)
 *   DEMO_TWIML         set to "1" to enable the demo /twiml/* endpoints
 *   LIGHT_TONE_RATIO   Phase 1 light-watermark Goertzel floor (default 3e-3)
 *   LIGHT_NEED_WINDOWS consecutive 50 ms windows for the watermark (default 6)
 *   PROMPT_NCC_THRESHOLD  fingerprint match threshold (default from asset: 0.75)
 *   SILENCE_TIMEOUT_MS   no-media timeout (default 15000)
 *   SESSION_TIMEOUT_MS   absolute per-session timeout (default 600000)
 *   HEARTBEAT_MS         websocket ping interval (default 30000)
 *   MAX_BODY_BYTES       HTTP JSON body cap (default 65536)
 *
 * Any Node 18+ host with WebSocket support works (Render, Railway, Fly.io,
 * a VPS, etc.). See README.md for step-by-step deployment.
 */
import fs from "fs";
import { createRelay } from "./lib/relay.js";

// Minimal .env loader (no dependency): reads a LOCAL, UNTRACKED .env for
// development. Real environment variables always take precedence. Never
// commit a .env — see .env.example.
try {
  for (const line of fs.readFileSync(new URL("./.env", import.meta.url), "utf8").split("\n")) {
    const i = line.indexOf("=");
    if (i > 0) {
      const k = line.slice(0, i).trim();
      if (k && !(k in process.env)) process.env[k] = line.slice(i + 1).trim();
    }
  }
} catch { /* no local .env — rely on real env vars */ }

const PORT = parseInt(process.env.PORT || "8080", 10);

let fingerprint = null;
try {
  fingerprint = JSON.parse(fs.readFileSync(new URL("./prompt-fingerprint.json", import.meta.url), "utf8"));
  console.log(`[relay] prompt fingerprint loaded: ${fingerprint.sourceAsset || fingerprint.asset} (${fingerprint.durationMs} ms)`);
} catch (err) {
  console.error("[relay] prompt-fingerprint.json missing/invalid — Phase 1 fingerprinting disabled:", err.message);
}

const relay = createRelay({
  callbackUrl: process.env.CALLBACK_URL || "",
  streamReadyUrl: process.env.STREAM_READY_URL || "",
  streamFailedUrl: process.env.STREAM_FAILED_URL || "",
  appEventsUrl: process.env.APP_EVENTS_URL || "",
  secret: process.env.STREAM_SECRET || "",
  stateFile: process.env.STATE_FILE || new URL("./relay-state.json", import.meta.url).pathname,
  fingerprint,
  demoTwiml: process.env.DEMO_TWIML === "1",
  lightRatioFloor: process.env.LIGHT_TONE_RATIO ? Number(process.env.LIGHT_TONE_RATIO) : undefined,
  lightNeedWindows: process.env.LIGHT_NEED_WINDOWS ? Number(process.env.LIGHT_NEED_WINDOWS) : undefined,
  promptThreshold: process.env.PROMPT_NCC_THRESHOLD ? Number(process.env.PROMPT_NCC_THRESHOLD) : undefined,
  silenceTimeoutMs: process.env.SILENCE_TIMEOUT_MS ? Number(process.env.SILENCE_TIMEOUT_MS) : undefined,
  sessionTimeoutMs: process.env.SESSION_TIMEOUT_MS ? Number(process.env.SESSION_TIMEOUT_MS) : undefined,
  heartbeatMs: process.env.HEARTBEAT_MS ? Number(process.env.HEARTBEAT_MS) : undefined,
  maxBodyBytes: process.env.MAX_BODY_BYTES ? Number(process.env.MAX_BODY_BYTES) : undefined,
});

process.on("uncaughtException", (err) => {
  relay.stats.uncaught++;
  relay.stats.lastError = "uncaught: " + err.message;
  console.error("[relay] uncaughtException:", err);
});

if (!process.env.CALLBACK_URL || !process.env.STREAM_SECRET) {
  // The process still serves /health and /ready (which reports 503) so the
  // hosting platform surfaces the misconfiguration instead of crash-looping.
  console.error("[relay] WARNING: CALLBACK_URL and STREAM_SECRET env vars are required — relay is NOT ready");
}

relay.server.listen(PORT, () => {
  console.log(`[relay] listening on :${PORT} — wss://<host>/ (customParameters identification)`);
});

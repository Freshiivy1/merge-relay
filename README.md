# CallVerify Merge-Detection Relay

Standalone WebSocket service that gives the CallVerify verification engine
**sub-second, two-phase merge detection**. The main app's hosting platform
blocks WebSockets, so Twilio's live Leg B audio stream terminates here.

## Two-phase detection (binding contract)

Leg B is the live call between the browser caller and the callee. Its TwiML
opens a **background, inbound-only** Twilio Media Stream
(`<Start><Stream track="inbound_track">`, never blocking `<Connect><Stream>`)
and immediately continues to `<Dial><Conference>`. The stream identifies
itself **only** through `start.customParameters`:

| Parameter | Value |
|---|---|
| `sid` | verification session ID |
| `leg` | `legB` |
| `mode` | `merge-detection` |
| `challengeToken` | per-session random token |

No session ID travels in the WebSocket URL query string (a legacy `?sid=`
path remains only as a backwards-compatible test path).

The relay runs **exactly one active phase** per session:

1. **`PROMPT_LIGHT_MODE`** — set by `POST /challenge-start`. A DSP prompt
   fingerprint (normalized cross-correlation of the PCM stream against the
   committed anchor in `prompt-fingerprint.json`) **and** an overlapping
   light 852+1336 Hz DTMF-8 watermark together finalize **`MERGE_DETECTED`
   immediately**. This is a final verdict, not a candidate.
2. At the persisted `promptEndsAt` (+ `phaseToleranceMs`) all partial
   Phase 1 evidence is cleared and the session switches to
   **`LOUD_DTMF_MODE`**.
3. **`LOUD_DTMF_MODE`** — the existing loud-tone Goertzel detector (same
   852+1336 Hz DTMF-8 pair, energy floor 1e6, dual-frequency requirement,
   six consecutive 50 ms windows, idempotent fire) **alone** finalizes
   **`MERGE_DETECTED` immediately**.

All three signals are never required; each phase decides independently.

The relay **never writes media into the Leg B stream** and has no duplex
`<Connect><Stream>` path. Verdicts reach the app only through the
authenticated callback.

## App integration

- **Verdict callback** → `POST {CALLBACK_URL}` (structured JSON body, plus
  legacy `?sid=`):
  `{ "sid", "result": "MERGE_DETECTED" | "DETECTION_FAILED" | "DETECTION_INCONCLUSIVE", "phase", "reason", "detectedAt" }`
- **Stream lifecycle events** → `POST {APP_EVENTS_URL}` (derived from
  `CALLBACK_URL` by replacing trailing `/stream-detected` with
  `/stream-event` when unset):
  `{ "sid", "event": "stream_ready" | "stream_stopped" | "stream_error" | "stream_timeout", "streamSid", "reason", "at" }`
- **Challenge start** → `POST /challenge-start` with
  `x-verify-secret: <STREAM_SECRET>` and
  `{ "sid", "challengeToken", "challengeStartedAt", "promptLightDurationMs", "promptEndsAt", "phaseToleranceMs" }`.
  Returns **409** until the stream is `streamReady` (valid start + first
  inbound media frame), **403** on secret/token mismatch, **200** with the
  active phase on success.

All callbacks carry the `x-verify-secret: <STREAM_SECRET>` header. Verdict
callbacks are retried with backoff; stream stop/error/timeout or detector
exceptions before a final verdict produce explicit
`DETECTION_FAILED`/`DETECTION_INCONCLUSIVE` — never a pass.

## Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /health` | liveness — `{"ok":true}` |
| `GET /ready` | readiness — checks config, fingerprint asset, STATE_FILE access |
| `GET /stats` | diagnostics (requires `x-verify-secret`; tokens are masked) |
| `POST /challenge-start` | arm/refresh the two-phase challenge (see above; bodies capped by `MAX_BODY_BYTES`) |
| `GET /twiml/legb?sid=…&token=…` | demo Leg B TwiML (`<Start><Stream track="inbound_track">` with nested parameters, no query sid in the stream URL, then `<Dial><Conference>`) |
| `GET /twiml/hold` · `/twiml/verdict` | demo TwiML used by tests |

## State persistence

Session state (sid, token, phase, `challengeStartedAt`, `promptEndsAt`,
stream readiness, final verdict) is written atomically to `STATE_FILE`
(tmp write + rename) on every transition. On boot the state is reloaded:
expired prompt windows are reconstructed as `LOUD_DTMF_MODE`, finalized
verdicts stay finalized (idempotent across restarts), and streams must
reconnect and re-identify before they are ready again.

Reliability: WebSocket heartbeat ping/pong, malformed-message counters,
10 s identification timeout, `SILENCE_TIMEOUT_MS` no-media timeout, and an
absolute `SESSION_TIMEOUT_MS` per session.

## Deploy (Render — what we use)

1. Push this repo to GitHub
2. render.com → New → Web Service → pick the repo (a `render.yaml`
   blueprint is included)
3. Build: `npm install` · Start: `npm start` · Health check: `/health`
4. Set the env vars below (never commit them — see `.env.example`)

Railway / Fly.io / a VPS work identically (any Node 18+ host with WSS).

## Environment variables

| Var | Required | Purpose |
|---|---|---|
| `CALLBACK_URL` | yes | app verdict endpoint, e.g. `https://<app>/api/verify/stream-detected` |
| `STREAM_SECRET` | yes | shared secret — must equal the app's `VERIFY_STREAM_SECRET` |
| `APP_EVENTS_URL` | no | stream-event endpoint (derived from `CALLBACK_URL` when unset) |
| `STATE_FILE` | no | atomic session state file (default `./relay-state.json`) |
| `LIGHT_TONE_RATIO` | no | Phase 1 light-watermark Goertzel floor (default `1e-3`) |
| `LIGHT_NEED_WINDOWS` | no | consecutive 50 ms windows for the watermark (default `6`) |
| `PROMPT_NCC_THRESHOLD` | no | fingerprint NCC threshold (default from asset: `0.5`) |
| `SILENCE_TIMEOUT_MS` | no | no-media timeout (default `15000`) |
| `SESSION_TIMEOUT_MS` | no | absolute session timeout (default `600000`) |
| `HEARTBEAT_MS` | no | WebSocket ping interval (default `30000`) |
| `MAX_BODY_BYTES` | no | HTTP JSON body cap (default `65536`) |
| `PORT` | platform | listen port (default `8080`) |

For local development, copy `.env.example` to an **untracked** `.env`
(`git status` must never show it). Real environment variables always win.
Generate a fresh random secret — do not reuse any previously committed one.

Then configure the main app:

| Var | Value |
|---|---|
| `VERIFY_STREAM_URL` | `wss://<relay-host>` (bare base URL — no `?sid=`; parameters are nested) |
| `VERIFY_STREAM_SECRET` | same value as `STREAM_SECRET` |

## Verify it works

```bash
curl https://<relay-host>/health   # {"ok":true,...}
curl https://<relay-host>/ready    # {"ready":true,...}
npm test                           # full self-test suite (no Twilio needed)
```

## Files

- `server.js` — entry point (env config, `.env` loader, asset loading)
- `lib/relay.js` — HTTP endpoints, WebSocket lifecycle, two-phase state machine, persistence, callbacks
- `lib/detectors.js` — loud `MergeToneDetector` (unchanged), `LightToneDetector`, `PromptFingerprintMatcher`
- `lib/dsp.js` — μ-law decode + Goertzel primitives
- `lib/state.js` — atomic JSON state load/save
- `prompt-fingerprint.json` — normalized-cross-correlation PCM anchor + light-Goertzel calibration for the 18.984 s prompt+watermark asset
- `test/` — unit + integration suite (`node --test`)
- `selftest.mjs` — end-to-end smoke: spawns the real server, drives both phases

# CallVerify Merge-Detection Relay

Standalone WebSocket service that gives the CallVerify verification engine
**two-phase merge detection** on live calls. The main app's hosting platform
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
| `token` | `hex(HMAC-SHA256(STREAM_SECRET, "merge-relay-stream:" + sid))` |

The token is validated statelessly against the shared secret
(`STREAM_SECRET`, constant-time compare). Streams with a missing/invalid
parameter set or token are closed (`4400`/`4403`). **No session ID is ever
read from the WebSocket URL query string.**

The relay runs **exactly one active phase** per session:

1. **`PROMPT_LIGHT_MODE`** — set by `POST /challenge-start`. A prompt
   fingerprint match (telephony-tolerant spectral contour: 16 log-spaced
   bands 300–3400 Hz, per-frame Pearson over a 2 s sliding window, energy
   gate + temporal-variation guard) **and** an overlapping light 852+1336 Hz
   DTMF-8 watermark together finalize **`MERGE_DETECTED` immediately**. This
   is a final verdict, not a candidate. Prompt alone or light tone alone
   never fire.
2. At the persisted `promptEndsAt` (+ `transitionToleranceMs`) all partial
   Phase 1 evidence is cleared and the session switches to
   **`LOUD_DTMF_MODE`**.
3. **`LOUD_DTMF_MODE`** — the existing loud-tone Goertzel detector (same
   852+1336 Hz DTMF-8 pair, energy floor 1e6, dual-frequency requirement,
   six consecutive 50 ms windows, idempotent fire) **alone** finalizes
   **`MERGE_DETECTED` immediately**.

All three signals are never required; each phase decides independently.

The relay **never writes media into the Leg B stream** and has no duplex
`<Connect><Stream>` path. Verdicts reach the app only through the
authenticated callbacks below.

## App integration

All callbacks are `POST` JSON with header `x-verify-secret: <STREAM_SECRET>`,
bounded retries with backoff, and idempotency per sid via persisted terminal
state (exactly one terminal callback per session, ever — across reconnects
and restarts).

| Callback | Body |
|---|---|
| `POST {base}/stream-ready` | `{ "sid", "streamSid", "readyAt" }` — sent once after a valid start + first inbound media frame |
| `POST {base}/stream-detected` | `{ "sid", "verdict": "MERGE_DETECTED", "phase": "PROMPT_LIGHT" \| "LOUD_DTMF", "detectedAt", "evidence" }` |
| `POST {base}/stream-failed` | `{ "sid", "verdict": "DETECTION_FAILED" \| "DETECTION_INCONCLUSIVE", "reason", "failedAt" }` |

`{base}` is `CALLBACK_URL` with the trailing `/stream-detected` replaced
(override with `STREAM_READY_URL` / `STREAM_FAILED_URL`).

Failure policy — **never a silent pass**: stream stop/error/timeout before a
verdict, malformed-stream floods, detector exceptions, an undeliverable
`stream-ready`, or an exhausted terminal callback all produce an explicit
`DETECTION_FAILED`/`DETECTION_INCONCLUSIVE`. If delivering `MERGE_DETECTED`
exhausts retries, a best-effort `stream-failed` (`callback_exhausted`) is
sent so a merge is never stranded as a silent pass.

Inbound control endpoints (header `x-verify-secret`):

- **`POST /arm`** — pre-register a session before the legs start:
  `{ "sid", "legA", "legB", "mode", "tone": { "low": 852, "high": 1336 },
  "promptLightDurationMs", "promptEndsAt" }`. Rejects unknown tone pairs
  (`400`), finalized sessions (`409`). Non-2xx must be treated as an arm
  failure by the caller.
- **`POST /challenge-start`** — `{ "sid", "challengeStartedAt",
  "promptLightDurationMs", "promptEndsAt", "transitionToleranceMs" }`.
  Returns **409** until the stream is ready (valid start + first inbound
  media frame), **403** on secret mismatch, **200** with the active phase on
  success. An already-expired `promptEndsAt` starts directly in
  `LOUD_DTMF_MODE`.

## Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /health` | liveness — `{"ok":true}` |
| `GET /ready` | readiness — checks config, fingerprint asset, STATE_FILE access |
| `GET /stats` | diagnostics (requires `x-verify-secret`) |
| `POST /arm` | pre-register a verification session (authenticated) |
| `POST /challenge-start` | arm/refresh the two-phase challenge (authenticated; bodies capped by `MAX_BODY_BYTES`) |
| `GET /twiml/*` | demo TwiML — **disabled unless `DEMO_TWIML=1`** |

## State persistence

Session state (sid, phase, `challengeStartedAt`, `promptEndsAt`,
`transitionToleranceMs`, stream readiness, delivered-callback flags, final
verdict) is written atomically to `STATE_FILE` (tmp write + rename) on every
transition. On boot the state is reloaded: expired prompt windows are
reconstructed as `LOUD_DTMF_MODE`, finalized verdicts stay finalized and
delivered callbacks are never re-sent (idempotent across restarts), and
streams must reconnect and re-identify before they are ready again.

Reliability: WebSocket heartbeat ping/pong, malformed-message counters,
10 s identification timeout, `SILENCE_TIMEOUT_MS` no-media timeout, and an
absolute `SESSION_TIMEOUT_MS` per session.

## Prompt fingerprint asset

`prompt-fingerprint.json` is generated deterministically from the rendered
Phase 1 WAV (speech + 852+1336 Hz watermark at −21 dB below prompt RMS):

```bash
node tools/generate-fingerprint.mjs <call-waiting-prompt-light.wav>
```

The measured WAV duration (`durationMs`, currently **18840 ms**, 752 frames)
is authoritative for the Phase 1 window and is asserted by tests. The
matcher consumes the documented
`normalized-log-band-spectral-contour-v1` format and is invariant to
per-frame affine transforms, so equivalent extractors remain compatible.

## Deploy (Render — what we use)

1. Push this repo to GitHub
2. render.com → New → Web Service → pick the repo (a `render.yaml`
   blueprint is included)
3. Build: `npm install` · Start: `npm start` · Health check: `/health`
4. Set the env vars below in the dashboard (never commit them — see
   `.env.example`)

Railway / Fly.io / a VPS work identically (any Node 18+ host with WSS).

## Environment variables

| Var | Required | Purpose |
|---|---|---|
| `CALLBACK_URL` | yes | app verdict endpoint, e.g. `https://<app>/api/verify/stream-detected` |
| `STREAM_SECRET` | yes | shared secret — must equal the app's `VERIFY_STREAM_SECRET` |
| `STREAM_READY_URL` | no | override derived stream-ready endpoint |
| `STREAM_FAILED_URL` | no | override derived stream-failed endpoint |
| `APP_EVENTS_URL` | no | optional lifecycle-event sink (not derived) |
| `STATE_FILE` | no | atomic session state file (default `./relay-state.json`) |
| `DEMO_TWIML` | no | set `1` to enable demo `/twiml/*` endpoints |
| `LIGHT_TONE_RATIO` | no | Phase 1 light-watermark Goertzel floor (default `3e-3`) |
| `LIGHT_NEED_WINDOWS` | no | consecutive 50 ms windows for the watermark (default `6`) |
| `PROMPT_NCC_THRESHOLD` | no | fingerprint match threshold (default from asset: `0.75`) |
| `SILENCE_TIMEOUT_MS` | no | no-media timeout (default `15000`) |
| `SESSION_TIMEOUT_MS` | no | absolute session timeout (default `600000`) |
| `HEARTBEAT_MS` | no | WebSocket ping interval (default `30000`) |
| `MAX_BODY_BYTES` | no | HTTP JSON body cap (default `65536`) |
| `PORT` | platform | listen port (default `8080`) |

For local development, copy `.env.example` to an **untracked** `.env`
(`git status` must never show it). Real environment variables always win.

Then configure the main app:

| Var | Value |
|---|---|
| `VERIFY_STREAM_URL` | `wss://<relay-host>` (bare base URL — no `?sid=`; parameters are nested) |
| `VERIFY_STREAM_SECRET` | same value as `STREAM_SECRET` |

The app must pass `token = hex(HMAC-SHA256(VERIFY_STREAM_SECRET,
"merge-relay-stream:" + sid))` as the `token` stream parameter.

## Secret rotation (required)

Earlier revisions of this repository committed a live `.env` and concrete
`CALLBACK_URL`/`STREAM_SECRET` values to git history. **Any secret that was
ever committed — the previously committed relay `STREAM_SECRET` and the
exposed GitHub token — must be treated as compromised and rotated.** Current
code only reads secrets from the environment (or a local untracked `.env`);
`render.yaml` marks all secret vars `sync: false`. Do not re-introduce
concrete values into files, tests, or docs.

## Verify it works

```bash
curl https://<relay-host>/health   # {"ok":true,...}
curl https://<relay-host>/ready    # {"ready":true,...}
npm test                           # full self-test suite (no Twilio needed)
```

## Files

- `server.js` — entry point (env config, `.env` loader, asset loading)
- `lib/relay.js` — HTTP endpoints, WebSocket lifecycle, HMAC token auth, two-phase state machine, persistence, callbacks
- `lib/detectors.js` — loud `MergeToneDetector` (unchanged), `LightToneDetector`, spectral `PromptFingerprintMatcher`
- `lib/dsp.js` — μ-law decode, Goertzel, FFT + spectral-contour primitives
- `lib/state.js` — atomic JSON state load/save
- `prompt-fingerprint.json` — spectral-contour fingerprint + light-Goertzel calibration for the 18.84 s prompt+watermark asset
- `tools/generate-fingerprint.mjs` — deterministic fingerprint regeneration from the rendered WAV
- `test/` — unit + integration suite (`node --test`), incl. the real μ-law prompt fixture
- `selftest.mjs` — end-to-end smoke: spawns the real server, drives both phases + failure paths

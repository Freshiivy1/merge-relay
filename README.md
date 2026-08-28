# CallVerify Merge-Detection Relay

Standalone WebSocket service that gives CloudTalk's CallVerify engine
**sub-0.5 second merge detection with an instant in-band verdict**.
The main app's hosting platform blocks WebSockets, so Twilio's live audio
stream terminates here instead. Without this relay the app falls back to the
recording-chunk detector (~2 seconds) — the relay is purely additive.

## What it does (current build)

1. Leg B's TwiML opens a **duplex** Twilio Media Stream (`<Connect><Stream>`)
   to `wss://<relay-host>/?sid=<sessionId>` with `<Parameter name="sid">` and
   `<Parameter name="mode" value="duplex"/>`.
   (Twilio strips query strings from Stream URLs — the sid/mode always travel
   as `<Parameter>` customParameters; the query string is only a nicety.)
2. The relay runs a Goertzel DSP detector (852 Hz + 1336 Hz, DTMF-9) over the
   live inbound audio — fires after **300 ms** of continuous tone.
3. The instant it fires, on duplex legs it:
   - speaks the verdict **directly into the open socket** (`verdict.ulaw`,
     pre-encoded mu-law 8 kHz — zero Twilio round-trip, merge→verdict ≈ 0.3-0.4s)
   - tears down Leg A via Twilio REST (needs `TWILIO_ACCOUNT_SID` /
     `TWILIO_AUTH_TOKEN` and a prior `POST /arm` from the app)
   - POSTs `CALLBACK_URL?sid=<sessionId>` with header
     `x-verify-secret: <STREAM_SECRET>` so the app resolves the session
4. Leg A hangs up / caller gets the verdict TwiML — done.

## Endpoints

| Endpoint | Purpose |
|---|---|
| `GET /health` | liveness — `{"ok":true}` |
| `GET /stats` | live diagnostics: connections, frames, last fire, verdict lag |
| `POST /arm` | app registers `{sid, legA}` (header `x-verify-secret`) so detection can tear down Leg A |
| `GET /twiml/step2` \| `/hold` \| `/legb` \| `/legb2` \| `/verdict` | self-hosted demo TwiML (used by tests; the app serves its own TwiML in production) |

## Deploy (Render — what we use)

1. Push this folder to GitHub
2. render.com → New → Web Service → pick the repo
3. Build: `npm install` · Start: `npm start`
4. Set the env vars below. Render gives `https://<name>.onrender.com`
   — ours is **`https://merge-relay-a7ws.onrender.com`**
   (Render appends a suffix to the service name; always use the real URL)

Railway / Fly.io / a VPS work identically (any Node 18+ host with WSS).

## Environment variables (current production values)

| Var | Value |
|---|---|
| `CALLBACK_URL` | `https://226orhimcsy72.kimi.pro/api/verify/stream-detected` |
| `STREAM_SECRET` | `k7X9mQ2vR8pL4wN6jH3fB5tY1zA0cV8b` |
| `TWILIO_ACCOUNT_SID` | the account SID (needed for Leg A teardown on duplex verdicts) |
| `TWILIO_AUTH_TOKEN` | the auth token (same) |
| `PORT` | injected by the platform (default 8080) |

A committed `.env` acts as a fallback for hosts where env vars were skipped —
real environment variables always win.

## Then configure the main app

| Var | Value |
|---|---|
| `VERIFY_STREAM_URL` | `wss://merge-relay-a7ws.onrender.com` (bare base URL — the app appends `?sid=`) |
| `VERIFY_STREAM_SECRET` | `k7X9mQ2vR8pL4wN6jH3fB5tY1zA0cV8b` — must equal `STREAM_SECRET` |

## Verify it works

```bash
curl https://merge-relay-a7ws.onrender.com/health
# {"ok":true,"service":"callverify-merge-relay"}
curl https://merge-relay-a7ws.onrender.com/stats
# {"connections":..,"frames":..,"lastFireMs":..,"verdicts":..,...}
```

Run a verification and merge: `lastFireMs`/`verdicts` increment and the callee
hears the verdict within half a second of merging.

## Files

- `server.js` — the whole service (detector, endpoints, duplex verdict)
- `verdict.ulaw` — verdict prompt, pre-encoded mu-law 8 kHz mono (spoken in-band)
- `selftest.mjs` — detector unit test: `npm test`

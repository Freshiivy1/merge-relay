# CallVerify Merge-Detection Relay

Tiny standalone WebSocket service that gives you **sub-0.5 second merge detection**.
The main app's hosting platform blocks WebSockets, so Twilio's live audio stream
needs this relay on any WebSocket-capable host. Without it, the app falls back to
the recording-chunk detector (~2 seconds) — the relay is purely additive.

## How it works

1. Leg B's TwiML opens a Twilio Media Stream to `wss://<relay-host>/?sid=<sessionId>`
2. The relay runs a Goertzel DSP detector (852 Hz + 1336 Hz) over the live audio
3. On detection (~300 ms of tone) it POSTs to the main app:
   `CALLBACK_URL?sid=<sessionId>` with header `x-verify-secret: <STREAM_SECRET>`
4. The main app plays the verdict and terminates both calls immediately

## Deploy (pick one — all free tiers work)

### Render
1. Push this folder to a GitHub repo
2. render.com → New → Web Service → pick the repo
3. Build command: `npm install` — Start command: `npm start`
4. Add env vars (below). Render gives you `https://<name>.onrender.com`

### Railway
1. `railway init` in this folder, or New Project → Deploy from repo
2. Add env vars. Railway gives you a public domain.

### Fly.io
1. `fly launch` in this folder (it auto-detects Node)
2. `fly secrets set CALLBACK_URL=... STREAM_SECRET=...`
3. `fly deploy`

### Any VPS
```bash
npm install
CALLBACK_URL=https://YOUR-APP/api/verify/stream-detected \
STREAM_SECRET=<random-long-string> \
PORT=8080 node server.js
```
(put it behind HTTPS — Twilio requires `wss://`)

## Environment variables

| Var | Value |
|---|---|
| `CALLBACK_URL` | `https://226orhimcsy72.kimi.pro/api/verify/stream-detected` |
| `STREAM_SECRET` | `k7X9mQ2vR8pL4wN6jH3fB5tY1zA0cV8b` |
| `PORT` | Listen port (default 8080; most platforms inject their own `PORT`) |

## Then configure the main app

Set these env vars on the main app and republish:

| Var | Value |
|---|---|
| `VERIFY_STREAM_URL` | `wss://merge-relay.onrender.com` (the relay's public WebSocket URL) |
| `VERIFY_STREAM_SECRET` | `k7X9mQ2vR8pL4wN6jH3fB5tY1zA0cV8b` (same as the relay's `STREAM_SECRET`) |

## Verify it works

```bash
curl https://<relay-host>/health
# {"ok":true,"service":"callverify-merge-relay"}
```

Then run a verification call and merge: the session event log will show
`MERGE_STREAM_DETECTED` (relay path) instead of `MERGE_RECORD_DETECTED`
(fallback path), and termination fires in under half a second.

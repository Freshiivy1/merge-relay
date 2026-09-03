/**
 * Two-phase merge-detection relay core.
 *
 * Architecture (binding contract: SPEC.md):
 *   Phase 1 PROMPT_LIGHT_MODE — prompt fingerprint AND overlapping light
 *     852+1336 Hz (DTMF-8 pair) watermark => immediate final MERGE_DETECTED.
 *   At persisted promptEndsAt (+ tolerance) partial Phase 1 evidence is
 *     cleared and the session switches to LOUD_DTMF_MODE.
 *   Phase 2 LOUD_DTMF_MODE — the existing loud-tone detector ALONE =>
 *     immediate final MERGE_DETECTED. All three signals are never required.
 *
 * The Leg B stream is background inbound-only: this service NEVER writes
 * media into it (no duplex <Connect><Stream> path). Verdicts go to the app
 * through the authenticated CALLBACK_URL; stream lifecycle events go to
 * APP_EVENTS_URL (or the URL derived from CALLBACK_URL).
 */
import http from "http";
import { WebSocketServer } from "ws";
import { MergeToneDetector, LightToneDetector, PromptFingerprintMatcher } from "./detectors.js";
import { loadState, saveState, stateFileWritable } from "./state.js";

export const PHASE_PROMPT_LIGHT = "PROMPT_LIGHT_MODE";
export const PHASE_LOUD = "LOUD_DTMF_MODE";

export const RESULT_MERGE = "MERGE_DETECTED";
export const RESULT_FAILED = "DETECTION_FAILED";
export const RESULT_INCONCLUSIVE = "DETECTION_INCONCLUSIVE";

const MAX_WS_MSG_BYTES = 64 * 1024;
const MAX_MALFORMED = 10; // consecutive malformed messages => stream error

export function deriveEventsUrl(callbackUrl) {
  if (!callbackUrl) return "";
  return callbackUrl.replace(/\/stream-detected\/?$/, "/stream-event");
}

function readBody(req, maxBytes) {
  return new Promise((resolve, reject) => {
    let size = 0;
    let tooLarge = false;
    const chunks = [];
    req.on("data", (c) => {
      size += c.length;
      if (size > maxBytes) {
        tooLarge = true; // keep draining so the 413 response can be delivered
        chunks.length = 0;
        return;
      }
      if (!tooLarge) chunks.push(c);
    });
    req.on("end", () => {
      if (tooLarge) reject(Object.assign(new Error("body too large"), { code: 413 }));
      else resolve(Buffer.concat(chunks).toString("utf8"));
    });
    req.on("error", reject);
  });
}

export function createRelay(config) {
  const cfg = {
    callbackUrl: (config.callbackUrl || "").replace(/\/+$/, ""),
    appEventsUrl: config.appEventsUrl || deriveEventsUrl(config.callbackUrl || ""),
    secret: config.secret || "",
    stateFile: config.stateFile,
    fingerprint: config.fingerprint || null,
    lightRatioFloor: config.lightRatioFloor ?? 1e-3,
    lightNeedWindows: config.lightNeedWindows ?? 6,
    promptThreshold: config.promptThreshold ?? undefined,
    silenceTimeoutMs: config.silenceTimeoutMs ?? 15000,
    sessionTimeoutMs: config.sessionTimeoutMs ?? 10 * 60 * 1000,
    heartbeatMs: config.heartbeatMs ?? 30000,
    identTimeoutMs: config.identTimeoutMs ?? 10000,
    maxBodyBytes: config.maxBodyBytes ?? 64 * 1024,
    callbackAttempts: config.callbackAttempts ?? 3,
    callbackRetryDelayMs: config.callbackRetryDelayMs ?? 500,
    log: config.log ?? ((...a) => console.log("[relay]", ...a)),
    now: config.now ?? (() => Date.now()),
  };

  const stats = {
    startedAt: new Date().toISOString(),
    node: process.version,
    connections: 0,
    streamsReady: 0,
    frames: 0,
    malformed: 0,
    invalidStarts: 0,
    closedNoSid: 0,
    verdicts: 0,
    lastSid: "",
    lastVerdict: null,
    lastCallbackStatus: 0,
    callbackRetries: 0,
    eventsSent: 0,
    lastError: "",
    uncaught: 0,
  };

  /** @type {Map<string, object>} live + persisted sessions keyed by sid */
  const sessions = new Map();

  /* ------------------------------ persistence ---------------------- */

  function persistSession(session) {
    return {
      sid: session.sid,
      leg: session.leg,
      mode: session.mode,
      challengeToken: session.challengeToken || "",
      streamSid: session.streamSid || "",
      legacy: !!session.legacy,
      streamReady: !!session.streamReady,
      phase: session.phase || null,
      challengeStartedAt: session.challengeStartedAt || null,
      promptLightDurationMs: session.promptLightDurationMs || null,
      promptEndsAt: session.promptEndsAt || null,
      phaseToleranceMs: session.phaseToleranceMs ?? 250,
      final: session.final || null,
      createdAt: session.createdAt,
      updatedAt: cfg.now(),
    };
  }

  function persist() {
    if (!cfg.stateFile) return;
    const out = {};
    for (const [sid, s] of sessions) out[sid] = persistSession(s);
    try {
      saveState(cfg.stateFile, out);
    } catch (err) {
      stats.lastError = "state save: " + err.message;
    }
  }

  function reconstructPhase(s) {
    if (!s.promptEndsAt) return s.phase || null;
    const tol = s.phaseToleranceMs ?? 250;
    return cfg.now() >= s.promptEndsAt + tol ? PHASE_LOUD : (s.phase || PHASE_PROMPT_LIGHT);
  }

  function loadPersisted() {
    if (!cfg.stateFile) return;
    const state = loadState(cfg.stateFile);
    for (const [sid, s] of Object.entries(state.sessions || {})) {
      const session = {
        ...s,
        sid,
        // A persisted stream is never live after a restart: it must reconnect
        // and send a fresh start + media frame before it is ready again.
        streamReady: false,
        awaitingReconnect: !s.final,
        phase: s.final ? s.phase : reconstructPhase(s),
        ws: null,
        frames: 0,
        malformed: 0,
        detectors: null,
        timers: {},
      };
      sessions.set(sid, session);
      if (!session.final && session.promptEndsAt) armSessionTimeout(session);
    }
    cfg.log(`state loaded: ${sessions.size} session(s) from ${cfg.stateFile}`);
  }

  /* ------------------------------ HTTP helpers --------------------- */

  async function postJson(url, body, attempts) {
    let lastStatus = 0;
    let lastErr = null;
    for (let i = 0; i < attempts; i++) {
      if (i > 0) {
        stats.callbackRetries++;
        await new Promise((r) => setTimeout(r, cfg.callbackRetryDelayMs * i));
      }
      try {
        const res = await fetch(url, {
          method: "POST",
          headers: { "Content-Type": "application/json", "x-verify-secret": cfg.secret },
          body: JSON.stringify(body),
        });
        lastStatus = res.status;
        stats.lastCallbackStatus = res.status;
        if (res.status < 500) return res.status;
      } catch (err) {
        lastErr = err;
        stats.lastError = err.message;
      }
    }
    if (lastErr) cfg.log("callback failed:", lastErr.message);
    return lastStatus;
  }

  function postEvent(session, event, reason) {
    if (!cfg.appEventsUrl || !cfg.secret || !session.sid) return;
    stats.eventsSent++;
    postJson(cfg.appEventsUrl, {
      sid: session.sid,
      event,
      streamSid: session.streamSid || "",
      reason: reason || "",
      at: new Date().toISOString(),
    }, 2).catch(() => {});
  }

  /* ------------------------------ finalization --------------------- */

  /** Idempotent: exactly one terminal verdict per session, ever. */
  function finalize(session, result, reason) {
    if (!session || session.final) return false;
    session.final = {
      result,
      phase: session.phase || null,
      reason,
      detectedAt: cfg.now(),
    };
    clearTimers(session);
    stats.verdicts++;
    stats.lastVerdict = { sid: session.sid, ...session.final, at: new Date().toISOString() };
    persist();
    cfg.log(`verdict sid=${session.sid} result=${result} phase=${session.phase} reason=${reason}`);
    if (cfg.callbackUrl && cfg.secret) {
      const sep = cfg.callbackUrl.includes("?") ? "&" : "?";
      postJson(`${cfg.callbackUrl}${sep}sid=${encodeURIComponent(session.sid)}`, {
        sid: session.sid,
        result,
        phase: session.phase || null,
        reason,
        detectedAt: session.final.detectedAt,
      }, cfg.callbackAttempts).catch(() => {});
    }
    return true;
  }

  function clearTimers(session) {
    for (const t of Object.values(session.timers || {})) clearTimeout(t);
    session.timers = {};
  }

  /* ------------------------------ timeouts ------------------------- */

  function armSilenceTimeout(session) {
    clearTimeout(session.timers.silence);
    if (!session.streamReady || session.final) return;
    session.timers.silence = setTimeout(() => {
      if (session.final || !session.streamReady) return;
      cfg.log(`silence timeout sid=${session.sid} (no media for ${cfg.silenceTimeoutMs}ms)`);
      postEvent(session, "stream_timeout", "silence_timeout");
      finalize(session, RESULT_INCONCLUSIVE, "silence_timeout");
      try { session.ws?.close(4408, "silence timeout"); } catch {}
    }, cfg.silenceTimeoutMs);
  }

  function armSessionTimeout(session) {
    clearTimeout(session.timers.absolute);
    const deadline = (session.createdAt || cfg.now()) + cfg.sessionTimeoutMs;
    const wait = Math.max(deadline - cfg.now(), 1);
    session.timers.absolute = setTimeout(() => {
      if (session.final) return;
      cfg.log(`absolute session timeout sid=${session.sid}`);
      postEvent(session, "stream_timeout", "session_timeout");
      finalize(session, RESULT_INCONCLUSIVE, "session_timeout");
      try { session.ws?.close(4408, "session timeout"); } catch {}
    }, wait);
  }

  /* ------------------------------ detection ------------------------ */

  function ensureDetectors(session) {
    if (session.detectors) return session.detectors;
    session.detectors = {
      loud: new MergeToneDetector(),
      light: new LightToneDetector({
        ratioFloor: cfg.lightRatioFloor,
        needWindows: cfg.lightNeedWindows,
      }),
      prompt: cfg.fingerprint ? new PromptFingerprintMatcher(cfg.fingerprint, cfg.promptThreshold) : null,
      promptMatchedAt: 0,
      lightMatchedAt: 0,
    };
    return session.detectors;
  }

  function switchToLoudMode(session) {
    const d = session.detectors;
    // Clear all partial Phase 1 evidence at the persisted boundary.
    if (d) {
      d.light = new LightToneDetector({ ratioFloor: cfg.lightRatioFloor, needWindows: cfg.lightNeedWindows });
      d.prompt = cfg.fingerprint ? new PromptFingerprintMatcher(cfg.fingerprint, cfg.promptThreshold) : null;
      d.promptMatchedAt = 0;
      d.lightMatchedAt = 0;
    }
    session.phase = PHASE_LOUD;
    persist();
    cfg.log(`sid=${session.sid} promptEndsAt reached -> LOUD_DTMF_MODE`);
  }

  function processMedia(session, payload) {
    if (session.final) return;
    if (!session.phase) return; // no challenge yet: stream is monitored but not scoring
    let d;
    try {
      d = ensureDetectors(session);
      if (session.phase === PHASE_PROMPT_LIGHT) {
        const tol = session.phaseToleranceMs ?? 250;
        if (session.promptEndsAt && cfg.now() >= session.promptEndsAt + tol) {
          switchToLoudMode(session);
        } else {
          if (d.prompt && d.prompt.push(payload)) d.promptMatchedAt = cfg.now();
          if (d.light.push(payload)) d.lightMatchedAt = cfg.now();
          // Prompt fingerprint AND overlapping light watermark => final verdict.
          if (d.promptMatchedAt && d.lightMatchedAt) {
            finalize(session, RESULT_MERGE, "prompt+light");
          }
          return;
        }
      }
      // LOUD_DTMF_MODE: the existing loud-tone detector alone decides.
      if (session.phase === PHASE_LOUD && d.loud.push(payload)) {
        finalize(session, RESULT_MERGE, "loud-tone");
      }
    } catch (err) {
      stats.lastError = "detector: " + err.message;
      cfg.log(`detector error sid=${session.sid}:`, err.message);
      postEvent(session, "stream_error", "detector_error");
      finalize(session, RESULT_FAILED, "detector_error");
    }
  }

  /* ------------------------------ websocket ------------------------ */

  function getOrCreateSession(sid) {
    let session = sessions.get(sid);
    if (!session) {
      session = {
        sid,
        leg: "",
        mode: "",
        challengeToken: "",
        streamSid: "",
        legacy: false,
        streamReady: false,
        phase: null,
        promptEndsAt: null,
        phaseToleranceMs: 250,
        final: null,
        createdAt: cfg.now(),
        frames: 0,
        malformed: 0,
        detectors: null,
        timers: {},
        ws: null,
      };
      sessions.set(sid, session);
    }
    return session;
  }

  function handleStreamEnd(session, kind, reason) {
    // kind: stream_stopped | stream_error
    if (!session.sid) return;
    postEvent(session, kind, reason);
    session.streamReady = false;
    session.ws = null;
    persist();
    if (session.final) return;
    if (!session.phase && !session.challengeStartedAt && session.frames === 0) return; // never really started
    // A stream that dies before a verdict can never be a pass.
    finalize(session, kind === "stream_error" ? RESULT_FAILED : RESULT_INCONCLUSIVE, reason);
  }

  function attachWebSocket(wss) {
    // Heartbeat: drop connections that stop answering pings.
    const heartbeat = setInterval(() => {
      for (const ws of wss.clients) {
        if (ws.isAlive === false) {
          ws.terminate();
          continue;
        }
        ws.isAlive = false;
        try { ws.ping(); } catch {}
      }
    }, cfg.heartbeatMs);
    wss.on("close", () => clearInterval(heartbeat));

    wss.on("connection", (ws, req) => {
      stats.connections++;
      ws.isAlive = true;
      ws.on("pong", () => { ws.isAlive = true; });

      // Backwards-compatible test path: session id via query string. New
      // TwiML identifies itself exclusively through start.customParameters.
      const querySid = new URL(req.url, "http://localhost").searchParams.get("sid") || "";
      let session = null;
      let gotStart = false;
      let streamEnded = false;

      const identTimer = setTimeout(() => {
        if (!session) {
          stats.closedNoSid++;
          try { ws.close(); } catch {}
        }
      }, cfg.identTimeoutMs);

      if (querySid) {
        session = getOrCreateSession(querySid);
        session.legacy = true;
        session.ws = ws;
        clearTimeout(identTimer);
      }

      function markReady() {
        if (!session || session.streamReady) return;
        session.streamReady = true;
        session.awaitingReconnect = false;
        stats.streamsReady++;
        stats.lastSid = session.sid;
        persist();
        cfg.log(`stream ready sid=${session.sid} streamSid=${session.streamSid}`);
        postEvent(session, "stream_ready", "");
        armSilenceTimeout(session);
        armSessionTimeout(session);
      }

      ws.on("message", (data) => {
        if (data.length > MAX_WS_MSG_BYTES) {
          stats.malformed++;
          if (session) session.malformed++;
          try { ws.close(4400, "message too large"); } catch {}
          return;
        }
        let msg;
        try {
          msg = JSON.parse(data.toString());
        } catch {
          stats.malformed++;
          if (session) {
            session.malformed++;
            if (session.malformed > MAX_MALFORMED && !session.final) {
              postEvent(session, "stream_error", "malformed_stream");
              finalize(session, RESULT_FAILED, "malformed_stream");
              streamEnded = true;
              try { ws.close(4400, "malformed stream"); } catch {}
            }
          }
          return;
        }
        if (!msg || typeof msg.event !== "string") {
          stats.malformed++;
          return;
        }

        if (msg.event === "start") {
          gotStart = true;
          const p = msg.start?.customParameters;
          const streamSid = msg.start?.streamSid || "";
          if (p && typeof p === "object" && Object.keys(p).length > 0) {
            // Canonical identification: nested Twilio parameters only. A
            // valid start carries sid, leg=legB, mode=merge-detection and the
            // per-session challenge token.
            if (!p.sid || p.leg !== "legB" || p.mode !== "merge-detection" || !p.challengeToken) {
              stats.invalidStarts++;
              cfg.log(`invalid start parameters (sid=${p.sid || "?"} leg=${p.leg || "?"} mode=${p.mode || "?"})`);
              try { ws.close(4400, "invalid stream parameters"); } catch {}
              return;
            }
            const existing = sessions.get(p.sid);
            if (existing && existing.challengeToken && p.challengeToken && existing.challengeToken !== p.challengeToken) {
              stats.invalidStarts++;
              try { ws.close(4403, "challenge token mismatch"); } catch {}
              return;
            }
            session = existing || getOrCreateSession(p.sid);
            session.legacy = false;
            session.leg = p.leg || "legB";
            session.mode = p.mode || "merge-detection";
            if (p.challengeToken) session.challengeToken = p.challengeToken;
            session.streamSid = streamSid;
            session.ws = ws;
            clearTimeout(identTimer);
          } else if (session) {
            session.streamSid = streamSid;
          } else {
            stats.invalidStarts++;
            try { ws.close(4400, "unidentified stream"); } catch {}
          }
          persist();
          return;
        }

        if (msg.event === "stop") {
          streamEnded = true;
          handleStreamEnd(session, "stream_stopped", "stop_message");
          try { ws.close(); } catch {}
          return;
        }

        if (msg.event !== "media" || !msg.media?.payload) return;
        if (msg.media.track && msg.media.track !== "inbound") return;
        if (!session || !gotStart) return; // unidentified yet — drop audio

        session.frames++;
        stats.frames++;
        session.lastMediaAt = cfg.now();
        if (!session.streamReady) markReady();
        armSilenceTimeout(session);
        processMedia(session, msg.media.payload);
      });

      ws.on("close", () => {
        clearTimeout(identTimer);
        if (session) {
          cfg.log(`stream closed sid=${session.sid} frames=${session.frames}`);
          if (!streamEnded) handleStreamEnd(session, "stream_stopped", "socket_closed");
        }
      });
      ws.on("error", (err) => {
        stats.lastError = err.message;
        cfg.log(`ws error sid=${session?.sid || "?"}:`, err.message);
        if (session && !streamEnded) {
          streamEnded = true;
          handleStreamEnd(session, "stream_error", "socket_error");
        }
      });
    });
  }

  /* ------------------------------ HTTP ----------------------------- */

  function challengeStart(req, res) {
    if (req.headers["x-verify-secret"] !== cfg.secret || !cfg.secret) {
      res.writeHead(403, { "Content-Type": "application/json" });
      res.end('{"error":"forbidden"}');
      return;
    }
    readBody(req, cfg.maxBodyBytes)
      .then((body) => {
        let j;
        try {
          j = JSON.parse(body);
        } catch {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end('{"error":"invalid_json"}');
          return;
        }
        if (!j || typeof j.sid !== "string" || !j.sid) {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end('{"error":"sid_required"}');
          return;
        }
        const session = sessions.get(j.sid);
        if (!session || !session.streamReady) {
          res.writeHead(409, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "stream_not_ready", sid: j.sid }));
          return;
        }
        if (session.challengeToken && j.challengeToken && session.challengeToken !== j.challengeToken) {
          res.writeHead(403, { "Content-Type": "application/json" });
          res.end('{"error":"challenge_token_mismatch"}');
          return;
        }
        if (j.challengeToken) session.challengeToken = j.challengeToken;
        session.challengeStartedAt = j.challengeStartedAt ?? cfg.now();
        session.promptLightDurationMs = j.promptLightDurationMs ?? null;
        session.promptEndsAt = j.promptEndsAt ?? (session.promptLightDurationMs
          ? session.challengeStartedAt + session.promptLightDurationMs
          : null);
        session.phaseToleranceMs = j.phaseToleranceMs ?? 250;
        // Reconstruct the phase from persisted timestamps: an already-expired
        // prompt window starts (or restarts) directly in LOUD_DTMF_MODE.
        session.phase = session.final ? session.phase : reconstructPhase(session) || PHASE_PROMPT_LIGHT;
        persist();
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({
          ok: true,
          sid: session.sid,
          phase: session.phase,
          promptEndsAt: session.promptEndsAt,
          final: session.final || null,
        }));
      })
      .catch((err) => {
        res.writeHead(err.code === 413 ? 413 : 400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: err.code === 413 ? "body_too_large" : "bad_request" }));
      });
  }

  function demoTwiml(req, res) {
    const kind = req.url.split("/")[2].split("?")[0];
    const base = "https://" + (req.headers.host || "");
    const wssBase = "wss://" + (req.headers.host || "");
    const q = new URL(req.url, "http://localhost").searchParams;
    const sid = q.get("sid") || "demo";
    const token = q.get("token") || "demo-token";
    let body = null;
    if (kind === "legb") {
      // Demo Leg B TwiML: background inbound-only stream identified purely by
      // nested parameters (NO sid in the stream URL query string), then
      // immediately continues to the conference. Never <Connect><Stream>.
      body = `<Response>` +
        `<Start><Stream url="${wssBase}/" track="inbound_track">` +
        `<Parameter name="sid" value="${sid}"/>` +
        `<Parameter name="leg" value="legB"/>` +
        `<Parameter name="mode" value="merge-detection"/>` +
        `<Parameter name="challengeToken" value="${token}"/>` +
        `</Stream></Start>` +
        `<Dial><Conference beep="false" startConferenceOnEnter="true" endConferenceOnExit="true">verification-${sid}</Conference></Dial>` +
        `</Response>`;
    } else if (kind === "hold") {
      body = `<Response><Pause length="60"/><Redirect method="GET">${base}/twiml/hold</Redirect></Response>`;
    } else if (kind === "verdict") {
      body = `<Response><Say voice="Polly.Brian">Merge detected. Verification complete. This line is confirmed as a cellular phone.</Say><Hangup/></Response>`;
    }
    if (body === null) {
      res.writeHead(404);
      res.end();
      return;
    }
    stats.lastTwiml = kind;
    res.writeHead(200, { "Content-Type": "text/xml" });
    res.end(body);
  }

  function readiness() {
    const reasons = [];
    if (!cfg.callbackUrl) reasons.push("CALLBACK_URL missing");
    if (!cfg.secret) reasons.push("STREAM_SECRET missing");
    if (!cfg.fingerprint) reasons.push("prompt fingerprint not loaded");
    if (cfg.stateFile && !stateFileWritable(cfg.stateFile)) reasons.push("STATE_FILE not writable");
    return reasons;
  }

  const server = http.createServer((req, res) => {
    const url = (req.url || "").split("?")[0];
    if (req.method === "GET" && (url === "/" || url === "/health")) {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: true, service: "callverify-merge-relay" }));
      return;
    }
    if (req.method === "GET" && url === "/ready") {
      const reasons = readiness();
      res.writeHead(reasons.length ? 503 : 200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        ready: reasons.length === 0,
        reasons,
        fingerprint: cfg.fingerprint
          ? { asset: cfg.fingerprint.asset, durationMs: cfg.fingerprint.durationMs }
          : null,
      }));
      return;
    }
    if (req.method === "GET" && url === "/stats") {
      if (req.headers["x-verify-secret"] !== cfg.secret || !cfg.secret) {
        res.writeHead(403, { "Content-Type": "application/json" });
        res.end('{"error":"forbidden"}');
        return;
      }
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        ...stats,
        sessions: [...sessions.values()].map((s) => ({
          ...persistSession(s),
          challengeToken: s.challengeToken ? "(set)" : "",
        })),
      }));
      return;
    }
    if (req.method === "POST" && url === "/challenge-start") {
      challengeStart(req, res);
      return;
    }
    if (req.method === "GET" && url.startsWith("/twiml/")) {
      demoTwiml(req, res);
      return;
    }
    res.writeHead(404);
    res.end();
  });

  const wss = new WebSocketServer({ server, maxPayload: MAX_WS_MSG_BYTES });
  attachWebSocket(wss);
  loadPersisted();

  return {
    server,
    wss,
    stats,
    sessions,
    cfg,
    finalize,
    close() {
      for (const s of sessions.values()) clearTimers(s);
      wss.close();
      server.close();
    },
  };
}

/**
 * Two-phase merge-detection relay core.
 *
 * Architecture (binding contract: SPEC.md):
 *   Phase 1 PROMPT_LIGHT_MODE — prompt fingerprint (spectral contour) AND
 *     overlapping light 852+1336 Hz (DTMF-8 pair) watermark => immediate
 *     final MERGE_DETECTED. Prompt alone / light alone never fire.
 *   At persisted promptEndsAt (+ transitionToleranceMs) partial Phase 1
 *     evidence is cleared and the session switches to LOUD_DTMF_MODE.
 *   Phase 2 LOUD_DTMF_MODE — the existing loud-tone detector ALONE =>
 *     immediate final MERGE_DETECTED. All three signals are never required.
 *
 * The Leg B stream is background inbound-only: this service NEVER writes
 * media into it (no duplex <Connect><Stream> path). Stream identity comes
 * exclusively from start.customParameters {sid, leg=legB,
 * mode=merge-detection, token}; the token is
 *   hex(HMAC-SHA256(key=STREAM_SECRET, msg="merge-relay-stream:" + sid))
 * — the same shared secret the app uses for x-verify-secret. Query-string
 * session IDs are not accepted.
 *
 * Outbound contract (all POST, JSON, x-verify-secret header, bounded
 * retries with backoff, idempotent per sid via persisted terminal state):
 *   {CALLBACK_BASE}/stream-ready    { sid, streamSid, readyAt }
 *   {CALLBACK_BASE}/stream-detected { sid, verdict: "MERGE_DETECTED",
 *                                     phase: "PROMPT_LIGHT" | "LOUD_DTMF",
 *                                     detectedAt, evidence }
 *   {CALLBACK_BASE}/stream-failed   { sid, verdict: "DETECTION_FAILED" |
 *                                     "DETECTION_INCONCLUSIVE",
 *                                     reason, failedAt }
 * CALLBACK_URL configures the stream-detected endpoint; the other two are
 * derived by replacing the trailing path segment (or overridden with
 * STREAM_READY_URL / STREAM_FAILED_URL). If delivering MERGE_DETECTED
 * exhausts retries, a best-effort stream-failed (callback_exhausted) is
 * sent — a merge is never silently lost.
 */
import http from "http";
import crypto from "crypto";
import { WebSocketServer } from "ws";
import { MergeToneDetector, LightToneDetector, PromptFingerprintMatcher } from "./detectors.js";
import { loadState, saveState, stateFileWritable } from "./state.js";

export const PHASE_PROMPT_LIGHT = "PROMPT_LIGHT_MODE";
export const PHASE_LOUD = "LOUD_DTMF_MODE";

export const RESULT_MERGE = "MERGE_DETECTED";
export const RESULT_FAILED = "DETECTION_FAILED";
export const RESULT_INCONCLUSIVE = "DETECTION_INCONCLUSIVE";

/** Phase names used in the stream-detected callback body (SPEC §3). */
export const PHASE_CALLBACK = {
  [PHASE_PROMPT_LIGHT]: "PROMPT_LIGHT",
  [PHASE_LOUD]: "LOUD_DTMF",
};

const MAX_WS_MSG_BYTES = 64 * 1024;
const MAX_MALFORMED = 10; // consecutive malformed messages => stream error
const TONE_LOW = 852;
const TONE_HIGH = 1336;

/**
 * Per-session stream token: hex HMAC-SHA256 of the sid keyed by the shared
 * secret. The app computes the same value and passes it as the `token`
 * stream parameter; the relay validates it statelessly.
 */
export function streamToken(secret, sid) {
  return crypto.createHmac("sha256", String(secret)).update(`merge-relay-stream:${sid}`).digest("hex");
}

export function tokenValid(secret, sid, token) {
  if (!secret || !sid || typeof token !== "string" || !token) return false;
  const expected = Buffer.from(streamToken(secret, sid), "utf8");
  const actual = Buffer.from(token, "utf8");
  return expected.length === actual.length && crypto.timingSafeEqual(expected, actual);
}

/** Replace the trailing /stream-<name> path segment of a callback URL. */
export function deriveCallbackUrl(callbackUrl, name) {
  if (!callbackUrl) return "";
  return callbackUrl.replace(/\/stream-[a-z-]+\/?$/, "/stream-" + name);
}

/**
 * Normalize a wire timestamp to epoch milliseconds. Cloudtalk has sent
 * `challengeStartedAt` / `promptEndsAt` as BOTH epoch-ms numbers and
 * ISO-8601 strings; raw arithmetic on a string silently breaks the
 * Phase 1 -> Phase 2 boundary (`now >= promptEndsAt + tol` becomes
 * concatenation/NaN and LOUD_DTMF_MODE never activates). Accepts a finite
 * number, a numeric string, a Date, or an ISO-8601 date string; returns
 * null for anything unparseable so callers can reject with 400.
 */
export function toEpochMs(value) {
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (value instanceof Date) {
    const t = value.getTime();
    return Number.isFinite(t) ? t : null;
  }
  if (typeof value === "string") {
    const s = value.trim();
    if (!s) return null;
    if (/^[+-]?\d+(\.\d+)?$/.test(s)) {
      const n = Number(s);
      return Number.isFinite(n) ? n : null;
    }
    const t = Date.parse(s);
    return Number.isNaN(t) ? null : t;
  }
  return null;
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
    // CALLBACK_URL is the app's stream-detected endpoint.
    callbackUrl: (config.callbackUrl || "").replace(/\/+$/, ""),
    streamReadyUrl:
      config.streamReadyUrl || deriveCallbackUrl((config.callbackUrl || "").replace(/\/+$/, ""), "ready"),
    streamFailedUrl:
      config.streamFailedUrl || deriveCallbackUrl((config.callbackUrl || "").replace(/\/+$/, ""), "failed"),
    // Optional extra lifecycle-event sink (stream_stopped/error/timeout).
    // Not derived by default: only used when explicitly configured.
    appEventsUrl: config.appEventsUrl || "",
    secret: config.secret || "",
    stateFile: config.stateFile,
    fingerprint: config.fingerprint || null,
    demoTwiml: !!config.demoTwiml,
    lightRatioFloor: config.lightRatioFloor ?? config.fingerprint?.lightTone?.ratioFloor ?? 3e-3,
    lightNeedWindows: config.lightNeedWindows ?? config.fingerprint?.lightTone?.consecutiveWindows ?? 6,
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
    callbackExhausted: 0,
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
      armed: !!session.armed,
      legA: session.legA || "",
      legB: session.legB || "",
      streamSid: session.streamSid || "",
      streamReady: !!session.streamReady,
      streamReadyAt: session.streamReadyAt || null,
      readySent: !!session.readySent,
      phase: session.phase || null,
      challengeStartedAt: session.challengeStartedAt || null,
      promptLightDurationMs: session.promptLightDurationMs || null,
      promptEndsAt: session.promptEndsAt || null,
      transitionToleranceMs: session.transitionToleranceMs ?? 250,
      final: session.final || null,
      finalDelivered: !!session.finalDelivered,
      callbackDeliveredAt: session.callbackDeliveredAt || null,
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
    const tol = s.transitionToleranceMs ?? 250;
    return cfg.now() >= s.promptEndsAt + tol ? PHASE_LOUD : (s.phase || PHASE_PROMPT_LIGHT);
  }

  function loadPersisted() {
    if (!cfg.stateFile) return;
    const state = loadState(cfg.stateFile);
    for (const [sid, s] of Object.entries(state.sessions || {})) {
      const session = {
        ...s,
        sid,
        // Normalize persisted timestamps: state files written by older
        // builds may hold ISO-8601 strings, and phase reconstruction does
        // epoch-ms arithmetic on them.
        challengeStartedAt: toEpochMs(s.challengeStartedAt),
        promptEndsAt: toEpochMs(s.promptEndsAt),
        callbackDeliveredAt: s.callbackDeliveredAt || null,
        // A persisted stream is never live after a restart: it must reconnect
        // with a valid start + media frame before it is ready again. A
        // delivered stream-ready / terminal verdict is never re-sent.
        streamReady: false,
        awaitingReconnect: !s.final,
        ws: null,
        frames: 0,
        malformed: 0,
        detectors: null,
        timers: {},
      };
      session.phase = s.final ? s.phase : reconstructPhase(session);
      sessions.set(sid, session);
      if (!session.final && session.promptEndsAt) armSessionTimeout(session);
      if (session.final && !session.finalDelivered) {
        // The terminal verdict was persisted but its callback never reached
        // the app (crash between verdict and delivery, or retries exhausted
        // before shutdown). Resume delivery now with the normal retry/backoff
        // path so a terminal outcome is never silently stranded; the
        // callbackDeliveredAt marker keeps it exactly-once.
        cfg.log(`resuming undelivered terminal callback sid=${sid} verdict=${session.final.result}`);
        sendTerminalCallback(session).catch(() => {});
      }
    }
    cfg.log(`state loaded: ${sessions.size} session(s) from ${cfg.stateFile}`);
  }

  /* ------------------------------ HTTP helpers --------------------- */

  /** POST JSON with the shared-secret header; bounded retries w/ backoff. */
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
        if (res.status >= 200 && res.status < 300) return res.status;
        if (res.status < 500) return res.status; // 4xx: no point retrying
      } catch (err) {
        lastErr = err;
        stats.lastError = err.message;
      }
    }
    if (lastErr) cfg.log("callback failed:", lastErr.message);
    return lastStatus;
  }

  /** Optional lifecycle events (only when APP_EVENTS_URL is configured). */
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

  /**
   * Authenticated stream-ready callback (SPEC §3). Sent once per sid after
   * a valid Twilio start + first inbound media frame; persisted so
   * reconnects/restarts never duplicate it.
   */
  async function sendStreamReady(session) {
    if (!cfg.streamReadyUrl || !cfg.secret || session.readySent || session.final) return;
    const status = await postJson(cfg.streamReadyUrl, {
      sid: session.sid,
      streamSid: session.streamSid || "",
      readyAt: session.streamReadyAt,
    }, cfg.callbackAttempts).catch(() => 0);
    if (status >= 200 && status < 300) {
      session.readySent = true;
      persist();
    } else {
      // Missing readiness must never become a silent success: the challenge
      // cannot start without it, so the session is an explicit failure.
      stats.callbackExhausted++;
      stats.lastError = `stream-ready callback exhausted (status ${status})`;
      cfg.log(`stream-ready callback exhausted sid=${session.sid} status=${status}`);
      finalize(session, RESULT_FAILED, "stream_ready_callback_exhausted");
    }
  }

  /**
   * Terminal verdict callback: MERGE_DETECTED goes to stream-detected,
   * failures to stream-failed (SPEC §3 field names). Exactly one terminal
   * callback per sid (persisted terminal state makes it idempotent across
   * reconnects and restarts).
   */
  async function sendTerminalCallback(session) {
    const f = session.final;
    if (!f || session.finalDelivered || !cfg.secret) return;
    const isMerge = f.result === RESULT_MERGE;
    const url = isMerge ? cfg.callbackUrl : cfg.streamFailedUrl;
    if (!url) return;
    const body = isMerge
      ? {
          sid: session.sid,
          verdict: RESULT_MERGE,
          phase: PHASE_CALLBACK[f.phase] ?? f.phase ?? null,
          detectedAt: f.detectedAt,
          evidence: f.evidence || {},
        }
      : {
          sid: session.sid,
          verdict: f.result,
          reason: f.reason,
          failedAt: f.detectedAt,
        };
    const status = await postJson(url, body, cfg.callbackAttempts).catch(() => 0);
    if (status >= 200 && status < 300) {
      session.finalDelivered = true;
      session.callbackDeliveredAt = cfg.now(); // exactly-once marker, persisted
      persist();
      return;
    }
    stats.callbackExhausted++;
    stats.lastError = `terminal callback exhausted (status ${status})`;
    cfg.log(`terminal callback exhausted sid=${session.sid} status=${status} verdict=${f.result}`);
    if (isMerge && cfg.streamFailedUrl) {
      // Callback failure after retry must never strand a merge as a silent
      // pass: escalate to an explicit inconclusive outcome (best effort).
      postJson(cfg.streamFailedUrl, {
        sid: session.sid,
        verdict: RESULT_INCONCLUSIVE,
        reason: "callback_exhausted",
        failedAt: cfg.now(),
      }, 2).catch(() => {});
    }
  }

  /* ------------------------------ finalization --------------------- */

  /** Idempotent: exactly one terminal verdict per session, ever. */
  function finalize(session, result, reason, evidence) {
    if (!session || session.final) return false;
    session.final = {
      result,
      phase: session.phase || null,
      reason,
      detectedAt: cfg.now(),
      evidence: evidence || null,
    };
    clearTimers(session);
    stats.verdicts++;
    stats.lastVerdict = { sid: session.sid, ...session.final, at: new Date().toISOString() };
    persist();
    cfg.log(`verdict sid=${session.sid} result=${result} phase=${session.phase} reason=${reason}`);
    sendTerminalCallback(session).catch(() => {});
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
        const tol = session.transitionToleranceMs ?? 250;
        if (session.promptEndsAt && cfg.now() >= session.promptEndsAt + tol) {
          switchToLoudMode(session);
        } else {
          if (d.prompt && d.prompt.push(payload)) d.promptMatchedAt = cfg.now();
          if (d.light.push(payload)) d.lightMatchedAt = cfg.now();
          // Prompt fingerprint AND overlapping light watermark => final verdict.
          if (d.promptMatchedAt && d.lightMatchedAt) {
            finalize(session, RESULT_MERGE, "prompt+light", {
              promptScore: Number(d.prompt.bestScore.toFixed(4)),
              promptMatchedAt: d.promptMatchedAt,
              lightMatchedAt: d.lightMatchedAt,
              lightHitWindows: d.light.hitWindows,
              toneFrequenciesHz: [TONE_LOW, TONE_HIGH],
            });
          }
          return;
        }
      }
      // LOUD_DTMF_MODE: the existing loud-tone detector alone decides.
      if (session.phase === PHASE_LOUD && d.loud.push(payload)) {
        finalize(session, RESULT_MERGE, "loud-tone", {
          toneFrequenciesHz: [TONE_LOW, TONE_HIGH],
          consecutiveWindows: d.loud.streak,
        });
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
        armed: false,
        legA: "",
        legB: "",
        streamSid: "",
        streamReady: false,
        streamReadyAt: null,
        readySent: false,
        phase: null,
        challengeStartedAt: null,
        promptLightDurationMs: null,
        promptEndsAt: null,
        transitionToleranceMs: 250,
        final: null,
        finalDelivered: false,
        callbackDeliveredAt: null,
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
    if (!session || !session.sid) return;
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

    wss.on("connection", (ws) => {
      stats.connections++;
      ws.isAlive = true;
      ws.on("pong", () => { ws.isAlive = true; });

      // Identity comes EXCLUSIVELY from start.customParameters. A socket
      // that fails to identify within identTimeoutMs is closed.
      let session = null;
      let gotStart = false;
      let streamEnded = false;

      const identTimer = setTimeout(() => {
        if (!session) {
          stats.closedNoSid++;
          try { ws.close(); } catch {}
        }
      }, cfg.identTimeoutMs);

      function markReady() {
        if (!session || session.streamReady) return;
        session.streamReady = true;
        session.streamReadyAt = session.streamReadyAt || cfg.now();
        session.awaitingReconnect = false;
        stats.streamsReady++;
        stats.lastSid = session.sid;
        persist();
        cfg.log(`stream ready sid=${session.sid} streamSid=${session.streamSid}`);
        sendStreamReady(session).catch(() => {});
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
          // Canonical identification: nested Twilio parameters only. A valid
          // start carries sid, leg=legB, mode=merge-detection and a valid
          // HMAC token. Query-string session IDs are never consulted.
          if (!p || typeof p !== "object" || Object.keys(p).length === 0) {
            stats.invalidStarts++;
            cfg.log("start without customParameters — closing");
            try { ws.close(4400, "missing customParameters"); } catch {}
            return;
          }
          if (!p.sid || p.leg !== "legB" || p.mode !== "merge-detection") {
            stats.invalidStarts++;
            cfg.log(`invalid start parameters (sid=${p.sid || "?"} leg=${p.leg || "?"} mode=${p.mode || "?"})`);
            try { ws.close(4400, "invalid stream parameters"); } catch {}
            return;
          }
          if (!tokenValid(cfg.secret, p.sid, p.token)) {
            stats.invalidStarts++;
            cfg.log(`invalid stream token sid=${p.sid}`);
            try { ws.close(4403, "invalid stream token"); } catch {}
            return;
          }
          session = getOrCreateSession(p.sid);
          session.leg = "legB";
          session.mode = "merge-detection";
          session.streamSid = streamSid;
          session.ws = ws;
          clearTimeout(identTimer);
          persist();
          return;
        }

        if (msg.event === "stop") {
          if (session && session.ws !== ws) {
            // Stale socket (a newer stream owns this session): its stop must
            // not finalize the live session.
            try { ws.close(); } catch {}
            return;
          }
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
        if (!session) return;
        if (session.ws !== ws) {
          // A newer stream already owns this session: the stale socket's
          // close must not finalize the live session.
          cfg.log(`stale stream closed sid=${session.sid} (ignored)`);
          return;
        }
        cfg.log(`stream closed sid=${session.sid} frames=${session.frames}`);
        if (!streamEnded) handleStreamEnd(session, "stream_stopped", "socket_closed");
      });
      ws.on("error", (err) => {
        stats.lastError = err.message;
        cfg.log(`ws error sid=${session?.sid || "?"}:`, err.message);
        if (session && session.ws === ws && !streamEnded) {
          streamEnded = true;
          handleStreamEnd(session, "stream_error", "socket_error");
        }
      });
    });
  }

  /* ------------------------------ HTTP ----------------------------- */

  function secretOk(req) {
    return !!cfg.secret && req.headers["x-verify-secret"] === cfg.secret;
  }

  function sendJson(res, status, body) {
    res.writeHead(status, { "Content-Type": "application/json" });
    res.end(JSON.stringify(body));
  }

  function readJsonBody(req, res) {
    return readBody(req, cfg.maxBodyBytes).then((body) => {
      try {
        return JSON.parse(body);
      } catch {
        sendJson(res, 400, { error: "invalid_json" });
        return null;
      }
    }).catch((err) => {
      sendJson(res, err.code === 413 ? 413 : 400, { error: err.code === 413 ? "body_too_large" : "bad_request" });
      return null;
    });
  }

  /**
   * POST /arm — pre-register a verification session before the legs start
   * (SPEC §3). Authenticated; validates the merge-tone pair; idempotent.
   */
  async function arm(req, res) {
    if (!secretOk(req)) return sendJson(res, 403, { error: "forbidden" });
    const j = await readJsonBody(req, res);
    if (!j) return;
    if (!j || typeof j.sid !== "string" || !j.sid) return sendJson(res, 400, { error: "sid_required" });
    if (j.tone && (j.tone.low !== TONE_LOW || j.tone.high !== TONE_HIGH)) {
      return sendJson(res, 400, { error: "unsupported_tone", expected: { low: TONE_LOW, high: TONE_HIGH } });
    }
    const session = getOrCreateSession(j.sid);
    if (session.final) return sendJson(res, 409, { error: "session_final", sid: j.sid, final: session.final });
    session.armed = true;
    session.legA = j.legA || session.legA || "";
    session.legB = j.legB || session.legB || "";
    session.mode = j.mode || session.mode || "merge-detection";
    if (j.promptLightDurationMs != null) {
      const d = Number(j.promptLightDurationMs);
      if (!Number.isFinite(d)) return sendJson(res, 400, { error: "invalid_promptLightDurationMs" });
      session.promptLightDurationMs = d;
    }
    if (j.promptEndsAt != null) {
      // Wire type is epoch-ms OR ISO-8601 — normalize to epoch-ms.
      const promptEndsAt = toEpochMs(j.promptEndsAt);
      if (promptEndsAt == null) return sendJson(res, 400, { error: "invalid_promptEndsAt" });
      session.promptEndsAt = promptEndsAt;
    }
    persist();
    sendJson(res, 200, {
      ok: true,
      sid: session.sid,
      armed: true,
      phase: session.phase || null,
      streamReady: session.streamReady,
    });
  }

  /**
   * POST /challenge-start — arm/refresh the two-phase challenge (SPEC §3).
   * Body: { sid, challengeStartedAt, promptLightDurationMs, promptEndsAt,
   * transitionToleranceMs }. 409 until the stream is ready.
   */
  async function challengeStart(req, res) {
    if (!secretOk(req)) return sendJson(res, 403, { error: "forbidden" });
    const j = await readJsonBody(req, res);
    if (!j) return;
    if (!j || typeof j.sid !== "string" || !j.sid) return sendJson(res, 400, { error: "sid_required" });
    const session = sessions.get(j.sid);
    if (!session || !session.streamReady) {
      return sendJson(res, 409, { error: "stream_not_ready", sid: j.sid });
    }
    // Wire type for challengeStartedAt / promptEndsAt is epoch-ms OR
    // ISO-8601 — normalize to epoch-ms before any arithmetic (a raw string
    // would turn `now >= promptEndsAt + tol` into concatenation/NaN and
    // LOUD_DTMF_MODE would never activate).
    let challengeStartedAt = cfg.now();
    if (j.challengeStartedAt != null) {
      challengeStartedAt = toEpochMs(j.challengeStartedAt);
      if (challengeStartedAt == null) return sendJson(res, 400, { error: "invalid_challengeStartedAt" });
    }
    let promptEndsAt;
    if (j.promptEndsAt != null) {
      promptEndsAt = toEpochMs(j.promptEndsAt);
      if (promptEndsAt == null) return sendJson(res, 400, { error: "invalid_promptEndsAt" });
    }
    session.challengeStartedAt = challengeStartedAt;
    // Duration/tolerance fields feed the same phase arithmetic, so validate
    // them too: finite positive/non-negative numbers only, never raw strings.
    if (j.promptLightDurationMs != null) {
      const d = Number(j.promptLightDurationMs);
      if (!Number.isFinite(d)) return sendJson(res, 400, { error: "invalid_promptLightDurationMs" });
      session.promptLightDurationMs = d;
    }
    session.promptLightDurationMs = session.promptLightDurationMs ?? null;
    session.promptEndsAt = promptEndsAt ?? (session.promptLightDurationMs
      ? session.challengeStartedAt + session.promptLightDurationMs
      : null);
    // SPEC name is transitionToleranceMs; phaseToleranceMs accepted as alias.
    const tolRaw = j.transitionToleranceMs ?? j.phaseToleranceMs;
    if (tolRaw != null) {
      const tol = Number(tolRaw);
      if (!Number.isFinite(tol) || tol < 0) return sendJson(res, 400, { error: "invalid_transitionToleranceMs" });
      session.transitionToleranceMs = tol;
    }
    session.transitionToleranceMs = session.transitionToleranceMs ?? 250;
    // Reconstruct the phase from persisted timestamps: an already-expired
    // prompt window starts (or restarts) directly in LOUD_DTMF_MODE.
    session.phase = session.final ? session.phase : reconstructPhase(session) || PHASE_PROMPT_LIGHT;
    persist();
    sendJson(res, 200, {
      ok: true,
      sid: session.sid,
      phase: session.phase,
      promptEndsAt: session.promptEndsAt,
      transitionToleranceMs: session.transitionToleranceMs,
      final: session.final || null,
    });
  }

  /** Demo TwiML — DISABLED unless DEMO_TWIML=1 (restricted by default). */
  function demoTwiml(req, res) {
    if (!cfg.demoTwiml) {
      res.writeHead(404);
      res.end();
      return;
    }
    const kind = req.url.split("/")[2].split("?")[0];
    const base = "https://" + (req.headers.host || "");
    const wssBase = "wss://" + (req.headers.host || "");
    const q = new URL(req.url, "http://localhost").searchParams;
    const sid = q.get("sid") || "demo";
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
        `<Parameter name="token" value="${streamToken(cfg.secret, sid)}"/>` +
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
      sendJson(res, 200, { ok: true, service: "callverify-merge-relay" });
      return;
    }
    if (req.method === "GET" && url === "/ready") {
      const reasons = readiness();
      sendJson(res, reasons.length ? 503 : 200, {
        ready: reasons.length === 0,
        reasons,
        fingerprint: cfg.fingerprint
          ? { asset: cfg.fingerprint.sourceAsset || cfg.fingerprint.asset, durationMs: cfg.fingerprint.durationMs }
          : null,
      });
      return;
    }
    if (req.method === "GET" && url === "/stats") {
      if (!secretOk(req)) return sendJson(res, 403, { error: "forbidden" });
      sendJson(res, 200, {
        ...stats,
        sessions: [...sessions.values()].map((s) => persistSession(s)),
      });
      return;
    }
    if (req.method === "POST" && url === "/arm") {
      arm(req, res);
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

  let closed = false;
  /** Idempotent shutdown; safe before start (listen) and after failures. */
  function stop() {
    if (closed) return;
    closed = true;
    for (const s of sessions.values()) clearTimers(s);
    try { wss.close(); } catch {}
    try {
      // server.close() on a never-listening server is an error — skip it.
      if (server.listening) server.close();
    } catch {}
  }

  return {
    server,
    wss,
    stats,
    sessions,
    cfg,
    finalize,
    streamToken: (sid) => streamToken(cfg.secret, sid),
    close: stop,
    stop,
  };
}

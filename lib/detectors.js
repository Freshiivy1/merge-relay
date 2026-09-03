/**
 * Audio detectors used by the two-phase merge-detection state machine.
 *
 * - MergeToneDetector: the EXISTING loud-tone detector. Thresholds and
 *   semantics are intentionally unchanged: 852+1336 Hz (the DTMF-8 pair),
 *   energy floor 1e6, dual-frequency requirement, six consecutive 50 ms
 *   windows, idempotent fire.
 * - LightToneDetector: same Goertzel pair but with its own, separately
 *   configurable thresholds for the quiet watermark mixed under the prompt.
 * - PromptFingerprintMatcher: DSP fingerprint match (normalized cross
 *   correlation of raw PCM against the committed anchor) — no speech-to-text.
 */
import { goertzelPower, appendMulawFrame } from "./dsp.js";

export const WIN = 400; // 50 ms windows @ 8 kHz

/* ------------------------------------------------------------------ */
/* Loud merge-tone detector (existing behavior — do not weaken)        */
/* ------------------------------------------------------------------ */

const LOUD_TONE_RATIO = 0.05; // p/(E·N²): dual-tone ≈ 0.25, noise ≈ 0.0025
const LOUD_ENERGY_FLOOR = 1e6;
const LOUD_NEED_WINDOWS = 6; // 300 ms continuous tone → fire

export class MergeToneDetector {
  constructor() {
    this.buf = [];
    this.streak = 0;
    this.fired = false;
  }
  /** Feed one base64 μ-law frame (20 ms); returns true exactly once on fire. */
  push(payloadB64) {
    if (this.fired) return false;
    appendMulawFrame(this.buf, payloadB64);
    while (this.buf.length >= WIN) {
      const window = this.buf.slice(0, WIN);
      this.buf = this.buf.slice(WIN);
      let e = 0;
      for (let i = 0; i < window.length; i++) e += window[i] * window[i];
      e /= window.length;
      const norm = e * WIN * WIN;
      const hit =
        e > LOUD_ENERGY_FLOOR &&
        goertzelPower(window, 852) / norm > LOUD_TONE_RATIO &&
        goertzelPower(window, 1336) / norm > LOUD_TONE_RATIO;
      this.streak = hit ? this.streak + 1 : 0;
      if (this.streak >= LOUD_NEED_WINDOWS) {
        this.fired = true;
        return true;
      }
    }
    return false;
  }
}

/* ------------------------------------------------------------------ */
/* Light watermark detector (Phase 1 only)                             */
/* ------------------------------------------------------------------ */

export class LightToneDetector {
  /**
   * @param {object} opts
   * @param {number} opts.ratioFloor  normalized Goertzel power floor p/(E·N²)
   * @param {number} opts.energyMin   minimum mean-square energy per window
   * @param {number} opts.needWindows consecutive 50 ms windows required
   */
  constructor({ ratioFloor = 1e-3, energyMin = 1e3, needWindows = 6 } = {}) {
    this.ratioFloor = ratioFloor;
    this.energyMin = energyMin;
    this.needWindows = needWindows;
    this.buf = [];
    this.streak = 0;
    this.fired = false;
    this.windows = 0;
    this.hitWindows = 0;
  }
  /** Feed one base64 μ-law frame; returns true exactly once on fire. */
  push(payloadB64) {
    if (this.fired) return false;
    appendMulawFrame(this.buf, payloadB64);
    while (this.buf.length >= WIN) {
      const window = this.buf.slice(0, WIN);
      this.buf = this.buf.slice(WIN);
      this.windows++;
      let e = 0;
      for (let i = 0; i < window.length; i++) e += window[i] * window[i];
      e /= window.length;
      const norm = e * WIN * WIN;
      const hit =
        e > this.energyMin &&
        goertzelPower(window, 852) / norm > this.ratioFloor &&
        goertzelPower(window, 1336) / norm > this.ratioFloor;
      if (hit) this.hitWindows++;
      this.streak = hit ? this.streak + 1 : 0;
      if (this.streak >= this.needWindows) {
        this.fired = true;
        return true;
      }
    }
    return false;
  }
}

/* ------------------------------------------------------------------ */
/* Prompt fingerprint matcher (Phase 1 only)                           */
/* ------------------------------------------------------------------ */

export class PromptFingerprintMatcher {
  /**
   * @param {object} fingerprint parsed prompt-fingerprint.json
   * @param {number} [threshold] NCC threshold (default: fingerprint value)
   */
  constructor(fingerprint, threshold) {
    const fp = fingerprint?.promptFingerprint;
    if (!fp || fp.algorithm !== "normalized-cross-correlation-pcm16" || !fp.anchorPcm16Base64) {
      throw new Error("unsupported prompt fingerprint");
    }
    if (fingerprint.sampleRate && fingerprint.sampleRate !== 8000) {
      throw new Error(`unsupported fingerprint sampleRate ${fingerprint.sampleRate}`);
    }
    const raw = Buffer.from(fp.anchorPcm16Base64, "base64");
    this.anchor = new Float64Array(raw.length / 2);
    for (let i = 0; i < this.anchor.length; i++) this.anchor[i] = raw.readInt16LE(i * 2);
    let aa = 0;
    for (let i = 0; i < this.anchor.length; i++) aa += this.anchor[i] * this.anchor[i];
    this.anchorEnergy = aa;
    this.threshold = threshold ?? fp.threshold ?? 0.5;
    this.buf = [];
    this.matched = false;
    this.bestScore = 0;
    this.hop = WIN; // score once per 50 ms hop
  }
  /** Normalized cross-correlation of the anchor with the newest full window. */
  scoreLatest() {
    const n = this.anchor.length;
    const x = this.buf.slice(this.buf.length - n);
    let dot = 0;
    let xx = 0;
    for (let i = 0; i < n; i++) {
      dot += this.anchor[i] * x[i];
      xx += x[i] * x[i];
    }
    const denom = Math.sqrt(this.anchorEnergy * xx);
    return denom > 0 ? dot / denom : 0;
  }
  /** Feed one base64 μ-law frame; returns true exactly once on match. */
  push(payloadB64) {
    if (this.matched) return false;
    appendMulawFrame(this.buf, payloadB64);
    let fired = false;
    // Score every `hop` new samples once a full anchor window is buffered.
    while (this.buf.length >= this.anchor.length + this.hop) {
      this.buf = this.buf.slice(this.hop);
      const score = this.scoreLatest();
      if (score > this.bestScore) this.bestScore = score;
      if (score >= this.threshold) {
        this.matched = true;
        fired = true;
        break;
      }
    }
    return fired;
  }
}

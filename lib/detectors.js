/**
 * Audio detectors used by the two-phase merge-detection state machine.
 *
 * - MergeToneDetector: the EXISTING loud-tone detector. Thresholds and
 *   semantics are intentionally unchanged: 852+1336 Hz (the DTMF-8 pair),
 *   energy floor 1e6, dual-frequency requirement, six consecutive 50 ms
 *   windows, idempotent fire.
 * - LightToneDetector: same Goertzel pair but with its own, separately
 *   configurable thresholds for the quiet watermark mixed under the prompt.
 *   Default floor (3e-3) is calibrated against the reference asset: the
 *   -21 dB watermark fires reliably (max ratio ≈ 0.25) while speech-only
 *   and noise never sustain a 6-window streak.
 * - PromptFingerprintMatcher: telephony-tolerant spectral matcher consuming
 *   prompt-fingerprint.json (algorithm "normalized-log-band-spectral-contour-v1").
 *   Per-frame log-band contour vectors are compared by Pearson correlation
 *   (invariant to channel gain); a sliding 2 s window is aligned against the
 *   reference contour and the best mean correlation is the match score. A
 *   temporal-variation guard rejects stationary tones/noise. No
 *   speech-to-text, no raw-PCM anchor.
 */
import {
  goertzelPower, appendMulawFrame,
  CONTOUR_FRAME, CONTOUR_HOP, bandBinRanges, frameLogBands, pearsonPrep,
} from "./dsp.js";

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
  constructor({ ratioFloor = 3e-3, energyMin = 1e3, needWindows = 6 } = {}) {
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
   *   (algorithm "normalized-log-band-spectral-contour-v1")
   * @param {number} [threshold] match score override (default: fingerprint value)
   */
  constructor(fingerprint, threshold) {
    if (
      !fingerprint ||
      fingerprint.algorithm !== "normalized-log-band-spectral-contour-v1" ||
      !Array.isArray(fingerprint.frames) || fingerprint.frames.length === 0 ||
      !Array.isArray(fingerprint.bandEdgesHz)
    ) {
      throw new Error("unsupported prompt fingerprint");
    }
    if (fingerprint.sampleRate && fingerprint.sampleRate !== 8000) {
      throw new Error(`unsupported fingerprint sampleRate ${fingerprint.sampleRate}`);
    }
    const frames = fingerprint.frames;
    this.nBands = fingerprint.bandEdgesHz.length - 1;
    this.nRef = frames.length;
    this.ranges = bandBinRanges(fingerprint.bandEdgesHz);
    const match = fingerprint.match || {};
    this.windowFrames = match.windowFrames ?? 80; // 2 s sliding window
    this.threshold = threshold ?? match.threshold ?? 0.75;
    this.energyGateRms = match.energyGateRms ?? 50;
    this.minTemporalStd = match.minTemporalStd ?? 0.4;
    if (this.nRef < this.windowFrames) throw new Error("fingerprint shorter than match window");
    // Precompute per-frame unit (mean-centered, normalized) reference vectors.
    this.refUnit = new Float64Array(this.nRef * this.nBands);
    this.refNormOk = new Uint8Array(this.nRef);
    for (let f = 0; f < this.nRef; f++) {
      const row = frames[f];
      if (!Array.isArray(row) || row.length !== this.nBands) {
        throw new Error(`fingerprint frame ${f} must have ${this.nBands} bands`);
      }
      const { unit, norm } = pearsonPrep(Float64Array.from(row));
      this.refUnit.set(unit, f * this.nBands);
      this.refNormOk[f] = norm > 0 ? 1 : 0;
    }
    this.buf = []; // PCM sample backlog
    // Sliding window of incoming vectors (ring buffers).
    this.inUnit = new Float64Array(this.windowFrames * this.nBands);
    this.inRaw = new Float64Array(this.windowFrames * this.nBands); // raw log bands
    this.inOk = new Uint8Array(this.windowFrames);
    this.inCount = 0; // total frames seen
    this.matched = false;
    this.bestScore = 0;
    this.bestOffset = -1;
  }

  /**
   * Temporal-variation guard: mean per-band temporal std of the raw log-band
   * energies across the current window. Stationary signals (a pure merge
   * tone, steady noise) score near zero and can never match the prompt,
   * even where their static band shape resembles a speech segment.
   */
  temporalStd() {
    const W = this.windowFrames;
    const B = this.nBands;
    const start = this.inCount - W;
    let total = 0;
    for (let b = 0; b < B; b++) {
      let mean = 0;
      let count = 0;
      for (let j = 0; j < W; j++) {
        const slot = (start + j) % W;
        if (!this.inOk[slot]) continue;
        mean += this.inRaw[slot * B + b];
        count++;
      }
      if (count < 2) continue;
      mean /= count;
      let v = 0;
      for (let j = 0; j < W; j++) {
        const slot = (start + j) % W;
        if (!this.inOk[slot]) continue;
        const d = this.inRaw[slot * B + b] - mean;
        v += d * d;
      }
      total += Math.sqrt(v / count);
    }
    return total / B;
  }

  /** Best mean per-frame Pearson of the current window over all offsets. */
  scoreWindow() {
    const W = this.windowFrames;
    const B = this.nBands;
    const quorum = Math.ceil(W * 0.75);
    const start = this.inCount - W; // absolute index of window's first frame
    let best = -1;
    let bestOff = -1;
    const maxOff = this.nRef - W;
    for (let off = 0; off <= maxOff; off++) {
      let acc = 0;
      let valid = 0;
      for (let j = 0; j < W; j++) {
        const slot = (start + j) % W;
        if (!this.inOk[slot] || !this.refNormOk[off + j]) continue; // gated frames excluded
        const inIdx = slot * B;
        const refIdx = (off + j) * B;
        let dot = 0;
        for (let b = 0; b < B; b++) dot += this.inUnit[inIdx + b] * this.refUnit[refIdx + b];
        acc += dot;
        valid++;
      }
      if (valid < quorum) continue;
      const score = acc / valid;
      if (score > best) { best = score; bestOff = off; }
    }
    return { best, bestOff };
  }

  /** Feed one base64 μ-law frame; returns true exactly once on match. */
  push(payloadB64) {
    if (this.matched) return false;
    appendMulawFrame(this.buf, payloadB64);
    const frame = new Float64Array(CONTOUR_FRAME);
    while (this.buf.length >= CONTOUR_FRAME) {
      for (let i = 0; i < CONTOUR_FRAME; i++) frame[i] = this.buf[i];
      this.buf = this.buf.slice(CONTOUR_HOP);
      const { bands, rms } = frameLogBands(frame, this.ranges);
      const slot = this.inCount % this.windowFrames;
      if (rms >= this.energyGateRms) {
        const { unit, norm } = pearsonPrep(bands);
        this.inUnit.set(unit, slot * this.nBands);
        this.inRaw.set(bands, slot * this.nBands);
        this.inOk[slot] = norm > 0 ? 1 : 0;
      } else {
        this.inOk[slot] = 0; // energy gate: frame excluded from matching
      }
      this.inCount++;
      if (this.inCount >= this.windowFrames) {
        const { best, bestOff } = this.scoreWindow();
        if (best > this.bestScore) {
          this.bestScore = best;
          this.bestOffset = bestOff;
        }
        if (best >= this.threshold && this.temporalStd() >= this.minTemporalStd) {
          this.matched = true;
          this.bestScore = best;
          this.bestOffset = bestOff;
          return true;
        }
      }
    }
    return false;
  }
}

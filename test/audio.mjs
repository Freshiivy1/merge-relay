/**
 * Synthetic-audio helpers for tests: μ-law encoding and the four signal
 * classes the two-phase state machine must distinguish. All vectors are
 * derived from the committed fingerprint asset or pure synthesis — the
 * measured behavior was calibrated against the real prompt+light WAV:
 *
 *   anchor replay   -> prompt NCC ≈ 0.999, light streak ≥ 6   (prompt+light)
 *   notched anchor  -> prompt NCC ≈ 0.99,  light streak ≤ 1   (prompt only)
 *   quiet dual tone -> NCC ≈ 0.05, light fires, loud cannot   (light only)
 *   loud  dual tone -> loud detector fires                     (merge tone)
 */
import fs from "fs";

export const SAMPLE_RATE = 8000;

export function loadAnchor() {
  const fp = JSON.parse(fs.readFileSync(new URL("../prompt-fingerprint.json", import.meta.url), "utf8"));
  const raw = Buffer.from(fp.promptFingerprint.anchorPcm16Base64, "base64");
  const pcm = new Float64Array(raw.length / 2);
  for (let i = 0; i < pcm.length; i++) pcm[i] = raw.readInt16LE(i * 2);
  return { fingerprint: fp, anchor: pcm };
}

export function encodeMulaw(s) {
  const BIAS = 0x84, CLIP = 32604;
  const sign = s < 0 ? 0x80 : 0;
  let mag = Math.min(Math.abs(Math.round(s)), CLIP) + BIAS;
  let exp = 7;
  for (let mask = 0x4000; (mag & mask) === 0 && exp > 0; mask >>= 1) exp--;
  return ~(sign | (exp << 4) | ((mag >> (exp + 3)) & 0x0f)) & 0xff;
}

/** PCM samples (array-like) -> array of base64 20 ms μ-law frames. */
export function toFrames(pcm) {
  const frames = [];
  for (let off = 0; off + 160 <= pcm.length; off += 160) {
    const b = Buffer.alloc(160);
    for (let j = 0; j < 160; j++) b[j] = encodeMulaw(pcm[off + j]);
    frames.push(b.toString("base64"));
  }
  return frames;
}

export function dualTone({ amplitude = 12000, ms = 1000, freqs = [852, 1336] }) {
  const n = Math.floor((ms / 1000) * SAMPLE_RATE);
  const pcm = new Float64Array(n);
  for (let i = 0; i < n; i++) {
    for (const f of freqs) pcm[i] += amplitude * Math.sin((2 * Math.PI * f * i) / SAMPLE_RATE);
  }
  return pcm;
}

/** Second-order IIR notch (RBJ), used to strip the watermark from the anchor. */
export function notch(pcm, freq, Q = 30) {
  const w0 = (2 * Math.PI * freq) / SAMPLE_RATE;
  const alpha = Math.sin(w0) / (2 * Q);
  const b0 = 1, b1 = -2 * Math.cos(w0), b2 = 1;
  const a0 = 1 + alpha, a1 = -2 * Math.cos(w0), a2 = 1 - alpha;
  const out = new Float64Array(pcm.length);
  let x1 = 0, x2 = 0, y1 = 0, y2 = 0;
  for (let i = 0; i < pcm.length; i++) {
    out[i] = (b0 / a0) * pcm[i] + (b1 / a0) * x1 + (b2 / a0) * x2 - (a1 / a0) * y1 - (a2 / a0) * y2;
    x2 = x1; x1 = pcm[i]; y2 = y1; y1 = out[i];
  }
  return out;
}

function loop(pcm, times) {
  const out = new Float64Array(pcm.length * times);
  for (let i = 0; i < times; i++) out.set(pcm, i * pcm.length);
  return out;
}

/** prompt fingerprint + light watermark (both Phase 1 signals present) */
export function promptLightFrames() {
  const { anchor } = loadAnchor();
  return toFrames(loop(anchor, 2));
}
/** prompt fingerprint only (watermark removed by notch filters) */
export function promptOnlyFrames() {
  const { anchor } = loadAnchor();
  return toFrames(loop(notch(notch(anchor, 852), 1336), 2));
}
/** light watermark only (quiet DTMF-8 pair, below the loud energy floor) */
export function lightOnlyFrames(ms = 1200) {
  return toFrames(dualTone({ amplitude: 600, ms }));
}
/** the existing loud merge tone (Phase 2) */
export function loudToneFrames(ms = 1200) {
  return toFrames(dualTone({ amplitude: 12000, ms }));
}
export function silenceFrames(ms = 1000) {
  return toFrames(new Float64Array(Math.floor((ms / 1000) * SAMPLE_RATE)));
}
export function noiseFrames(ms = 1000, amplitude = 3000) {
  const n = Math.floor((ms / 1000) * SAMPLE_RATE);
  const pcm = new Float64Array(n);
  let seed = 42;
  for (let i = 0; i < n; i++) {
    seed = (seed * 1103515245 + 12345) & 0x7fffffff;
    pcm[i] = ((seed / 0x40000000) - 1) * amplitude;
  }
  return toFrames(pcm);
}

/**
 * Audio helpers for tests: μ-law encode/decode, the REAL prompt+watermark
 * asset (Twilio-format μ-law, committed under test/fixtures/), and the
 * signal classes the two-phase state machine must distinguish:
 *
 *   real prompt+light (fixture) -> prompt score ≈ 1.0, light fires (prompt+light)
 *   notched fixture             -> prompt matches, light never   (prompt only)
 *   quiet dual tone             -> light fires, prompt never     (light only)
 *   loud  dual tone             -> loud detector fires           (merge tone)
 *   noise / silence             -> nothing fires
 */
import fs from "fs";

export const SAMPLE_RATE = 8000;

export function loadFingerprint() {
  return JSON.parse(fs.readFileSync(new URL("../prompt-fingerprint.json", import.meta.url), "utf8"));
}

export function decodeMulawByte(u) {
  u = ~u & 0xff;
  let t = ((u & 0x0f) << 3) + 0x84;
  t <<= (u & 0x70) >> 4;
  return u & 0x80 ? 0x84 - t : t - 0x84;
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

/** Raw μ-law bytes of the committed prompt+watermark fixture. */
export function fixtureMulawBytes() {
  const buf = fs.readFileSync(new URL("./fixtures/call-waiting-prompt-light-mulaw.wav", import.meta.url));
  let off = 12;
  while (off + 8 <= buf.length) {
    const id = buf.toString("ascii", off, off + 4);
    const size = buf.readUInt32LE(off + 4);
    if (id === "data") return buf.subarray(off + 8, off + 8 + size);
    off += 8 + size + (size & 1);
  }
  throw new Error("fixture: no data chunk");
}

/** Fixture decoded to PCM16 samples (Float64Array). */
export function fixturePcm() {
  const bytes = fixtureMulawBytes();
  const pcm = new Float64Array(bytes.length);
  for (let i = 0; i < bytes.length; i++) pcm[i] = decodeMulawByte(bytes[i]);
  return pcm;
}

/** Real prompt + light watermark, exactly as Twilio would stream it. */
export function promptLightFrames() {
  const bytes = fixtureMulawBytes();
  const frames = [];
  for (let off = 0; off + 160 <= bytes.length; off += 160) {
    frames.push(bytes.subarray(off, off + 160).toString("base64"));
  }
  return frames;
}

/** Second-order IIR notch (RBJ), used to strip the watermark from the prompt. */
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

/** prompt fingerprint only (watermark removed from the real asset) */
export function promptOnlyFrames() {
  return toFrames(notch(notch(fixturePcm(), 852), 1336));
}

export function dualTone({ amplitude = 12000, ms = 1000, freqs = [852, 1336] }) {
  const n = Math.floor((ms / 1000) * SAMPLE_RATE);
  const pcm = new Float64Array(n);
  for (let i = 0; i < n; i++) {
    for (const f of freqs) pcm[i] += amplitude * Math.sin((2 * Math.PI * f * i) / SAMPLE_RATE);
  }
  return pcm;
}

/** light watermark only (quiet DTMF-8 pair, below the loud energy floor) */
export function lightOnlyFrames(ms = 19000) {
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

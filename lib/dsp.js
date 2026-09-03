/**
 * DSP primitives shared by all detectors: μ-law decode + Goertzel power.
 * These are byte-for-byte the same algorithms the relay has always used.
 */

export const SAMPLE_RATE = 8000;

export function decodeMulaw(u) {
  u = ~u & 0xff;
  let t = ((u & 0x0f) << 3) + 0x84;
  t <<= (u & 0x70) >> 4;
  return u & 0x80 ? 0x84 - t : t - 0x84;
}

export function goertzelPower(samples, freq, sampleRate = SAMPLE_RATE) {
  const w = (2 * Math.PI * freq) / sampleRate;
  const cw = 2 * Math.cos(w);
  let s1 = 0;
  let s2 = 0;
  for (let i = 0; i < samples.length; i++) {
    const s0 = samples[i] + cw * s1 - s2;
    s2 = s1;
    s1 = s0;
  }
  return s1 * s1 + s2 * s2 - cw * s1 * s2;
}

/** Decode one base64 μ-law frame and append its PCM samples to `out`. */
export function appendMulawFrame(out, payloadB64) {
  const bytes = Buffer.from(payloadB64, "base64");
  for (const b of bytes) out.push(decodeMulaw(b));
  return bytes.length;
}

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

/* ------------------------------------------------------------------ */
/* Spectral-contour extraction (normalized-log-band-spectral-contour-v1) */
/* ------------------------------------------------------------------ */

export const CONTOUR_FRAME = 400; // 50 ms @ 8 kHz
export const CONTOUR_HOP = 200;   // 25 ms @ 8 kHz
export const CONTOUR_NFFT = 512;  // zero-padded FFT size
const LOG_EPS = 1e-12;

const HANN = (() => {
  const w = new Float64Array(CONTOUR_FRAME);
  for (let i = 0; i < CONTOUR_FRAME; i++) {
    w[i] = 0.5 * (1 - Math.cos((2 * Math.PI * i) / (CONTOUR_FRAME - 1)));
  }
  return w;
})();

/**
 * In-place iterative radix-2 FFT. `re`/`im` must be Float64Array(nfft).
 * Returns nothing; results are in re/im.
 */
export function fftInPlace(re, im) {
  const n = re.length;
  for (let i = 1, j = 0; i < n; i++) {
    let bit = n >> 1;
    for (; j & bit; bit >>= 1) j ^= bit;
    j ^= bit;
    if (i < j) {
      const tr = re[i]; re[i] = re[j]; re[j] = tr;
      const ti = im[i]; im[i] = im[j]; im[j] = ti;
    }
  }
  for (let len = 2; len <= n; len <<= 1) {
    const ang = (-2 * Math.PI) / len;
    const wr = Math.cos(ang), wi = Math.sin(ang);
    for (let i = 0; i < n; i += len) {
      let cwr = 1, cwi = 0;
      for (let k = 0; k < len / 2; k++) {
        const ur = re[i + k], ui = im[i + k];
        const vr = re[i + k + len / 2] * cwr - im[i + k + len / 2] * cwi;
        const vi = re[i + k + len / 2] * cwi + im[i + k + len / 2] * cwr;
        re[i + k] = ur + vr; im[i + k] = ui + vi;
        re[i + k + len / 2] = ur - vr; im[i + k + len / 2] = ui - vi;
        const nwr = cwr * wr - cwi * wi;
        cwi = cwr * wi + cwi * wr;
        cwr = nwr;
      }
    }
  }
}

/** Precompute FFT bin index ranges for each log-spaced band. */
export function bandBinRanges(bandEdgesHz, nfft = CONTOUR_NFFT, sampleRate = SAMPLE_RATE) {
  const binHz = sampleRate / nfft;
  const ranges = [];
  for (let b = 0; b < bandEdgesHz.length - 1; b++) {
    const lo = Math.max(1, Math.ceil(bandEdgesHz[b] / binHz));
    const hi = Math.min(nfft / 2, Math.ceil(bandEdgesHz[b + 1] / binHz));
    ranges.push([lo, Math.max(hi, lo + 1)]);
  }
  return ranges;
}

const fftRe = new Float64Array(CONTOUR_NFFT);
const fftIm = new Float64Array(CONTOUR_NFFT);

/**
 * log10 band energies of one 400-sample frame for the given band ranges.
 * Returns { bands: Float64Array, rms } — rms is the frame RMS (pre-window).
 */
export function frameLogBands(frame, ranges) {
  let rms = 0;
  fftRe.fill(0);
  fftIm.fill(0);
  for (let i = 0; i < CONTOUR_FRAME; i++) {
    rms += frame[i] * frame[i];
    fftRe[i] = frame[i] * HANN[i];
  }
  rms = Math.sqrt(rms / CONTOUR_FRAME);
  fftInPlace(fftRe, fftIm);
  const bands = new Float64Array(ranges.length);
  for (let b = 0; b < ranges.length; b++) {
    const [lo, hi] = ranges[b];
    let p = 0;
    for (let k = lo; k < hi; k++) p += fftRe[k] * fftRe[k] + fftIm[k] * fftIm[k];
    bands[b] = Math.log10(p + LOG_EPS);
  }
  return { bands, rms };
}

/** Mean-center a band vector and return { centered, norm } (Pearson prep). */
export function pearsonPrep(bands) {
  const n = bands.length;
  let mean = 0;
  for (let i = 0; i < n; i++) mean += bands[i];
  mean /= n;
  const centered = new Float64Array(n);
  let norm = 0;
  for (let i = 0; i < n; i++) {
    centered[i] = bands[i] - mean;
    norm += centered[i] * centered[i];
  }
  norm = Math.sqrt(norm);
  if (norm > 0) for (let i = 0; i < n; i++) centered[i] /= norm; // unit vector
  return { unit: centered, norm };
}

/**
 * Global min-max normalization of a (frames × bands) matrix, in place.
 * Used by the fingerprint generator (documented extraction step).
 */
export function minMaxNormalize(frames) {
  let lo = Infinity, hi = -Infinity;
  for (const row of frames) for (const v of row) { if (v < lo) lo = v; if (v > hi) hi = v; }
  const span = hi - lo || 1;
  for (const row of frames) for (let i = 0; i < row.length; i++) row[i] = (row[i] - lo) / span;
  return frames;
}

#!/usr/bin/env node
/**
 * Regenerate prompt-fingerprint.json from the rendered Phase 1 asset.
 *
 * Usage: node tools/generate-fingerprint.mjs <prompt-light.wav> [out.json]
 *
 * The asset must be 8 kHz mono PCM16 WAV (speech + 852+1336 Hz watermark
 * mixed ~21 dB below prompt RMS). The measured WAV duration is authoritative
 * and is written into the fingerprint as durationMs (consumed by the relay
 * and asserted by tests).
 *
 * Extraction (algorithm "normalized-log-band-spectral-contour-v1"):
 *   frame 400 samples (50 ms), hop 200 (25 ms), Hann window, 512-point FFT,
 *   per-band summed power over 16 log-spaced bands 300–3400 Hz, log10,
 *   global min-max normalization to [0,1]. The relay matcher is invariant
 *   to any per-frame affine transform of these values (per-frame Pearson),
 *   so fingerprints produced by an equivalent extractor remain consumable.
 */
import fs from "fs";
import crypto from "crypto";
import {
  SAMPLE_RATE, CONTOUR_FRAME, CONTOUR_HOP, CONTOUR_NFFT,
  bandBinRanges, frameLogBands, minMaxNormalize,
} from "../lib/dsp.js";

const BAND_EDGES_HZ = [
  300.0, 349.155, 406.365, 472.948, 550.441, 640.631, 745.599, 867.766,
  1009.95, 1175.432, 1368.027, 1592.18, 1853.06, 2156.686, 2510.061,
  2921.336, 3400.0,
];

function readWavPcm16(path) {
  const buf = fs.readFileSync(path);
  if (buf.toString("ascii", 0, 4) !== "RIFF" || buf.toString("ascii", 8, 12) !== "WAVE") {
    throw new Error("not a RIFF/WAVE file: " + path);
  }
  let fmt = null;
  let data = null;
  for (let off = 12; off + 8 <= buf.length;) {
    const id = buf.toString("ascii", off, off + 4);
    const size = buf.readUInt32LE(off + 4);
    if (id === "fmt ") {
      fmt = {
        format: buf.readUInt16LE(off + 8),
        channels: buf.readUInt16LE(off + 10),
        sampleRate: buf.readUInt32LE(off + 12),
        bits: buf.readUInt16LE(off + 22),
      };
    } else if (id === "data") {
      data = buf.subarray(off + 8, off + 8 + size);
    }
    off += 8 + size + (size & 1);
  }
  if (!fmt || !data) throw new Error("missing fmt/data chunk: " + path);
  if (fmt.format !== 1 || fmt.channels !== 1 || fmt.bits !== 16 || fmt.sampleRate !== SAMPLE_RATE) {
    throw new Error(`need 8 kHz mono PCM16 WAV, got ${JSON.stringify(fmt)}`);
  }
  const pcm = new Float64Array(data.length / 2);
  for (let i = 0; i < pcm.length; i++) pcm[i] = data.readInt16LE(i * 2);
  return { pcm, rawPcm: data };
}

const [, , wavPath, outPath = new URL("../prompt-fingerprint.json", import.meta.url).pathname] = process.argv;
if (!wavPath) {
  console.error("usage: node tools/generate-fingerprint.mjs <prompt-light.wav> [out.json]");
  process.exit(2);
}

const { pcm, rawPcm } = readWavPcm16(wavPath);
const durationMs = Math.round((pcm.length / SAMPLE_RATE) * 1000);
const ranges = bandBinRanges(BAND_EDGES_HZ);

const nFrames = Math.floor((pcm.length - CONTOUR_FRAME) / CONTOUR_HOP) + 1;
const frame = new Float64Array(CONTOUR_FRAME);
const frames = [];
for (let f = 0; f < nFrames; f++) {
  for (let i = 0; i < CONTOUR_FRAME; i++) frame[i] = pcm[f * CONTOUR_HOP + i];
  const { bands } = frameLogBands(frame, ranges);
  frames.push(Array.from(bands, (v) => v));
}
minMaxNormalize(frames);
for (const row of frames) for (let i = 0; i < row.length; i++) row[i] = Number(row[i].toFixed(6));

const fingerprint = {
  version: 1,
  algorithm: "normalized-log-band-spectral-contour-v1",
  sourceAsset: wavPath.split("/").pop(),
  sampleRate: SAMPLE_RATE,
  frameMs: (CONTOUR_FRAME / SAMPLE_RATE) * 1000,
  hopMs: (CONTOUR_HOP / SAMPLE_RATE) * 1000,
  durationMs, // measured from the rendered WAV — authoritative Phase 1 length
  bandEdgesHz: BAND_EDGES_HZ,
  extraction: {
    window: "hann",
    nfft: CONTOUR_NFFT,
    bandAggregate: "sum-power",
    transform: "log10",
    normalize: "global-minmax-0-1",
    note: "matcher is invariant to per-frame affine transforms (per-frame Pearson)",
  },
  match: {
    windowFrames: 80,      // 2 s sliding window (hop 25 ms)
    threshold: 0.75,       // mean per-frame Pearson; measured: prompt 1.0, noisy 0.81, pure tone ≤ 0.61
    energyGateRms: 50,     // PCM16 RMS floor per frame
    minTemporalStd: 0.4,   // raw log-band temporal std floor; speech ≥ 0.78, stationary tone ≈ 0.08
  },
  lightTone: {
    frequenciesHz: [852, 1336],
    watermarkDbBelowPromptRms: 21,
    ratioFloor: 3e-3, // normalized Goertzel p/(E·N²); measured: asset ≈ 0.25 max, speech-only/noise < 0.01 without 6-window streak
    consecutiveWindows: 6,
    windowSamples: 400,
  },
  pcmSha256: crypto.createHash("sha256").update(rawPcm).digest("hex"),
  frames,
};

fs.writeFileSync(outPath, JSON.stringify(fingerprint));
console.log(`wrote ${outPath}: ${frames.length} frames, durationMs=${durationMs}`);

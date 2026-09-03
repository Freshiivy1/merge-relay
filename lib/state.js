/**
 * Atomic JSON persistence for relay session state.
 * Writes go to `STATE_FILE.tmp` and are renamed into place, so a crash
 * mid-write can never leave a truncated state file.
 */
import fs from "fs";
import path from "path";

const MAX_AGE_MS = 24 * 60 * 60 * 1000; // prune sessions older than 24 h

export function loadState(stateFile) {
  try {
    const raw = fs.readFileSync(stateFile, "utf8");
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object" || typeof parsed.sessions !== "object") {
      return { sessions: {} };
    }
    const now = Date.now();
    for (const [sid, s] of Object.entries(parsed.sessions)) {
      const age = now - (s?.updatedAt || 0);
      if (!s || typeof s !== "object" || age > MAX_AGE_MS) delete parsed.sessions[sid];
    }
    return parsed;
  } catch {
    return { sessions: {} };
  }
}

export function saveState(stateFile, sessions) {
  const dir = path.dirname(stateFile);
  const tmp = stateFile + ".tmp";
  const payload = JSON.stringify({ version: 1, savedAt: new Date().toISOString(), sessions }, null, 1);
  fs.writeFileSync(tmp, payload);
  fs.renameSync(tmp, stateFile);
  return dir;
}

/** True when the state file location is writable (used by /ready). */
export function stateFileWritable(stateFile) {
  try {
    // Append-mode open probes writability without truncating existing state.
    fs.closeSync(fs.openSync(stateFile, "a"));
    return true;
  } catch {
    return false;
  }
}

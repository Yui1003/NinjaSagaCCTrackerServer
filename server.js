/**
 * HCLD Bleed Tracker — Always-On Background Poller
 *
 * Replicates the browser tracker's 5-second polling loop on the server so
 * cheat-reversal events (member rep peak→drop) and clan deduction changes are
 * caught 24/7 even when no browser tab is open.
 *
 * Detected events are written to the same Firestore collections
 * (suspectLog / deductionLog) that the HTML tracker already listens to,
 * so all devices see server-caught events the moment they open the tracker.
 */

'use strict';

// ─── Firebase web SDK (same config as the HTML tracker) ──────────────────────
import { initializeApp } from 'firebase/app';
import {
  getFirestore,
  collection,
  doc,
  setDoc,
  serverTimestamp,
} from 'firebase/firestore';

// ─── Express health check ─────────────────────────────────────────────────────
import express from 'express';

// ─── Config ───────────────────────────────────────────────────────────────────
const API_URL    = 'https://static.ninjasaga.cc/data/clan_rankings.json';
const POLL_MS    = 5000;   // poll every 5 seconds (same as browser)
const PORT       = process.env.PORT || 5000;

const FIREBASE_CONFIG = {
  apiKey:            'AIzaSyBt1VtMUsUd_pVOG6sKbCJTHS2bka8kVpo',
  authDomain:        'clantracker-22435.firebaseapp.com',
  projectId:         'clantracker-22435',
  storageBucket:     'clantracker-22435.firebasestorage.app',
  messagingSenderId: '354867534107',
  appId:             '1:354867534107:web:8452c807f24ef26dcedbbe',
};

// ─── Round constants (same as HTML) ──────────────────────────────────────────
const ROUND_SWITCH_GRACE_MS        = 15000;
const BLEED_EVENT_TTL_MS           = 15000;
const BLEED_MULTI_MEMBER_WINDOW_MS = 5000;
const BLEED_SINGLE_MEMBER_WINDOW_MS = 10000;

// ─── In-memory state (equivalent of the browser's `state` object) ─────────────
let prevData         = null;   // previous API response
let currentData      = null;   // latest API response

// Per-member peak tracking. Key: `${clanId}_${memberId}`
// Value: { peak, peakTs, dropAmount, dropTs, dropCount }
let memberPeakRep    = {};

// Per-member gain-event timestamps. Key: `${clanId}_${memberId}` → [ts, ...]
let memberGainEvents = {};

// Clan activity (last rep change). Key: clanId → { amount, ts }
let clanActivity     = {};

// Current 30-min round
let round = { id: null, startTs: null, startSnap: null };

// Already-written dedupKeys (prevents double-writes across polls)
const suspectKeysSeen   = new Set();
// Already-written deductionLog keys
const deductionKeysSeen = new Set();

// Counters for the status log
let totalSuspectsWritten   = 0;
let totalDeductionsWritten = 0;
let pollCount              = 0;

// ─── Firebase init ────────────────────────────────────────────────────────────
const firebaseApp = initializeApp(FIREBASE_CONFIG);
const db          = getFirestore(firebaseApp);

// ─── Helpers ──────────────────────────────────────────────────────────────────
function getRoundStart(d) {
  const r = new Date(d);
  r.setSeconds(0, 0);
  if (r.getMinutes() < 30) r.setMinutes(0);
  else                      r.setMinutes(30);
  return r;
}

function fmtNum(n) {
  return n == null ? '—' : n.toLocaleString();
}

function ts() {
  return new Date().toLocaleTimeString('en-US', { hour12: false });
}

function repSnapshot(json) {
  const snap = {};
  for (const clan of json.clans) {
    const members = {};
    for (const m of clan.member_list) members[m.id] = m.reputation;
    snap[clan.id] = { name: clan.name, rep: clan.reputation, members };
  }
  return snap;
}

// ─── Round tracking ───────────────────────────────────────────────────────────
function updateRoundTracking() {
  if (!currentData) return false;
  const now        = new Date();
  const roundStart = getRoundStart(new Date(now.getTime() - ROUND_SWITCH_GRACE_MS));
  const roundId    = roundStart.toISOString();

  if (!round.id) {
    round = { id: roundId, startTs: roundStart.toISOString(), startSnap: repSnapshot(currentData) };
    console.log(`[${ts()}] Round opened: ${roundId}`);
    return false;
  }

  if (roundId === round.id) return false;

  // Round boundary crossed — start fresh round
  console.log(`[${ts()}] Round rolled over → ${roundId}`);
  round = { id: roundId, startTs: roundStart.toISOString(), startSnap: repSnapshot(currentData) };
  return true;
}

// ─── Member gain events (for bleed detection, mirrored from HTML) ─────────────
function recordMemberGainEvents(roundRolledOver) {
  if (!currentData) return;
  const now = Date.now();

  for (const clan of currentData.clans) {
    for (const m of clan.member_list) {
      const key = `${clan.id}_${m.id}`;
      if (roundRolledOver || !memberGainEvents[key]) {
        memberGainEvents[key] = [];
        continue;
      }
      if (prevData) {
        const prevClan   = prevData.clans.find(c => c.id === clan.id);
        const prevMember = prevClan?.member_list.find(mm => mm.id === m.id);
        if (prevMember && m.reputation > prevMember.reputation) {
          memberGainEvents[key].push(now);
        }
      }
      memberGainEvents[key] = memberGainEvents[key].filter(t => now - t <= BLEED_EVENT_TTL_MS);
    }
  }
}

// ─── Clan activity ────────────────────────────────────────────────────────────
function recordClanActivity(roundRolledOver) {
  if (!currentData) return;
  const now = Date.now();

  for (const clan of currentData.clans) {
    if (roundRolledOver || !clanActivity[clan.id]) {
      clanActivity[clan.id] = { amount: 0, ts: null };
      continue;
    }
    if (!prevData) continue;
    const prev  = prevData.clans.find(c => c.id === clan.id);
    if (!prev) continue;
    const delta = clan.reputation - prev.reputation;
    if (delta !== 0) clanActivity[clan.id] = { amount: delta, ts: now };
  }
}

// ─── Member peak tracking — MEMORY ONLY, no Firestore writes ─────────────────
// Tracks each member's highest rep seen this round and whether it later dropped.
// The drop here is the ACP's initial "flag" — NOT the official deduction yet.
// Suspects are only written to Firestore inside recordDeductionChanges() when
// the clan's official deduction value actually changes in the API.
function recordMemberPeaks(roundRolledOver) {
  if (!currentData) return;
  const now = Date.now();

  for (const clan of currentData.clans) {
    for (const m of clan.member_list) {
      const key   = `${clan.id}_${m.id}`;
      const entry = memberPeakRep[key];

      // Round rollover or first time seeing this member — set baseline
      if (roundRolledOver || !entry) {
        memberPeakRep[key] = {
          peak:       m.reputation,
          peakTs:     now,
          dropAmount: 0,
          dropTs:     null,
          dropCount:  0,
        };
        continue;
      }

      if (m.reputation > entry.peak) {
        // New high — update peak
        memberPeakRep[key] = { ...entry, peak: m.reputation, peakTs: now };

      } else if (entry.peak > 0 && m.reputation < entry.peak) {
        // Rep fell below peak — ACP flagged this member (store in memory only)
        const dropAmount   = entry.peak - m.reputation;
        const newDropCount = (entry.dropCount || 0) + 1;
        memberPeakRep[key] = {
          ...entry,
          dropAmount,
          dropTs:    now,
          dropCount: newDropCount,
        };
        // No Firestore write here — we wait for the official clan deduction event
      }
    }
  }
}

// ─── Deduction change tracking + official suspect capture ────────────────────
// This is the ONLY place Firestore writes happen.
// When clan.deduction changes in the API, that is the "official" ACP event —
// both the clan penalty and the individual member penalty are now confirmed.
// At that exact moment we:
//   1. Write the deduction entry to deductionLog
//   2. Look at every member of that clan in memory — anyone with a dropAmount > 0
//      is a confirmed suspect for this event — write them to suspectLog
async function recordDeductionChanges() {
  if (!currentData || !prevData) return;
  const now = Date.now();

  for (const clan of currentData.clans) {
    const prev    = prevData.clans.find(c => c.id === clan.id);
    if (!prev) continue;

    const prevDed = prev.deduction || 0;
    const currDed = clan.deduction || 0;
    if (currDed === prevDed) continue;

    const deductionChange = currDed - prevDed; // positive = penalty grew
    const docKey          = `${clan.id}_${now}`;
    if (deductionKeysSeen.has(docKey)) continue;
    deductionKeysSeen.add(docKey);

    // ── 1. Write deduction entry ──────────────────────────────────────────────
    const deductionEntry = {
      clanId:        clan.id,
      clanName:      clan.name,
      clanRank:      clan.rank,
      deduction:     currDed,
      prevDeduction: prevDed,
      change:        deductionChange,
      ts:            now,
      capturedBy:    'server-poller',
    };

    try {
      await setDoc(doc(collection(db, 'deductionLog'), docKey), deductionEntry);
      totalDeductionsWritten++;
      const sign = deductionChange > 0 ? '+' : '';
      console.log(`[${ts()}] 📊 DEDUCTION ${clan.name} (rank ${clan.rank}) | ${fmtNum(prevDed)} → ${fmtNum(currDed)} (${sign}${fmtNum(deductionChange)})`);
    } catch (e) {
      console.error(`[${ts()}] Firestore deduction write error:`, e.message);
    }

    // ── 2. Capture suspects from memory at the moment of official deduction ───
    // Find all members of this clan who have a flagged drop in memory.
    // These are the confirmed cheaters — their individual penalty is now official.
    for (const m of clan.member_list) {
      const key   = `${clan.id}_${m.id}`;
      const entry = memberPeakRep[key];
      if (!entry || entry.dropAmount <= 0) continue; // no drop recorded for this member

      const dedupKey = `${clan.id}_${m.id}_${entry.peak}`;
      if (suspectKeysSeen.has(dedupKey)) continue; // already written from a previous deduction
      suspectKeysSeen.add(dedupKey);

      // HIGH if this member's drop amount exactly matches the clan deduction change
      const suspLvl = (deductionChange > 0 && entry.dropAmount === deductionChange) ? 'HIGH' : 'MEDIUM';

      const suspectEntry = {
        dedupKey,
        ts:              now,          // timestamp of the OFFICIAL deduction event
        clanId:          clan.id,
        clanName:        clan.name,
        clanRank:        clan.rank,
        clanDeduction:   currDed,
        memberId:        m.id,
        memberName:      m.name,
        memberLevel:     m.level,
        memberRep:       m.reputation,
        peakRep:         entry.peak,
        dropAmount:      entry.dropAmount,
        dropCount:       entry.dropCount,
        confidence:      suspLvl,
        suspicionLevel:  suspLvl,
        detectionSource: 'live',
        reason:          `Official clan deduction confirmed — member peak was ${fmtNum(entry.peak)}, rep dropped by ${fmtNum(entry.dropAmount)} — captured at deduction event`,
        capturedBy:      'server-poller',
      };

      try {
        await setDoc(doc(collection(db, 'suspectLog'), dedupKey), suspectEntry);
        totalSuspectsWritten++;
        console.log(`[${ts()}] 🚨 SUSPECT  ${clan.name} · ${m.name} (Lv${m.level}) | peak ${fmtNum(entry.peak)} → drop -${fmtNum(entry.dropAmount)} | ${suspLvl}`);
      } catch (e) {
        console.error(`[${ts()}] Firestore suspect write error:`, e.message);
      }
    }
  }
}

// ─── Main poll function ───────────────────────────────────────────────────────
let fetchInProgress  = false;
let lastGeneratedAt  = null;
let consecutiveErrors = 0;

async function poll() {
  if (fetchInProgress) return;
  fetchInProgress = true;

  try {
    const res  = await fetch(API_URL, { cache: 'no-store' });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const json = await res.json();

    if (currentData) prevData = currentData;
    currentData = json;
    pollCount++;
    consecutiveErrors = 0;

    const roundRolledOver = updateRoundTracking();
    recordClanActivity(roundRolledOver);
    recordMemberGainEvents(roundRolledOver);
    recordMemberPeaks(roundRolledOver);
    await recordDeductionChanges();

    const newGeneratedAt = json.generated_at;
    if (newGeneratedAt !== lastGeneratedAt) {
      lastGeneratedAt = newGeneratedAt;
      if (pollCount % 60 === 0) {
        // Status log every ~5 minutes
        console.log(`[${ts()}] ✅ Polls: ${pollCount} | Suspects caught: ${totalSuspectsWritten} | Deductions logged: ${totalDeductionsWritten}`);
      }
    }

  } catch (e) {
    consecutiveErrors++;
    if (consecutiveErrors <= 3 || consecutiveErrors % 60 === 0) {
      console.error(`[${ts()}] ⚠ Poll error (${consecutiveErrors} consecutive): ${e.message}`);
    }
  } finally {
    fetchInProgress = false;
  }
}

// ─── Express health check / status page ───────────────────────────────────────
const app = express();

app.get('/', (_req, res) => {
  const uptime  = process.uptime();
  const hours   = Math.floor(uptime / 3600);
  const minutes = Math.floor((uptime % 3600) / 60);
  const seconds = Math.floor(uptime % 60);
  const uptimeStr = `${hours}h ${minutes}m ${seconds}s`;

  const clanCount  = currentData?.clans?.length ?? 0;
  const trackedAt  = lastGeneratedAt
    ? new Date(lastGeneratedAt).toLocaleString()
    : 'waiting for first poll…';

  res.type('html').send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta http-equiv="refresh" content="10">
<title>HCLD Tracker — Server Status</title>
<style>
  body{background:#0d0d14;color:#cdd6f4;font-family:'Segoe UI',system-ui,sans-serif;padding:32px;max-width:560px;margin:0 auto}
  h1{color:#f38ba8;margin-bottom:4px;font-size:20px}
  p.sub{color:#6c7086;font-size:13px;margin-bottom:24px}
  .card{background:#1e1e2e;border:1px solid #2a2a3e;border-radius:12px;padding:16px 20px;margin-bottom:12px}
  .label{font-size:11px;color:#6c7086;text-transform:uppercase;letter-spacing:.6px;margin-bottom:4px}
  .value{font-size:18px;font-weight:700;font-variant-numeric:tabular-nums}
  .green{color:#a6e3a1} .red{color:#f38ba8} .amber{color:#f9e2af} .blue{color:#89b4fa} .mauve{color:#cba6f7}
  .row{display:flex;gap:12px}
  .row .card{flex:1}
  .dot{display:inline-block;width:8px;height:8px;border-radius:50%;background:#a6e3a1;margin-right:6px;animation:pulse 2s infinite}
  @keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
</style>
</head>
<body>
<h1>⚡ HCLD Bleed Tracker — Server Poller</h1>
<p class="sub">Running 24/7 · auto-refreshes every 10s</p>

<div class="card">
  <div class="label"><span class="dot"></span>Status</div>
  <div class="value green">Live · polling every 5s</div>
</div>

<div class="row">
  <div class="card">
    <div class="label">Uptime</div>
    <div class="value blue">${uptimeStr}</div>
  </div>
  <div class="card">
    <div class="label">Total polls</div>
    <div class="value">${pollCount.toLocaleString()}</div>
  </div>
</div>

<div class="row">
  <div class="card">
    <div class="label">🚨 Suspects caught</div>
    <div class="value red">${totalSuspectsWritten}</div>
  </div>
  <div class="card">
    <div class="label">📊 Deductions logged</div>
    <div class="value amber">${totalDeductionsWritten}</div>
  </div>
</div>

<div class="card">
  <div class="label">Clans tracked</div>
  <div class="value mauve">${clanCount}</div>
</div>

<div class="card">
  <div class="label">Last API update seen</div>
  <div class="value" style="font-size:14px">${trackedAt}</div>
</div>

<p style="color:#45475a;font-size:11px;margin-top:20px">
  Events are written to Firebase Firestore (suspectLog / deductionLog) — 
  the HTML tracker picks them up automatically via its real-time listener.
</p>
</body>
</html>`);
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`[${ts()}] 🌐 Status page listening on port ${PORT}`);
});

// ─── Start polling ────────────────────────────────────────────────────────────
console.log('═══════════════════════════════════════════════════════');
console.log('  HCLD Bleed Tracker — Always-On Background Poller');
console.log('  Polling:', API_URL);
console.log('  Interval: 5s | Firestore project: clantracker-22435');
console.log('═══════════════════════════════════════════════════════');

// First poll immediately, then every 5 seconds
poll();
setInterval(poll, POLL_MS);

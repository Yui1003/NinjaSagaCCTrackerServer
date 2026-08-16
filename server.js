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
  getDoc,
  getDocs,
  updateDoc,
} from 'firebase/firestore';

// ─── Express health check ─────────────────────────────────────────────────────
import express from 'express';

// ─── Config ───────────────────────────────────────────────────────────────────
const API_URL    = 'https://static.ninjasaga.cc/data/clan_rankings.json';
const POLL_MS    = 5000;   // poll every 5 seconds (same as browser)
const PORT       = process.env.PORT || 5000;

// How many consecutive polls a new clan.deduction value must hold before it's
// treated as an official event (filters single-tick read noise, ~5-10s @ 2).
const DEDUCTION_DEBOUNCE_POLLS = 2;

// Bound on the in-memory dedup Sets so long-running (weeks+) uptime doesn't leak.
const SEEN_KEYS_MAX = 5000;

// Base URL of the HTML tracker's own server (HiddenCloud project), which owns
// the coordinated write path (/api/deductions, /api/suspects) already used by
// browser clients. When set, this poller reports through that same path
// instead of writing to Firestore directly, so there's a single writer.
// Falls back to direct Firestore writes (old behavior) when unset.
const TRACKER_SERVER_URL = (process.env.TRACKER_SERVER_URL || '').replace(/\/+$/, '');

// Shared-secret token for the live pause-control endpoint. Leave unset to
// disable the endpoint entirely (env-var pause flags below still work).
const ADMIN_TOKEN = process.env.ADMIN_TOKEN || null;

const FIREBASE_CONFIG = {
  apiKey:            'AIzaSyBt1VtMUsUd_pVOG6sKbCJTHS2bka8kVpo',
  authDomain:        'clantracker-22435.firebaseapp.com',
  projectId:         'clantracker-22435',
  storageBucket:     'clantracker-22435.firebasestorage.app',
  messagingSenderId: '354867534107',
  appId:             '1:354867534107:web:8452c807f24ef26dcedbbe',
};

// ─── Clan config ─────────────────────────────────────────────────────────────
const YOUR_CLAN_ID = 777; // Hidden Cloud Village

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

// Already-written dedupKeys (prevents double-writes). Bounded FIFO — see
// addSeen() below — so these can't grow forever across weeks of uptime.
const suspectKeysSeen   = new Set();
// Already-written deductionLog keys
const deductionKeysSeen = new Set();

// Add a key to a bounded Set, evicting the oldest entry (insertion order)
// once it grows past SEEN_KEYS_MAX. Keeps recent-dedup protection without
// an unbounded memory leak on a process meant to run 24/7 indefinitely.
function addSeen(set, key) {
  set.add(key);
  if (set.size > SEEN_KEYS_MAX) set.delete(set.values().next().value);
}

// Per-clan "official" deduction state — replaces the old adjacent-poll diff.
// confirmed = last value we've actually logged as an event (or the first
// value seen, if nothing has been logged yet). pendingValue/pendingCount
// implement the debounce: a new value must repeat for
// DEDUCTION_DEBOUNCE_POLLS consecutive polls before it's promoted to
// `confirmed` and written. Key: clanId
let deductionState = {};

// Two independent pause switches. Defaults come from env vars (set-and-forget
// at deploy time); either can also be flipped live via the /admin/pause
// endpoint below (protected by ADMIN_TOKEN). Polling, round tracking, weekly
// gains, and events processing are NEVER gated by these — only the two
// Firestore/HTTP write paths are.
let pausedState = {
  deductions: String(process.env.DEDUCTIONS_PAUSED).toLowerCase() === 'true',
  suspects:   String(process.env.SUSPECTS_PAUSED).toLowerCase()   === 'true',
};

// Counters for the status log
let totalSuspectsWritten   = 0;
let totalDeductionsWritten = 0;
let totalSuspectsSkipped   = 0;   // skipped because deductions/suspects were paused
let totalDeductionsSkipped = 0;
let pollCount               = 0;

// ─── Events tracking state ────────────────────────────────────────────────────
let cachedEvents      = [];   // array of event docs from Firestore
let eventsCacheTs     = 0;    // when cachedEvents was last refreshed
const EVENTS_CACHE_MS = 30000; // re-read events list every 30s

// In-memory snapshot data — loaded from Firestore once per event per server run
// so we don't hit Firestore on every 5s poll
let eventSnapshotsLoaded  = {}; // eventId → bool
let eventSnapshotMembers  = {}; // eventId → {memberId(str) → {name, startRep}}
let eventLastWritten      = {}; // eventId → {memberId(str) → gain} (skip unchanged writes)

// ─── Weekly gains state ───────────────────────────────────────────────────────
let weeklyState = {
  weekKey:          null,   // e.g. "2026-07-20_2026-07-26"
  weekStartUTC:     null,   // ms epoch of Mon 00:00 PHT in UTC
  weekEndUTC:       null,   // ms epoch of Sun 23:59:59 PHT in UTC
  weekStartLabel:   null,   // "2026-07-20"
  weekEndLabel:     null,   // "2026-07-26"
  memberBaselines:  {},     // memberId(str) → { name, startRep }
  lastWrittenGains: {},     // memberId(str) → lastGain (skip unchanged writes)
  initialized:      false,
};

// ─── Firebase init ────────────────────────────────────────────────────────────
const firebaseApp = initializeApp(FIREBASE_CONFIG);
const db          = getFirestore(firebaseApp);

// ─── Coordinated write path ───────────────────────────────────────────────────
// When TRACKER_SERVER_URL is configured, report through the HTML tracker's own
// server instead of writing to Firestore directly — that server already
// dedupes + fans out over SSE, so this collapses two independent writers into
// one. If the request fails (or the URL isn't configured), fall back to a
// direct Firestore write so an event is never silently dropped.
async function reportEvent(path, entry) {
  if (TRACKER_SERVER_URL) {
    try {
      const res = await fetch(`${TRACKER_SERVER_URL}${path}`, {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify(entry),
      });
      if (res.ok) return { ok: true, via: 'http' };
      console.warn(`[${ts()}] ${path} HTTP write failed: HTTP ${res.status} — falling back to direct Firestore write`);
    } catch (e) {
      console.warn(`[${ts()}] ${path} HTTP write error: ${e.message} — falling back to direct Firestore write`);
    }
  }
  return { ok: false, via: 'firestore' }; // caller does the direct Firestore write
}

// ─── Weekly gains helpers ─────────────────────────────────────────────────────

// Returns the current PH-time week bounds.
// PH = UTC+8. Week = Monday 00:00 → Sunday 23:59:59 (PH time).
function getPHWeekInfo() {
  const PH_OFFSET_MS = 8 * 60 * 60 * 1000;
  const nowUTC       = Date.now();
  const phEpoch      = nowUTC + PH_OFFSET_MS;

  // Treat the PH-shifted timestamp as if it were UTC to extract day/date components
  const phDate      = new Date(phEpoch);
  const day         = phDate.getUTCDay();                      // 0=Sun 1=Mon … 6=Sat in PH time
  const daysSinceMon = day === 0 ? 6 : day - 1;

  // Monday midnight (PH) — expressed in the shifted epoch space
  const monMidPH = new Date(phEpoch);
  monMidPH.setUTCDate(monMidPH.getUTCDate() - daysSinceMon);
  monMidPH.setUTCHours(0, 0, 0, 0);

  const sunMidPH = new Date(monMidPH.getTime() + 6 * 24 * 3600 * 1000);

  // Convert back to real UTC timestamps
  const weekStartUTC = monMidPH.getTime() - PH_OFFSET_MS;
  const weekEndUTC   = sunMidPH.getTime() - PH_OFFSET_MS + 24 * 3600 * 1000 - 1;

  const fmtD = d => {
    const y  = d.getUTCFullYear();
    const mo = String(d.getUTCMonth() + 1).padStart(2, '0');
    const dy = String(d.getUTCDate()).padStart(2, '0');
    return `${y}-${mo}-${dy}`;
  };
  const weekStartLabel = fmtD(monMidPH);
  const weekEndLabel   = fmtD(sunMidPH);
  const weekKey        = `${weekStartLabel}_${weekEndLabel}`;
  return { weekKey, weekStartLabel, weekEndLabel, weekStartUTC, weekEndUTC };
}

// On startup: restore baselines from Firestore so a server restart mid-week
// doesn't lose progress. Also archives the previous week if it rolled over.
async function initWeeklyTracking() {
  const info = getPHWeekInfo();
  try {
    const snap = await getDoc(doc(db, 'weeklyGainsConfig', 'currentWeek'));
    if (snap.exists()) {
      const saved = snap.data();
      if (saved.weekKey === info.weekKey) {
        // Same week — restore baselines
        weeklyState.weekKey         = saved.weekKey;
        weeklyState.weekStartUTC    = saved.weekStartUTC;
        weeklyState.weekEndUTC      = saved.weekEndUTC;
        weeklyState.weekStartLabel  = saved.weekStartLabel;
        weeklyState.weekEndLabel    = saved.weekEndLabel;
        weeklyState.memberBaselines = saved.memberBaselines || {};
        console.log(`[${ts()}] 📅 Weekly tracker resumed: ${saved.weekKey} (${Object.keys(weeklyState.memberBaselines).length} members)`);
      } else {
        // Old week config — archive what's in weeklyGains then start fresh
        const oldGainsSnap = await getDoc(doc(db, 'weeklyGains', String(YOUR_CLAN_ID)));
        if (oldGainsSnap.exists()) {
          await archiveWeek(saved, oldGainsSnap.data());
        }
        console.log(`[${ts()}] 📅 Stale week found (${saved.weekKey}), starting fresh for ${info.weekKey}`);
      }
    }
  } catch(e) {
    console.error(`[${ts()}] Weekly init error:`, e.message);
  }
  weeklyState.initialized = true;
}

// Archive a completed week's gains into weeklyGainsHistory.
async function archiveWeek(weekInfo, gainsDoc) {
  try {
    const membersMap = gainsDoc.members || {};
    const membersArr = Object.entries(membersMap).map(([id, m]) => ({
      memberId:  parseInt(id),
      name:      m.name,
      weekGain:  m.weekGain  || 0,
      startRep:  m.weekStartRep || 0,
      endRep:    m.currentRep   || 0,
    })).sort((a, b) => b.weekGain - a.weekGain);

    await setDoc(doc(db, 'weeklyGainsHistory', weekInfo.weekKey), {
      weekKey:        weekInfo.weekKey,
      weekStartLabel: weekInfo.weekStartLabel,
      weekEndLabel:   weekInfo.weekEndLabel,
      archivedAt:     Date.now(),
      members:        membersArr,
    });
    console.log(`[${ts()}] 📦 Week ${weekInfo.weekKey} archived — ${membersArr.length} members`);
  } catch(e) {
    console.error(`[${ts()}] Archive error:`, e.message);
  }
}

// Called every poll. Computes weekly rep gains for clan 777 members and writes
// to weeklyGains/777 only when something actually changed (saves writes).
async function updateWeeklyGains() {
  if (!weeklyState.initialized || !currentData) return;
  const clan = currentData.clans.find(c => c.id === YOUR_CLAN_ID);
  if (!clan || !clan.member_list) return;

  const info = getPHWeekInfo();

  // ── Week rollover ──────────────────────────────────────────────────────────
  if (weeklyState.weekKey && weeklyState.weekKey !== info.weekKey) {
    console.log(`[${ts()}] 📅 Week rolled over: ${weeklyState.weekKey} → ${info.weekKey}`);
    const oldSnap = await getDoc(doc(db, 'weeklyGains', String(YOUR_CLAN_ID)));
    if (oldSnap.exists()) await archiveWeek(weeklyState, oldSnap.data());
    weeklyState.weekKey          = null;
    weeklyState.memberBaselines  = {};
    weeklyState.lastWrittenGains = {};
  }

  // ── New week or first run ──────────────────────────────────────────────────
  if (!weeklyState.weekKey) {
    weeklyState.weekKey        = info.weekKey;
    weeklyState.weekStartUTC   = info.weekStartUTC;
    weeklyState.weekEndUTC     = info.weekEndUTC;
    weeklyState.weekStartLabel = info.weekStartLabel;
    weeklyState.weekEndLabel   = info.weekEndLabel;

    // Seed baselines from current rep for any member not yet recorded
    for (const m of clan.member_list) {
      const id = String(m.id);
      if (!weeklyState.memberBaselines[id]) {
        weeklyState.memberBaselines[id] = { name: m.name, startRep: m.reputation };
      }
    }

    // Persist config so restarts can recover baselines
    try {
      await setDoc(doc(db, 'weeklyGainsConfig', 'currentWeek'), {
        weekKey:         weeklyState.weekKey,
        weekStartUTC:    weeklyState.weekStartUTC,
        weekEndUTC:      weeklyState.weekEndUTC,
        weekStartLabel:  weeklyState.weekStartLabel,
        weekEndLabel:    weeklyState.weekEndLabel,
        memberBaselines: weeklyState.memberBaselines,
      });
      console.log(`[${ts()}] 📅 New week started: ${weeklyState.weekKey}`);
    } catch(e) {
      console.error(`[${ts()}] Config write error:`, e.message);
    }
  }

  // ── Compute per-member weekly gains ────────────────────────────────────────
  const now     = Date.now();
  const members = {};
  let changed   = (Object.keys(weeklyState.lastWrittenGains).length === 0); // always write first time

  for (const m of clan.member_list) {
    const id       = String(m.id);
    // Register new members who joined mid-week
    if (!weeklyState.memberBaselines[id]) {
      weeklyState.memberBaselines[id] = { name: m.name, startRep: m.reputation };
      changed = true;
    }
    const startRep = weeklyState.memberBaselines[id].startRep;
    const weekGain = Math.max(0, m.reputation - startRep);

    if (weekGain !== weeklyState.lastWrittenGains[id]) {
      changed = true;
    }
    members[id] = {
      name:        m.name,
      weekGain,
      currentRep:  m.reputation,
      weekStartRep: startRep,
      lastUpdated: now,
    };
  }

  if (!changed) return; // nothing changed — skip the Firestore write

  // ── Write single batched doc to Firestore ──────────────────────────────────
  try {
    await setDoc(doc(db, 'weeklyGains', String(YOUR_CLAN_ID)), {
      clanId:         YOUR_CLAN_ID,
      weekKey:        weeklyState.weekKey,
      weekStartLabel: weeklyState.weekStartLabel,
      weekEndLabel:   weeklyState.weekEndLabel,
      weekStartUTC:   weeklyState.weekStartUTC,
      weekEndUTC:     weeklyState.weekEndUTC,
      lastUpdated:    now,
      members,
    });
    // Update local cache so next poll skips unchanged members
    for (const [id, m] of Object.entries(members)) {
      weeklyState.lastWrittenGains[id] = m.weekGain;
    }
  } catch(e) {
    console.error(`[${ts()}] Weekly gains write error:`, e.message);
  }
}

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
// When clan.deduction changes in the API AND the new value holds for
// DEDUCTION_DEBOUNCE_POLLS consecutive polls, that's treated as the "official"
// ACP event — both the clan penalty and the individual member penalty are now
// confirmed. At that point we:
//   1. Report the deduction entry (deductionLog)
//   2. Look at every member of that clan in memory — anyone with a dropAmount > 0
//      is a confirmed suspect for this event — report them (suspectLog)
//
// NOTE on the debounce: `deductionState[clanId].confirmed` — not the previous
// poll's raw value — is the baseline for "did this change". That's what makes
// a single noisy tick harmless: a value that blips up and immediately back
// down never reaches pendingCount >= DEDUCTION_DEBOUNCE_POLLS, so `confirmed`
// never moves and nothing is written.
async function recordDeductionChanges() {
  if (!currentData) return;
  const now = Date.now();

  for (const clan of currentData.clans) {
    const currDed = clan.deduction || 0;
    let st = deductionState[clan.id];

    // First time seeing this clan — seed baseline, don't fire an event for it.
    if (!st) {
      deductionState[clan.id] = { confirmed: currDed, pendingValue: null, pendingCount: 0 };
      continue;
    }

    if (currDed === st.confirmed) {
      // Back at (or still at) the confirmed baseline — clear any in-progress debounce.
      if (st.pendingValue !== null) { st.pendingValue = null; st.pendingCount = 0; }
      continue;
    }

    // Differs from the confirmed baseline — advance (or start) the debounce.
    if (st.pendingValue === currDed) {
      st.pendingCount++;
    } else {
      st.pendingValue = currDed;
      st.pendingCount = 1;
    }
    if (st.pendingCount < DEDUCTION_DEBOUNCE_POLLS) continue; // not stable yet — wait

    // Stable for the required number of consecutive polls — this is official.
    const prevDed = st.confirmed;
    const deductionChange = currDed - prevDed; // positive = penalty grew
    st.confirmed     = currDed;
    st.pendingValue  = null;
    st.pendingCount  = 0;

    const docKey = `${clan.id}_${prevDed}_${currDed}`; // value-based — a real repeat
    if (deductionKeysSeen.has(docKey)) continue;         // transition still gets its own key
    addSeen(deductionKeysSeen, docKey);

    // ── 1. Report deduction entry ─────────────────────────────────────────────
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

    if (pausedState.deductions) {
      totalDeductionsSkipped++;
      console.log(`[${ts()}] ⏸ DEDUCTION (paused, not written) ${clan.name} | ${fmtNum(prevDed)} → ${fmtNum(currDed)}`);
    } else {
      const sign = deductionChange > 0 ? '+' : '';
      try {
        const routed = await reportEvent('/api/deductions', deductionEntry);
        if (!routed.ok) {
          await setDoc(doc(collection(db, 'deductionLog'), docKey), deductionEntry);
        }
        totalDeductionsWritten++;
        console.log(`[${ts()}] 📊 DEDUCTION ${clan.name} (rank ${clan.rank}) | ${fmtNum(prevDed)} → ${fmtNum(currDed)} (${sign}${fmtNum(deductionChange)}) [${routed.via}]`);
      } catch (e) {
        console.error(`[${ts()}] Deduction write error:`, e.message);
      }
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
      addSeen(suspectKeysSeen, dedupKey);

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

      if (pausedState.suspects) {
        totalSuspectsSkipped++;
        console.log(`[${ts()}] ⏸ SUSPECT (paused, not written) ${clan.name} · ${m.name}`);
        continue;
      }

      try {
        const routed = await reportEvent('/api/suspects', suspectEntry);
        if (!routed.ok) {
          await setDoc(doc(collection(db, 'suspectLog'), dedupKey), suspectEntry);
        }
        totalSuspectsWritten++;
        console.log(`[${ts()}] 🚨 SUSPECT  ${clan.name} · ${m.name} (Lv${m.level}) | peak ${fmtNum(entry.peak)} → drop -${fmtNum(entry.dropAmount)} | ${suspLvl} [${routed.via}]`);
      } catch (e) {
        console.error(`[${ts()}] Suspect write error:`, e.message);
      }
    }
  }
}

// ─── Events: load list from Firestore ────────────────────────────────────────
async function loadEventsFromFirestore() {
  try {
    const snap = await getDocs(collection(db, 'events'));
    cachedEvents = snap.docs.map(d => ({ id: d.id, ...d.data() }));
    eventsCacheTs = Date.now();
    if (cachedEvents.length > 0) {
      console.log(`[${ts()}] 🎯 Events loaded: ${cachedEvents.length} event(s)`);
    }
  } catch(e) {
    console.warn(`[${ts()}] Events load error:`, e.message);
  }
}

// ─── Events: process active events each poll ──────────────────────────────────
async function processEvents() {
  const now = Date.now();

  // Refresh events list from Firestore every 30s
  if (now - eventsCacheTs > EVENTS_CACHE_MS) {
    await loadEventsFromFirestore();
  }

  if (!currentData || cachedEvents.length === 0) return;
  const clan = currentData.clans.find(c => c.id === YOUR_CLAN_ID);
  if (!clan || !clan.member_list) return;

  for (const ev of cachedEvents) {
    const { id: eventId, startTs, endTs, snapshotTaken, title } = ev;
    if (!startTs || !endTs) continue;

    // Skip events that haven't started yet
    if (now < startTs) continue;

    // Skip events that ended more than 24h ago — no more tracking needed
    if (now > endTs + 86400000) continue;

    const isActive = now <= endTs;

    // ── Step 1: Capture baseline snapshot the moment start time arrives ───────
    // snapshotTaken is false when event is created; server sets it true here.
    if (!snapshotTaken) {
      const snapshotMembers = {};
      for (const m of clan.member_list) {
        snapshotMembers[String(m.id)] = { name: m.name, startRep: m.reputation };
      }
      try {
        await setDoc(doc(db, 'eventSnapshots', eventId), {
          eventId,
          capturedAt: now,
          members: snapshotMembers,
        });
        await updateDoc(doc(db, 'events', eventId), { snapshotTaken: true });
        ev.snapshotTaken = true; // update local cache to avoid re-capturing
        eventSnapshotsLoaded[eventId] = true;
        eventSnapshotMembers[eventId] = snapshotMembers;
        console.log(`[${ts()}] 📸 Event snapshot captured: "${title}" (${Object.keys(snapshotMembers).length} members)`);
      } catch(e) {
        console.warn(`[${ts()}] Snapshot capture error for "${title}":`, e.message);
        continue;
      }
    }

    // ── Step 2: Load snapshot into memory if not already there ────────────────
    if (!eventSnapshotsLoaded[eventId]) {
      try {
        const snapDoc = await getDoc(doc(db, 'eventSnapshots', eventId));
        if (!snapDoc.exists()) continue; // snapshot not ready yet
        eventSnapshotMembers[eventId] = snapDoc.data().members || {};
        eventSnapshotsLoaded[eventId] = true;
      } catch(e) {
        console.warn(`[${ts()}] Snapshot load error for "${title}":`, e.message);
        continue;
      }
    }

    // ── Step 3: Compute per-member gains from baseline ────────────────────────
    const snapshot = eventSnapshotMembers[eventId];
    if (!snapshot) continue;

    const members = {};
    let changed = (Object.keys(eventLastWritten[eventId] || {}).length === 0); // always write first time

    for (const m of clan.member_list) {
      const id       = String(m.id);
      const startRep = snapshot[id] ? snapshot[id].startRep : m.reputation;
      const eventGain = Math.max(0, m.reputation - startRep);

      if (eventGain !== (eventLastWritten[eventId] || {})[id]) changed = true;

      members[id] = {
        name:        m.name,
        eventGain,
        startRep,
        currentRep:  m.reputation,
        lastUpdated: now,
      };
    }

    if (!changed) continue; // nothing changed — skip Firestore write

    // ── Step 4: Write gains doc ───────────────────────────────────────────────
    try {
      await setDoc(doc(db, 'eventGains', eventId), {
        eventId,
        eventTitle:  title,
        isActive,
        lastUpdated: now,
        members,
      });
      if (!eventLastWritten[eventId]) eventLastWritten[eventId] = {};
      for (const [id, m] of Object.entries(members)) {
        eventLastWritten[eventId][id] = m.eventGain;
      }
    } catch(e) {
      console.warn(`[${ts()}] Event gains write error for "${title}":`, e.message);
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
    await updateWeeklyGains();
    await processEvents();

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
app.use(express.json({ limit: '8kb' }));

// Protected live pause-control endpoint. Two independent switches — either
// can be flipped without a redeploy. Requires ADMIN_TOKEN to be set; if it
// isn't, the endpoint is disabled (env-var defaults from startup still apply).
function checkAdminToken(req, res) {
  if (!ADMIN_TOKEN) {
    res.status(503).json({ error: 'ADMIN_TOKEN is not configured on this server.' });
    return false;
  }
  const token = req.get('x-admin-token') || req.query.token;
  if (token !== ADMIN_TOKEN) {
    res.status(401).json({ error: 'Invalid or missing admin token.' });
    return false;
  }
  return true;
}

app.get('/admin/pause', (req, res) => {
  if (!checkAdminToken(req, res)) return;
  res.json({ paused: pausedState });
});

app.post('/admin/pause', (req, res) => {
  if (!checkAdminToken(req, res)) return;
  const { deductions, suspects } = req.body || {};
  if (typeof deductions === 'boolean') pausedState.deductions = deductions;
  if (typeof suspects === 'boolean') pausedState.suspects = suspects;
  console.log(`[${ts()}] 🔧 Pause state updated via admin endpoint: ${JSON.stringify(pausedState)}`);
  res.json({ ok: true, paused: pausedState });
});

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
  .btn{margin-top:10px;background:#313244;color:#cdd6f4;border:1px solid #45475a;border-radius:8px;padding:6px 14px;font-size:13px;font-weight:600;cursor:pointer}
  .btn:hover{background:#45475a}
  input[type=password]{width:100%;box-sizing:border-box;background:#11111a;border:1px solid #2a2a3e;border-radius:8px;padding:8px 10px;color:#cdd6f4;font-size:13px;margin-top:4px}
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
    <div class="value red">${totalSuspectsWritten}${totalSuspectsSkipped ? ` <span style="font-size:12px;color:#6c7086">(+${totalSuspectsSkipped} skipped)</span>` : ''}</div>
  </div>
  <div class="card">
    <div class="label">📊 Deductions logged</div>
    <div class="value amber">${totalDeductionsWritten}${totalDeductionsSkipped ? ` <span style="font-size:12px;color:#6c7086">(+${totalDeductionsSkipped} skipped)</span>` : ''}</div>
  </div>
</div>

<div class="row">
  <div class="card">
    <div class="label">Deduction monitoring</div>
    <div class="value ${pausedState.deductions ? 'red' : 'green'}">${pausedState.deductions ? 'PAUSED' : 'LIVE'}</div>
    <button class="btn" onclick="togglePause('deductions', ${pausedState.deductions})">${pausedState.deductions ? 'Resume' : 'Pause'}</button>
  </div>
  <div class="card">
    <div class="label">Suspect monitoring</div>
    <div class="value ${pausedState.suspects ? 'red' : 'green'}">${pausedState.suspects ? 'PAUSED' : 'LIVE'}</div>
    <button class="btn" onclick="togglePause('suspects', ${pausedState.suspects})">${pausedState.suspects ? 'Resume' : 'Pause'}</button>
  </div>
</div>

<div class="card">
  <div class="label">Admin token</div>
  <input id="adminToken" type="password" placeholder="Paste ADMIN_TOKEN to enable the buttons above" autocomplete="off">
  <div id="pauseStatus" style="font-size:12px;color:#6c7086;margin-top:8px"></div>
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
  ${TRACKER_SERVER_URL
    ? `Events are reported to ${TRACKER_SERVER_URL} (/api/deductions, /api/suspects), with a direct Firestore write as fallback if that request fails.`
    : `Events are written directly to Firebase Firestore (suspectLog / deductionLog). Set TRACKER_SERVER_URL to route through the HTML tracker's own coordinated write path instead.`}
</p>

<script>
  // Token is kept only in this browser's localStorage — never embedded in
  // the page source, never sent anywhere except the x-admin-token header on
  // this page's own /admin/pause calls.
  const tokenInput = document.getElementById('adminToken');
  const statusEl   = document.getElementById('pauseStatus');
  tokenInput.value = localStorage.getItem('hcld_admin_token') || '';
  tokenInput.addEventListener('input', () => {
    localStorage.setItem('hcld_admin_token', tokenInput.value);
  });

  async function togglePause(kind, currentlyPaused) {
    const token = tokenInput.value.trim();
    if (!token) {
      statusEl.textContent = 'Paste your ADMIN_TOKEN above first.';
      statusEl.style.color = '#f38ba8';
      return;
    }
    statusEl.textContent = 'Updating…';
    statusEl.style.color = '#6c7086';
    try {
      const res = await fetch('/admin/pause', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-admin-token': token },
        body: JSON.stringify({ [kind]: !currentlyPaused }),
      });
      const data = await res.json();
      if (!res.ok) {
        statusEl.textContent = data.error || ('HTTP ' + res.status);
        statusEl.style.color = '#f38ba8';
        return;
      }
      location.reload();
    } catch (e) {
      statusEl.textContent = 'Request failed: ' + e.message;
      statusEl.style.color = '#f38ba8';
    }
  }
</script>
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

// Initialize weekly tracking (async — restores baselines from Firestore)
initWeeklyTracking();

// Load events list on startup (refreshed every 30s during polling)
loadEventsFromFirestore();

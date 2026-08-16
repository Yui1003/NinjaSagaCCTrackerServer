# HCLD Bleed Tracker — Background Poller Server

## Project overview
A Node.js server that runs 24/7 and replicates the Hidden Cloud CW Live Tracker's browser-side polling logic on the server. It polls `https://static.ninjasaga.cc/data/clan_rankings.json` every 5 seconds, detects cheat-reversal events (member rep peak→drop) and clan deduction changes, then writes them to the same Firestore collections (`suspectLog`, `deductionLog`) that the HTML tracker already listens to in real time.

**Why this exists:** The HTML tracker can only catch events while a browser tab is open. This server is always running, so events are caught 24/7 even when no device has the tracker open.

## Architecture
- `server.js` — main poller (detection logic + Firestore writes + Express health endpoint)
- `package.json` — dependencies: `firebase` (web SDK), `express`

## Firebase project
- Project ID: `clantracker-22435`
- Uses the same public web SDK config as the HTML tracker
- Collections written: `suspectLog` (doc key = dedupKey), `deductionLog` (doc key = `${clanId}_${prevDeduction}_${deduction}`)
- When `TRACKER_SERVER_URL` is set, these are reported via HTTP to that
  server's `/api/deductions` + `/api/suspects` endpoints instead (same
  coordinated write path the browser clients use), with a direct Firestore
  write as a fallback if the HTTP request fails.

## Deduction detection
A `clan.deduction` change is only treated as official once the new value
holds for `DEDUCTION_DEBOUNCE_POLLS` (default 2, i.e. ~5-10s) consecutive
polls — this filters single-tick read noise from the upstream API without
meaningfully delaying real detections. The dedup Sets (`suspectKeysSeen`,
`deductionKeysSeen`) are bounded (FIFO eviction past `SEEN_KEYS_MAX`) so they
don't leak memory over weeks of uptime.

## Pause controls
Two independent switches gate the two write paths only — polling, round
tracking, weekly gains, and events processing always keep running:
- `DEDUCTIONS_PAUSED` / `SUSPECTS_PAUSED` env vars — set-and-forget defaults.
- `GET/POST /admin/pause` — flip either switch live without a redeploy.
  Requires header `x-admin-token: <ADMIN_TOKEN>` (or `?token=`). Disabled
  entirely if `ADMIN_TOKEN` isn't set.
- Reflected on the status page (`GET /`) as "Deduction/Suspect monitoring: LIVE/PAUSED".

## Running
```
npm install
npm start
```
The server exposes a health check + status page at `GET /` (port 5000) for Replit's workflow monitor.
Deploy with "Always On" so it runs continuously.

## Environment variables
- `TRACKER_SERVER_URL` — optional; base URL of the HiddenCloud tracker server for coordinated writes.
- `ADMIN_TOKEN` — optional; enables the live `/admin/pause` endpoint.
- `DEDUCTIONS_PAUSED`, `SUSPECTS_PAUSED` — optional; `"true"`/`"false"`, default `false`.

## User preferences
- Keep detection logic consistent with the HTML tracker's JavaScript

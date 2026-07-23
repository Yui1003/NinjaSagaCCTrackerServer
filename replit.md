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
- Collections written: `suspectLog` (doc key = dedupKey), `deductionLog` (doc key = `${clanId}_${ts}`)

## Running
```
npm install
npm start
```
The server exposes a health check at `GET /` (port 5000) for Replit's workflow monitor.
Deploy with "Always On" so it runs continuously.

## User preferences
- Keep detection logic consistent with the HTML tracker's JavaScript

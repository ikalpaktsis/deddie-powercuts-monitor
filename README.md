# DEDDIE Power Cuts Monitor

Monitors the official DEDDIE public REST API for power outages and sends notifications to a Microsoft Teams channel **via email** only when changes occur.

## Features
- Runs entirely in GitHub Actions (no local dependencies)
- Robust HTTP retries and timeouts
- Idempotent state tracking via `state.json`
- No spam: only notifies on changes
- Groups outages by Nomos (prefecture) when mapping is provided
- Includes estimated restoration time in notifications
- Incident-based alert format with full details (ID, creator, type, status)

## Setup
1. Enable 2-Step Verification on the Gmail account that will send the alerts.
2. Create a Gmail App Password and save it.
3. In your GitHub repo, add these secrets:
   - `GMAIL_ADDRESS` (the sender Gmail address)
   - `GMAIL_APP_PASSWORD` (the 16-character app password)
   - `TEAMS_CHANNEL_EMAIL` (the Teams channel email address)

## Configuration
- Region IDs are set in `monitor.py` via `NE_IDS` (default: `['0205']`).
- The workflow overrides this via `NE_IDS` env with all IDs.

## Nomos Mapping
To show the Nomos name in emails, fill `ne_id_map.json` with a map from NE ID to Nomos name.

Example (placeholder):
```json
{
  "0101": "Νομός Παράδειγμα"
}
```

If a mapping is missing, the email will show `ΝΕ <id>` as the fallback label.

## Alert Format
Each incident is sent in a table-friendly format (HTML email + plain-text fallback) with these columns:
- `Νομός`
- `NE_ID`
- `Επηρεαζόμενες περιοχές`
- `Έναρξη βλάβης`
- `Εκτιμώμενη αποκατάσταση`
- `Ανακοινωμένη αποκατάσταση`
- `Incident ID`
- `Created By`
- `Type`
- `Status`

Plain-text fallback example:

```text
[ΗΛΕΙΑ 0703] ΕΝΕΡΓΗ ΔΙΑΚΟΠΗ

Affected Areas:
Σκιλλούντος, Αρχαίας Ολυμπίας, Πύργου, Φολόης, Ωλένης

Start: 13/02/2026 08:58
ETA Restore: 13/02/2026 12:18
Announced Restore: 13/02/2026 12:30

Incident ID: 24002
Created By: M.Kalfountzou
Type: Emergency Outage
Status: Active
```

Notification types:
- `ΕΝΕΡΓΗ ΔΙΑΚΟΠΗ` for new incidents
- `ΕΝΗΜΕΡΩΣΗ ΔΙΑΚΟΠΗΣ` when incident details change (ETA/areas/status/etc.)
- `ΑΠΟΚΑΤΑΣΤΑΣΗ` when an incident is no longer present in API results

## How It Works
Every 15 minutes, the workflow:
1. Calls the DEDDIE API for each configured region ID
2. Extracts affected area names
3. Groups by Nomos
4. Tracks restoration ETA (`end_date_announced`/`end_date`)
5. Compares incidents with previous run (`state.json`)
6. Sends email alerts only for **new**, **updated**, or **restored** incidents
7. Commits updated `state.json` back to the repo

## GitHub Actions
Workflow file: `.github/workflows/monitor.yml`

Manual run is also supported from the GitHub Actions UI.
Manual run inputs:
- `force_notify` to send a test notification even if no changes
- `debug_log` to enable extra debug logging

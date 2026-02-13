# DEDDIE Power Cuts Monitor

Monitors the official DEDDIE public REST API for power outages and sends notifications to a Microsoft Teams channel **via email** only when changes occur.

## Features
- Runs entirely in GitHub Actions (no local dependencies)
- Robust HTTP retries and timeouts
- Idempotent state tracking via `state.json`
- No spam: only notifies on changes

## Setup
1. Enable 2-Step Verification on the Gmail account that will send the alerts.
2. Create a Gmail App Password and save it.
3. In your GitHub repo, add these secrets:
   - `GMAIL_ADDRESS` (the sender Gmail address)
   - `GMAIL_APP_PASSWORD` (the 16-character app password)
   - `TEAMS_CHANNEL_EMAIL` (the Teams channel email address)

## Configuration
- Region IDs are set in `monitor.py` via `NE_IDS` (default: `['0205']`).

## How It Works
Every 15 minutes, the workflow:
1. Calls the DEDDIE API for each configured region ID
2. Extracts affected area names
3. Compares with previous run (`state.json`)
4. Sends email alerts only for **new** or **restored** areas
5. Commits updated `state.json` back to the repo

## GitHub Actions
Workflow file: `.github/workflows/monitor.yml`

Manual run is also supported from the GitHub Actions UI.
Manual run inputs:
- `force_notify` to send a test notification even if no changes
- `debug_log` to enable extra debug logging

# DEDDIE Power Cuts Monitor

Monitors the official DEDDIE public REST API for power outages and sends Microsoft Teams notifications **only when changes occur**.

## Features
- Runs entirely in GitHub Actions (no local dependencies)
- Robust HTTP retries and timeouts
- Idempotent state tracking via `state.json`
- No spam: only notifies on changes

## Setup
1. Create a Microsoft Teams Incoming Webhook and copy the URL.
2. In your GitHub repo, add a secret named `TEAMS_WEBHOOK` with the webhook URL.

## Configuration
- Region IDs are set in `monitor.py` via `NE_IDS` (default: `['0205']`).

## How It Works
Every 15 minutes, the workflow:
1. Calls the DEDDIE API for each configured region ID
2. Extracts affected area names
3. Compares with previous run (`state.json`)
4. Sends Teams alerts only for **new** or **restored** areas
5. Commits updated `state.json` back to the repo

## GitHub Actions
Workflow file: `.github/workflows/monitor.yml`

Manual run is also supported from the GitHub Actions UI.

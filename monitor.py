import json
import os
import sys
from datetime import datetime, timezone
from typing import Iterable, List, Set
import smtplib
from email.message import EmailMessage

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_BASE = (
    "https://apps.deddie.gr/gr.deddie.pfr-2.1/rest/powercutreport/"
    "getPowerOutagesperNE?nomarxiaki_enothta_id={ne_id}"
)

NE_IDS = ["0205"]
STATE_PATH = os.path.join(os.path.dirname(__file__), "state.json")
TIMEOUT_SECONDS = 20

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

ENV_NE_IDS = "NE_IDS"
ENV_GMAIL_ADDRESS = "GMAIL_ADDRESS"
ENV_GMAIL_APP_PASSWORD = "GMAIL_APP_PASSWORD"
ENV_TEAMS_CHANNEL_EMAIL = "TEAMS_CHANNEL_EMAIL"
FROM_ALIAS_EMAIL = "giannis_kalpaktsis@yahoo.gr"
ENV_DEBUG_LOG = "DEBUG_LOG"


def _log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}")


def _build_session() -> requests.Session:
    retry = Retry(
        total=5,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "POST"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "deddie-powercuts-monitor/1.0",
            "Accept": "application/json",
        }
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _get_ne_ids() -> List[str]:
    raw = os.environ.get(ENV_NE_IDS, "").strip()
    if not raw:
        return NE_IDS
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]


def _safe_get_json(session: requests.Session, url: str) -> List[dict]:
    _log(f"Fetching: {url}")
    resp = session.get(url, timeout=TIMEOUT_SECONDS)
    if resp.status_code != 200:
        _log(f"Non-200 response: {resp.status_code}")
    try:
        data = resp.json()
    except Exception as exc:
        _log(f"Failed to parse JSON: {exc}")
        return []
    if not isinstance(data, list):
        _log("Unexpected JSON structure; expected list")
        return []
    return data


def _extract_areas(payloads: Iterable[dict]) -> Set[str]:
    areas: Set[str] = set()
    for outage in payloads:
        items = outage.get("lektikoGenikonDiakoponList")
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            text = item.get("text")
            if not text:
                continue
            normalized = " ".join(str(text).strip().split())
            if normalized:
                areas.add(normalized)
    return areas


def _read_state() -> Set[str]:
    if not os.path.exists(STATE_PATH):
        return set()
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        areas = data.get("areas", [])
        if isinstance(areas, list):
            return {str(a) for a in areas if a}
    except Exception as exc:
        _log(f"Failed to read state.json: {exc}")
    return set()


def _write_state(areas: Set[str]) -> None:
    payload = {
        "areas": sorted(areas),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _build_message(new_areas: Set[str], restored_areas: Set[str]) -> str:
    lines: List[str] = []
    if new_areas:
        lines.append("⚡ ΝΕΕΣ διακοπές")
        for area in sorted(new_areas):
            lines.append(f"• {area}")
    if restored_areas:
        if lines:
            lines.append("")
        lines.append("✅ Αποκαταστάθηκαν")
        for area in sorted(restored_areas):
            lines.append(f"• {area}")
    return "\n".join(lines)


def _send_email(subject: str, body: str) -> bool:
    sender = os.environ.get(ENV_GMAIL_ADDRESS, "").strip()
    password = os.environ.get(ENV_GMAIL_APP_PASSWORD, "").strip()
    recipient = os.environ.get(ENV_TEAMS_CHANNEL_EMAIL, "").strip()

    if not sender or not password or not recipient:
        _log("Missing email env vars; exiting without error")
        return False

    msg = EmailMessage()
    msg["From"] = FROM_ALIAS_EMAIL or sender
    msg["Reply-To"] = FROM_ALIAS_EMAIL or sender
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.set_content(body)

    try:
        _log("Sending email notification")
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=TIMEOUT_SECONDS) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(sender, password)
            server.send_message(msg)
        return True
    except Exception as exc:
        _log(f"Email send failed: {exc}")
    return False


def _debug_sample(payloads: List[dict]) -> None:
    if not os.environ.get(ENV_DEBUG_LOG):
        return
    if not payloads:
        _log("DEBUG: no payloads to sample")
        return
    first = payloads[0]
    if isinstance(first, dict):
        keys = sorted(first.keys())
        _log(f"DEBUG: first payload keys: {keys}")
        items = first.get("lektikoGenikonDiakoponList")
        if isinstance(items, list):
            _log(f"DEBUG: lektikoGenikonDiakoponList length: {len(items)}")
    else:
        _log(f"DEBUG: first payload type: {type(first)}")


def main() -> int:
    try:
        session = _build_session()
        all_payloads: List[dict] = []
        for ne_id in _get_ne_ids():
            url = API_BASE.format(ne_id=ne_id)
            payload = _safe_get_json(session, url)
            all_payloads.extend(payload)

        _log(f"Fetched payloads: {len(all_payloads)}")
        _debug_sample(all_payloads)
        current_areas = _extract_areas(all_payloads)
        previous_areas = _read_state()
        _log(f"Extracted areas: {len(current_areas)}")

        new_areas = current_areas - previous_areas
        restored_areas = previous_areas - current_areas

        if new_areas or restored_areas:
            message = _build_message(new_areas, restored_areas)
            subject = "DEDDIE Power Outage Updates"
            _send_email(subject, message)
        else:
            _log("No changes detected; no notification sent")

        _write_state(current_areas)
        return 0
    except Exception as exc:
        _log(f"Unexpected error: {exc}")
        return 0


if __name__ == "__main__":
    sys.exit(main())

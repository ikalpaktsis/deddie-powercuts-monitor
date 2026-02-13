import html
import json
import os
import smtplib
import sys
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from typing import Dict, Iterable, List, Optional, Set, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from zoneinfo import ZoneInfo

    ATHENS_TZ = ZoneInfo("Europe/Athens")
except Exception:
    ATHENS_TZ = timezone(timedelta(hours=2))

API_BASE = (
    "https://apps.deddie.gr/gr.deddie.pfr-2.1/rest/powercutreport/"
    "getPowerOutagesperNE?nomarxiaki_enothta_id={ne_id}"
)

NE_IDS = ["0205"]
STATE_PATH = os.path.join(os.path.dirname(__file__), "state.json")
NE_MAP_PATH = os.path.join(os.path.dirname(__file__), "ne_id_map.json")
TIMEOUT_SECONDS = 20

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

ENV_NE_IDS = "NE_IDS"
ENV_GMAIL_ADDRESS = "GMAIL_ADDRESS"
ENV_GMAIL_APP_PASSWORD = "GMAIL_APP_PASSWORD"
ENV_TEAMS_CHANNEL_EMAIL = "TEAMS_CHANNEL_EMAIL"
ENV_DEBUG_LOG = "DEBUG_LOG"
ENV_FORCE_NOTIFY = "FORCE_NOTIFY"
FROM_ALIAS_EMAIL = "iokalpaktsis@gmail.com"

AREA_LIST_KEYS = (
    "lektikoGenikonDiakoponList",
    "exyphretoumeniPerioxiList",
    "exyphretoumeniDhmEnothtaList",
    "kallikratikiDhmotikiEnothtaList",
    "kallikratikosOTAList",
)

AREA_TEXT_KEYS = (
    "text",
    "name",
    "perioxi",
    "perioxh",
    "peri",
    "description",
    "title",
    "ota",
    "nomos",
    "dhm_enothta",
    "dhm_enothta_name",
    "kallikratikos_ota",
)

NOMOS_LIST_KEYS = (
    "kallikratikiNomarxiaList",
)

NOMOS_TEXT_KEYS = (
    "peri",
    "name",
    "text",
    "nomos",
)

CAUSE_LABELS = {
    "OUTAGE": "Emergency Outage",
    "EMERGENCY": "Emergency Outage",
    "SCHEDULED": "Scheduled Outage",
}

TABLE_COLUMNS: List[Tuple[str, str]] = [
    ("nomos", "Νομός"),
    ("areas", "Επηρεαζόμενες περιοχές"),
    ("start", "Έναρξη βλάβης"),
    ("eta_restore", "Εκτιμώμενη αποκατάσταση"),
    ("announced_restore", "Ανακοινωμένη αποκατάσταση"),
    ("incident_id", "Incident ID"),
    ("created_by", "Created By"),
    ("type", "Type"),
    ("status", "Status"),
    ("ne_id", "NE_ID"),
]


def _log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}")


def _env_true(name: str) -> bool:
    value = os.environ.get(name, "").strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


def _normalize_text(value: str) -> str:
    return " ".join(value.strip().split())


def _to_int(value: object) -> Optional[int]:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        s = value.strip()
        if s and s.lstrip("-").isdigit():
            return int(s)
    return None


def _format_epoch_ms(value: object) -> str:
    ms = _to_int(value)
    if ms is None:
        return "Unknown"
    try:
        dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone(ATHENS_TZ)
        return dt.strftime("%d/%m/%Y %H:%M")
    except Exception:
        return str(ms)


def _get_ne_ids() -> List[str]:
    raw = os.environ.get(ENV_NE_IDS, "").strip()
    if not raw:
        return NE_IDS
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]


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
            "User-Agent": "deddie-powercuts-monitor/2.1",
            "Accept": "application/json",
        }
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


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


def _extract_texts(item: dict, keys: Iterable[str]) -> List[str]:
    texts: List[str] = []
    for key in keys:
        value = item.get(key)
        if isinstance(value, str) and value.strip():
            texts.append(value)
    if texts:
        return texts

    for item_key, value in item.items():
        if not isinstance(value, str) or not value.strip():
            continue
        lowered = item_key.lower()
        if "name" in lowered or "text" in lowered:
            texts.append(value)
    return texts


def _extract_areas(outage: dict) -> List[str]:
    areas: Set[str] = set()
    for key in AREA_LIST_KEYS:
        items = outage.get(key)
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, dict):
                for text in _extract_texts(item, AREA_TEXT_KEYS):
                    normalized = _normalize_text(text)
                    if normalized and not normalized.isdigit():
                        areas.add(normalized)
            elif isinstance(item, str):
                normalized = _normalize_text(item)
                if normalized and not normalized.isdigit():
                    areas.add(normalized)
    return sorted(areas)


def _extract_nomos_from_payload(outage: dict) -> Optional[str]:
    for key in NOMOS_LIST_KEYS:
        items = outage.get(key)
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, dict):
                for text in _extract_texts(item, NOMOS_TEXT_KEYS):
                    normalized = _normalize_text(text)
                    if normalized and not normalized.isdigit():
                        return normalized
            elif isinstance(item, str):
                normalized = _normalize_text(item)
                if normalized and not normalized.isdigit():
                    return normalized
    return None


def _load_ne_id_map() -> Dict[str, str]:
    if not os.path.exists(NE_MAP_PATH):
        return {}
    try:
        # Accept optional UTF-8 BOM if edited from Windows tools.
        with open(NE_MAP_PATH, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            _log("ne_id_map.json must be a JSON object")
            return {}
        result: Dict[str, str] = {}
        for key, value in data.items():
            if value:
                result[str(key)] = str(value)
        return result
    except Exception as exc:
        _log(f"Failed to read ne_id_map.json: {exc}")
        return {}


def _resolve_nomos(outage: dict, ne_id: str, ne_map: Dict[str, str]) -> str:
    payload_nomos = _extract_nomos_from_payload(outage)
    if payload_nomos:
        return payload_nomos
    mapped = ne_map.get(ne_id)
    if mapped:
        return mapped
    return f"ΝΕ {ne_id}" if ne_id else "Χωρίς νομό"


def _format_nomos_label(nomos: str) -> str:
    text = _normalize_text(nomos)
    lowered = text.lower()
    if lowered.startswith("νομός "):
        return text[6:].strip()
    return text


def _incident_type_label(cause: str, is_scheduled: bool) -> str:
    cause_upper = (cause or "").strip().upper()
    if is_scheduled:
        return "Scheduled Outage"
    if cause_upper in CAUSE_LABELS:
        return CAUSE_LABELS[cause_upper]
    if not cause_upper:
        return "Unknown"
    return cause_upper.replace("_", " ").title()


def _incident_status_label(is_active: bool, resolved: bool = False) -> str:
    if resolved:
        return "Restored"
    return "Active" if is_active else "Inactive"


def _incident_key(incident: Dict[str, object]) -> str:
    return f"{incident['ne_id']}:{incident['incident_id']}"


def _incident_signature(incident: Dict[str, object]) -> Tuple[object, ...]:
    return (
        incident.get("incident_id"),
        incident.get("ne_id"),
        incident.get("nomos"),
        tuple(incident.get("areas", [])),
        incident.get("start_date"),
        incident.get("end_date"),
        incident.get("end_date_announced"),
        incident.get("creator"),
        incident.get("cause"),
        bool(incident.get("is_active", True)),
        bool(incident.get("is_scheduled", False)),
    )


def _incident_sort_key(incident: Dict[str, object]) -> Tuple[str, int]:
    ne_id = str(incident.get("ne_id", ""))
    inc_id = _to_int(incident.get("incident_id"))
    return ne_id, inc_id if inc_id is not None else 0


def _build_incident_record(outage: dict, ne_id: str, ne_map: Dict[str, str]) -> Optional[Dict[str, object]]:
    incident_id = _to_int(outage.get("id"))
    if incident_id is None:
        return None

    areas = _extract_areas(outage)
    if not areas:
        return None

    return {
        "incident_id": incident_id,
        "ne_id": ne_id,
        "nomos": _resolve_nomos(outage, ne_id, ne_map),
        "areas": areas,
        "start_date": _to_int(outage.get("start_date")),
        "end_date": _to_int(outage.get("end_date")),
        "end_date_announced": _to_int(outage.get("end_date_announced")),
        "creator": str(outage.get("creator") or "Unknown"),
        "cause": str(outage.get("cause") or ""),
        "is_active": bool(outage.get("is_active", True)),
        "is_scheduled": bool(outage.get("is_scheduled", False)),
    }


def _merge_incident(current: Dict[str, Dict[str, object]], incident: Dict[str, object]) -> None:
    key = _incident_key(incident)
    existing = current.get(key)
    if not existing:
        current[key] = incident
        return

    existing_score = _to_int(existing.get("end_date_announced")) or _to_int(existing.get("end_date")) or 0
    new_score = _to_int(incident.get("end_date_announced")) or _to_int(incident.get("end_date")) or 0
    if new_score >= existing_score:
        current[key] = incident


def _incident_to_row(incident: Dict[str, object], status_override: Optional[str] = None) -> Dict[str, str]:
    areas = incident.get("areas", [])
    area_text = ", ".join(areas) if isinstance(areas, list) and areas else "Unknown"
    row = {
        "nomos": _format_nomos_label(str(incident.get("nomos", "Χωρίς νομό"))),
        "ne_id": str(incident.get("ne_id", "")),
        "areas": area_text,
        "start": _format_epoch_ms(incident.get("start_date")),
        "eta_restore": _format_epoch_ms(incident.get("end_date")),
        "announced_restore": _format_epoch_ms(incident.get("end_date_announced")),
        "incident_id": str(incident.get("incident_id", "Unknown")),
        "created_by": str(incident.get("creator", "Unknown")),
        "type": _incident_type_label(str(incident.get("cause", "")), bool(incident.get("is_scheduled", False))),
        "status": status_override or _incident_status_label(bool(incident.get("is_active", True))),
    }
    return row


def _build_rows_text(title: str, rows: List[Dict[str, str]]) -> str:
    if not rows:
        return ""
    lines: List[str] = [title]
    for idx, row in enumerate(rows, start=1):
        lines.extend(
            [
                f"{idx}. {row['nomos']} | NE_ID: {row['ne_id']}",
                f"Επηρεαζόμενες περιοχές: {row['areas']}",
                f"Έναρξη: {row['start']} | ETA: {row['eta_restore']} | Ανακοινωμένη: {row['announced_restore']}",
                f"Incident ID: {row['incident_id']} | Created By: {row['created_by']} | Type: {row['type']} | Status: {row['status']}",
                "",
            ]
        )
    return "\n".join(lines).strip()


def _build_rows_table_html(title: str, rows: List[Dict[str, str]]) -> str:
    if not rows:
        return ""
    parts: List[str] = [
        f"<h3 style='margin:16px 0 8px 0;'>{html.escape(title)}</h3>",
        "<table style='border-collapse:collapse;width:100%;font-family:Segoe UI,Arial,sans-serif;font-size:13px;'>",
        "<thead><tr>",
    ]
    for _, header in TABLE_COLUMNS:
        parts.append(
            f"<th style='border:1px solid #999;padding:6px;text-align:left;background:#f2f2f2;'>{html.escape(header)}</th>"
        )
    parts.append("</tr></thead><tbody>")
    for row in rows:
        parts.append("<tr>")
        for key, _ in TABLE_COLUMNS:
            parts.append(
                f"<td style='border:1px solid #999;padding:6px;vertical-align:top;'>{html.escape(row.get(key, ''))}</td>"
            )
        parts.append("</tr>")
    parts.append("</tbody></table>")
    return "".join(parts)


def _build_change_payloads(
    new_incidents: List[Dict[str, object]],
    known_incidents: List[Dict[str, object]],
    restored_incidents: List[Dict[str, object]],
) -> Tuple[str, str]:
    text_sections: List[str] = []
    html_sections: List[str] = []

    if new_incidents:
        rows = [_incident_to_row(i) for i in sorted(new_incidents, key=_incident_sort_key)]
        text_sections.append(_build_rows_text("ΝΕΕΣ ΔΙΑΚΟΠΕΣ ΔΕΔΔΗΕ", rows))
        html_sections.append(_build_rows_table_html("ΝΕΕΣ ΔΙΑΚΟΠΕΣ ΔΕΔΔΗΕ", rows))

    if known_incidents:
        rows = [_incident_to_row(i) for i in sorted(known_incidents, key=_incident_sort_key)]
        text_sections.append(_build_rows_text("ΓΝΩΣΤΕΣ ΔΙΑΚΟΠΕΣ ΔΕΔΔΗΕ", rows))
        html_sections.append(_build_rows_table_html("ΓΝΩΣΤΕΣ ΔΙΑΚΟΠΕΣ ΔΕΔΔΗΕ", rows))
    else:
        text_sections.append("ΓΝΩΣΤΕΣ ΔΙΑΚΟΠΕΣ ΔΕΔΔΗΕ\nΚαμία ενεργή διακοπή.")
        html_sections.append("<h3 style='margin:16px 0 8px 0;'>ΓΝΩΣΤΕΣ ΔΙΑΚΟΠΕΣ ΔΕΔΔΗΕ</h3><p>Καμία ενεργή διακοπή.</p>")

    if restored_incidents:
        rows = [
            _incident_to_row(i, status_override=_incident_status_label(False, resolved=True))
            for i in sorted(restored_incidents, key=_incident_sort_key)
        ]
        text_sections.append(_build_rows_text("ΑΠΟΚΑΤΑΣΤΑΣΕΙΣ", rows))
        html_sections.append(_build_rows_table_html("ΑΠΟΚΑΤΑΣΤΑΣΕΙΣ", rows))

    text_body = "\n\n".join([s for s in text_sections if s]).strip()
    html_body = (
        "<html><body style='font-family:Segoe UI,Arial,sans-serif;'>"
        + "".join([s for s in html_sections if s])
        + "</body></html>"
    )
    return text_body, html_body


def _build_snapshot_payload(current_incidents: List[Dict[str, object]]) -> Tuple[str, str]:
    rows = [_incident_to_row(i) for i in sorted(current_incidents, key=_incident_sort_key)]
    if not rows:
        text = "No active outages (test)."
        html_body = "<html><body><p>No active outages (test).</p></body></html>"
        return text, html_body

    text = _build_rows_text("ΕΝΕΡΓΕΣ ΔΙΑΚΟΠΕΣ (TEST)", rows)
    html_body = (
        "<html><body style='font-family:Segoe UI,Arial,sans-serif;'>"
        + _build_rows_table_html("ΕΝΕΡΓΕΣ ΔΙΑΚΟΠΕΣ (TEST)", rows)
        + "</body></html>"
    )
    return text, html_body


def _read_state() -> Tuple[Dict[str, Dict[str, object]], bool]:
    if not os.path.exists(STATE_PATH):
        return {}, False

    try:
        with open(STATE_PATH, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
    except Exception as exc:
        _log(f"Failed to read state.json: {exc}")
        return {}, False

    incidents = data.get("incidents")
    if isinstance(incidents, dict):
        normalized: Dict[str, Dict[str, object]] = {}
        for key, value in incidents.items():
            if isinstance(value, dict):
                normalized[str(key)] = value
        return normalized, False

    if "areas" in data:
        return {}, True

    return {}, False


def _write_state(current: Dict[str, Dict[str, object]]) -> None:
    payload = {
        "version": 2,
        "incidents": {k: current[k] for k in sorted(current)},
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _send_email(subject: str, text_body: str, html_body: Optional[str] = None) -> bool:
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
    msg.set_content(text_body)
    if html_body:
        msg.add_alternative(html_body, subtype="html")

    try:
        _log("Sending email notification")
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=TIMEOUT_SECONDS) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(sender, password)
            refused = server.send_message(msg)
            if refused:
                _log(f"Email refused for recipients: {refused}")
                return False
        return True
    except Exception as exc:
        _log(f"Email send failed: {exc}")
        return False


def _debug_payload_sample(payloads: List[dict]) -> None:
    if not _env_true(ENV_DEBUG_LOG):
        return

    if not payloads:
        _log("DEBUG: no payloads")
        return

    first = payloads[0]
    if not isinstance(first, dict):
        _log(f"DEBUG: first payload type: {type(first)}")
        return

    _log(f"DEBUG: first payload keys: {sorted(first.keys())}")
    for key in AREA_LIST_KEYS + NOMOS_LIST_KEYS:
        items = first.get(key)
        if isinstance(items, list):
            _log(f"DEBUG: {key} length: {len(items)}")
            if items and isinstance(items[0], dict):
                _log(f"DEBUG: {key} first item keys: {sorted(items[0].keys())}")


def main() -> int:
    try:
        session = _build_session()
        ne_map = _load_ne_id_map()

        all_payloads: List[dict] = []
        current_incidents: Dict[str, Dict[str, object]] = {}

        for ne_id in _get_ne_ids():
            url = API_BASE.format(ne_id=ne_id)
            payload = _safe_get_json(session, url)
            if _env_true(ENV_DEBUG_LOG):
                _log(f"DEBUG: NE {ne_id} payloads: {len(payload)}")
            all_payloads.extend(payload)

            for outage in payload:
                if not isinstance(outage, dict):
                    continue
                incident = _build_incident_record(outage, ne_id, ne_map)
                if incident:
                    _merge_incident(current_incidents, incident)

        _log(f"Fetched payloads: {len(all_payloads)}")
        _debug_payload_sample(all_payloads)
        _log(f"Extracted incidents: {len(current_incidents)}")

        previous_incidents, legacy_state_detected = _read_state()
        if legacy_state_detected and not previous_incidents and current_incidents:
            _log("Legacy state detected; suppressing one-time migration notifications")
            previous_incidents = dict(current_incidents)

        current_keys = set(current_incidents.keys())
        previous_keys = set(previous_incidents.keys())

        new_keys = current_keys - previous_keys
        restored_keys = previous_keys - current_keys
        shared_keys = current_keys & previous_keys

        updated_keys = [
            key
            for key in shared_keys
            if _incident_signature(current_incidents[key])
            != _incident_signature(previous_incidents[key])
        ]

        force_notify = _env_true(ENV_FORCE_NOTIFY)
        known_keys = current_keys - new_keys
        _log(
            "Changes summary: "
            f"new={len(new_keys)} restored={len(restored_keys)} updated={len(updated_keys)} "
            f"known={len(known_keys)} force_notify={force_notify}"
        )

        if new_keys or restored_keys or updated_keys:
            text_body, html_body = _build_change_payloads(
                [current_incidents[k] for k in new_keys],
                [current_incidents[k] for k in known_keys],
                [previous_incidents[k] for k in restored_keys],
            )
            _send_email("DEDDIE Power Outage Updates", text_body, html_body)
        elif force_notify:
            text_body, html_body = _build_snapshot_payload(list(current_incidents.values()))
            _send_email("DEDDIE Power Outage Updates (Test)", text_body, html_body)
        else:
            _log("No changes detected; no notification sent")

        _write_state(current_incidents)
        return 0
    except Exception as exc:
        _log(f"Unexpected error: {exc}")
        return 0


if __name__ == "__main__":
    sys.exit(main())

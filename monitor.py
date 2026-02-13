import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Set, Tuple
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
NE_MAP_PATH = os.path.join(os.path.dirname(__file__), "ne_id_map.json")
TIMEOUT_SECONDS = 20

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587

ENV_NE_IDS = "NE_IDS"
ENV_GMAIL_ADDRESS = "GMAIL_ADDRESS"
ENV_GMAIL_APP_PASSWORD = "GMAIL_APP_PASSWORD"
ENV_TEAMS_CHANNEL_EMAIL = "TEAMS_CHANNEL_EMAIL"
FROM_ALIAS_EMAIL = "iokalpaktsis@gmail.com"
ENV_DEBUG_LOG = "DEBUG_LOG"
ENV_FORCE_NOTIFY = "FORCE_NOTIFY"

AREA_LIST_KEYS = (
    "lektikoGenikonDiakoponList",
    "exyphretoumeniPerioxiList",
    "exyphretoumeniDhmEnothtaList",
    "kallikratikiDhmotikiEnothtaList",
    "kallikratikosOTAList",
    "kallikratikiNomarxiaList",
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


def _env_true(name: str) -> bool:
    value = os.environ.get(name, "").strip().lower()
    return value in {"1", "true", "yes", "y", "on"}


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


def _normalize_text(value: str) -> str:
    return " ".join(value.strip().split())


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


def _extract_areas_from_outage(outage: dict) -> Set[str]:
    areas: Set[str] = set()
    for key in AREA_LIST_KEYS:
        items = outage.get(key)
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, dict):
                texts = _extract_texts(item, AREA_TEXT_KEYS)
                for text in texts:
                    normalized = _normalize_text(text)
                    if normalized and not normalized.isdigit():
                        areas.add(normalized)
            elif isinstance(item, str):
                normalized = _normalize_text(item)
                if normalized and not normalized.isdigit():
                    areas.add(normalized)
    return areas


def _extract_nomos_from_outage(outage: dict) -> Set[str]:
    names: Set[str] = set()
    for key in NOMOS_LIST_KEYS:
        items = outage.get(key)
        if not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, dict):
                texts = _extract_texts(item, NOMOS_TEXT_KEYS)
                for text in texts:
                    normalized = _normalize_text(text)
                    if normalized and not normalized.isdigit():
                        names.add(normalized)
            elif isinstance(item, str):
                normalized = _normalize_text(item)
                if normalized and not normalized.isdigit():
                    names.add(normalized)
    return names


def _extract_eta(outage: dict) -> str:
    for key in ("end_date_announced", "end_date"):
        value = outage.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _format_eta(raw: str) -> str:
    if not raw:
        return "Άγνωστη"
    value = raw.strip()
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.strftime("%d/%m/%Y %H:%M")
    except Exception:
        pass
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M"):
        try:
            dt = datetime.strptime(value, fmt)
            return dt.strftime("%d/%m/%Y %H:%M")
        except Exception:
            continue
    return value


def _load_ne_id_map() -> Dict[str, str]:
    if not os.path.exists(NE_MAP_PATH):
        return {}
    try:
        with open(NE_MAP_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            _log("ne_id_map.json must be a JSON object")
            return {}
        mapped = {}
        for key, value in data.items():
            if not value:
                continue
            mapped[str(key)] = str(value)
        return mapped
    except Exception as exc:
        _log(f"Failed to read ne_id_map.json: {exc}")
    return {}


def _resolve_nomos_names(outage: dict, ne_id: str, ne_map: Dict[str, str]) -> Set[str]:
    names = _extract_nomos_from_outage(outage)
    if not names and ne_id:
        mapped = ne_map.get(ne_id)
        if mapped:
            names.add(mapped)
    if not names:
        names.add(f"ΝΕ {ne_id}" if ne_id else "Χωρίς νομό")
    return names


def _encode_key(nomos: str, area: str) -> str:
    return f"{nomos}::{area}"


def _decode_key(key: str) -> Tuple[str, str]:
    if "::" in key:
        nomos, area = key.split("::", 1)
        return nomos, area
    return "Χωρίς νομό", key


def _format_nomos_label(nomos: str) -> str:
    lowered = nomos.lower()
    if lowered.startswith("νομός"):
        return nomos
    return f"Νομός {nomos}"


def _group_by_nomos_eta(keys: Set[str], key_to_eta: Dict[str, str]) -> Dict[str, Dict[str, Set[str]]]:
    grouped: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))
    for key in keys:
        nomos, area = _decode_key(key)
        eta_display = _format_eta(key_to_eta.get(key, ""))
        grouped[nomos][eta_display].add(area)
    return grouped


def _group_by_nomos(keys: Set[str]) -> Dict[str, Set[str]]:
    grouped: Dict[str, Set[str]] = defaultdict(set)
    for key in keys:
        nomos, area = _decode_key(key)
        grouped[nomos].add(area)
    return grouped


def _append_grouped_with_eta(lines: List[str], title: str, grouped: Dict[str, Dict[str, Set[str]]]) -> None:
    if not grouped:
        return
    lines.append(title)
    for nomos in sorted(grouped):
        eta_map = grouped[nomos]
        for eta in sorted(eta_map):
            lines.append(f"{_format_nomos_label(nomos)} — Εκτιμώμενη αποκατάσταση: {eta}")
            for area in sorted(eta_map[eta]):
                lines.append(f"• {area}")
            lines.append("")
    if lines and lines[-1] == "":
        lines.pop()


def _append_grouped(lines: List[str], title: str, grouped: Dict[str, Set[str]]) -> None:
    if not grouped:
        return
    lines.append(title)
    nomoi = sorted(grouped)
    for idx, nomos in enumerate(nomoi):
        lines.append(_format_nomos_label(nomos))
        for area in sorted(grouped[nomos]):
            lines.append(f"• {area}")
        if idx != len(nomoi) - 1:
            lines.append("")


def _append_eta_changes(lines: List[str], changes: List[Tuple[str, str, str]]) -> None:
    if not changes:
        return
    lines.append("⏱️ Ενημέρωση αποκατάστασης")
    grouped: Dict[str, List[Tuple[str, str, str]]] = defaultdict(list)
    for key, old_eta, new_eta in changes:
        nomos, area = _decode_key(key)
        grouped[nomos].append((area, old_eta, new_eta))
    for nomos in sorted(grouped):
        lines.append(_format_nomos_label(nomos))
        for area, old_eta, new_eta in sorted(grouped[nomos]):
            lines.append(
                f"• {area}: {_format_eta(old_eta)} → {_format_eta(new_eta)}"
            )
        lines.append("")
    if lines and lines[-1] == "":
        lines.pop()


def _build_message(
    new_keys: Set[str],
    restored_keys: Set[str],
    eta_changes: List[Tuple[str, str, str]],
    current_map: Dict[str, str],
) -> str:
    lines: List[str] = []
    _append_grouped_with_eta(
        lines, "⚡ ΝΕΕΣ διακοπές", _group_by_nomos_eta(new_keys, current_map)
    )
    if lines and restored_keys:
        lines.append("")
    _append_grouped(lines, "✅ Αποκαταστάθηκαν", _group_by_nomos(restored_keys))
    if eta_changes:
        if lines:
            lines.append("")
        _append_eta_changes(lines, eta_changes)
    return "\n".join(lines)


def _build_snapshot_message(current_map: Dict[str, str]) -> str:
    if not current_map:
        return "ℹ️ Καμία ενεργή διακοπή (test)"
    lines: List[str] = []
    _append_grouped_with_eta(
        lines,
        "ℹ️ Τρέχουσες διακοπές (test)",
        _group_by_nomos_eta(set(current_map.keys()), current_map),
    )
    return "\n".join(lines)


def _read_state() -> Dict[str, str]:
    if not os.path.exists(STATE_PATH):
        return {}
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
        areas = data.get("areas", {})
        if isinstance(areas, dict):
            return {str(k): str(v) for k, v in areas.items()}
        if isinstance(areas, list):
            mapped: Dict[str, str] = {}
            for item in areas:
                if isinstance(item, str):
                    mapped[item] = ""
                elif isinstance(item, dict):
                    key = item.get("key") or item.get("area")
                    if key:
                        mapped[str(key)] = str(item.get("eta", ""))
            return mapped
    except Exception as exc:
        _log(f"Failed to read state.json: {exc}")
    return {}


def _write_state(current_map: Dict[str, str]) -> None:
    payload = {
        "areas": {k: v for k, v in sorted(current_map.items())},
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    with open(STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


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
            refused = server.send_message(msg)
            if refused:
                _log(f"Email refused for recipients: {refused}")
                return False
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
        for key in AREA_LIST_KEYS:
            items = first.get(key)
            if isinstance(items, list):
                _log(f"DEBUG: {key} length: {len(items)}")
                if items:
                    first_item = items[0]
                    if isinstance(first_item, dict):
                        _log(f"DEBUG: {key} first item keys: {sorted(first_item.keys())}")
    else:
        _log(f"DEBUG: first payload type: {type(first)}")


def main() -> int:
    try:
        session = _build_session()
        all_payloads: List[dict] = []
        ne_map = _load_ne_id_map()
        debug_enabled = _env_true(ENV_DEBUG_LOG)
        for ne_id in _get_ne_ids():
            url = API_BASE.format(ne_id=ne_id)
            payload = _safe_get_json(session, url)
            if debug_enabled:
                _log(f"DEBUG: NE {ne_id} payloads: {len(payload)}")
            for outage in payload:
                if isinstance(outage, dict):
                    outage["_ne_id"] = ne_id
            all_payloads.extend(payload)

        _log(f"Fetched payloads: {len(all_payloads)}")
        _debug_sample(all_payloads)

        current_map: Dict[str, str] = {}
        for outage in all_payloads:
            if not isinstance(outage, dict):
                continue
            ne_id = str(outage.get("_ne_id", "")).strip()
            nomos_names = _resolve_nomos_names(outage, ne_id, ne_map)
            areas = _extract_areas_from_outage(outage)
            if not areas:
                continue
            eta_raw = _extract_eta(outage)
            for nomos in nomos_names:
                for area in areas:
                    key = _encode_key(nomos, area)
                    if key not in current_map or (not current_map[key] and eta_raw):
                        current_map[key] = eta_raw

        previous_map = _read_state()
        _log(
            f"Extracted areas: {len(current_map)} across "
            f"{len(_group_by_nomos(set(current_map.keys())))} nomoi"
        )

        current_keys = set(current_map.keys())
        previous_keys = set(previous_map.keys())
        new_keys = current_keys - previous_keys
        restored_keys = previous_keys - current_keys

        eta_changes: List[Tuple[str, str, str]] = []
        for key in current_keys & previous_keys:
            old_eta = previous_map.get(key, "")
            new_eta = current_map.get(key, "")
            if (old_eta or "") != (new_eta or ""):
                eta_changes.append((key, old_eta, new_eta))

        force_notify = _env_true(ENV_FORCE_NOTIFY)
        _log(
            "Changes summary: "
            f"new={len(new_keys)} restored={len(restored_keys)} "
            f"eta_changed={len(eta_changes)} force_notify={force_notify}"
        )

        if new_keys or restored_keys or eta_changes:
            message = _build_message(new_keys, restored_keys, eta_changes, current_map)
            subject = "DEDDIE Power Outage Updates"
            _send_email(subject, message)
        elif force_notify:
            message = _build_snapshot_message(current_map)
            subject = "DEDDIE Power Outage Updates (Test)"
            _send_email(subject, message)
        else:
            _log("No changes detected; no notification sent")

        _write_state(current_map)
        return 0
    except Exception as exc:
        _log(f"Unexpected error: {exc}")
        return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""
Gmail -> Google Sheets invoice request sync.

Scans Gmail messages for invoice requests and guest special requests, matches
them to Previo reservations by reservation/voucher number, and updates the
"Previo" sheet.
"""
# VERSION: 2026-10-21-airbnb-guest-messages
import base64
import json
import os
import re
import time
import unicodedata
from datetime import datetime

from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

SHEET_ID = os.environ["GOOGLE_SHEET_ID"]
SHEET_NAME = "Previo"
SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT"]

INVOICE_GMAIL_CLIENT_ID = os.environ["INVOICE_GMAIL_CLIENT_ID"]
INVOICE_GMAIL_CLIENT_SECRET = os.environ["INVOICE_GMAIL_CLIENT_SECRET"]
INVOICE_GMAIL_REFRESH_TOKEN = os.environ["INVOICE_GMAIL_REFRESH_TOKEN"]

GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
SHEETS_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
STATE_SHEET_NAME = "_InvoiceMailSyncState"
STATE_KEY_LAST_INTERNAL_MS = "last_internal_ms"
INITIAL_LOOKBACK_DAYS = 120
GMAIL_QUERY_OVERLAP_SECONDS = 3 * 24 * 60 * 60

GMAIL_QUERY_BASE = (
    '("Nowa rezerwacja" OR "System zarządzania obiektem PREVIO" OR PREVIO) '
    '(affiliation OR numbertype OR "Informacje dodatkowe")'
)
BOOKING_GUEST_REQUEST_QUERY_BASE = (
    'from:(guest.booking.com) '
    '("łóżeczko" OR lozeczko OR lozeczka OR crib OR cot OR "baby cot" OR '
    '"krzesełko" OR krzeselko OR krzeselka OR "high chair")'
)
AIRBNB_GUEST_MESSAGE_QUERY_BASE = (
    'from:(airbnb.com) '
    '("łóżeczko" OR lozeczko OR lozeczka OR crib OR cot OR "baby cot" OR '
    '"krzesełko" OR krzeselko OR krzeselka OR "high chair" OR '
    'Kinderbett OR Babybett OR Reisebett OR Hochstuhl OR seggiolone OR '
    'faktura OR fakture OR fakturę OR faktury OR invoice OR "VAT invoice" OR '
    'NIP OR VAT OR "tax id" OR "vat id" OR Rechnung OR factura OR fattura OR facture)'
)
STATE_KEY_AIRBNB_LAST_INTERNAL_MS = "airbnb_last_internal_ms"
AIRBNB_INITIAL_LOOKBACK_DAYS = 30
AIRBNB_GUEST_SOURCE = "Gmail / Airbnb guest message"
BOOKING_INVOICE_QUERY_BASE = (
    'from:(guest.booking.com) '
    '(faktura OR fakture OR fakturę OR faktury OR invoice OR "VAT invoice" OR '
    'NIP OR VAT OR "tax id" OR "vat id")'
)


def normalize_text(value):
    text = str(value or "").replace("ł", "l").replace("Ł", "L")
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return text.lower()


def clean_tax_number(value):
    digits = re.sub(r"\D", "", str(value or ""))
    return digits[:10] if len(digits) >= 10 else ""


def is_plausible_polish_tax_number(value):
    digits = clean_tax_number(value)
    if len(digits) != 10:
        return False
    weights = [6, 5, 7, 2, 3, 4, 5, 6, 7]
    checksum = sum(int(digits[i]) * weights[i] for i in range(9)) % 11
    return checksum != 10 and checksum == int(digits[9])


def extract_tax_number(text, allow_loose=False):
    patterns = [
        r"number\s*=\s*([A-Za-z0-9\- ]{6,30})\s+numbertype\s*=\s*vat",
        r"(?:nip|vat|tax\s*id|vat\s*id)\s*[:=]?\s*([A-Za-z0-9\- ]{6,30})",
    ]
    if allow_loose:
        patterns.append(r"\b(\d{3}[- ]?\d{3}[- ]?\d{2}[- ]?\d{2})\b")
    for pattern in patterns:
        match = re.search(pattern, text or "", flags=re.IGNORECASE)
        if match:
            tax_id = clean_tax_number(match.group(1))
            if tax_id:
                return tax_id
    return ""


def extract_booking_guest_message(text):
    raw = str(text or "")
    markers = [
        r"\bpisze\s*:",
        r"\bwrites\s*:",
        r"\bnapisał(?:a)?\s*:",
    ]
    for marker in markers:
        matches = list(re.finditer(marker, raw, flags=re.IGNORECASE))
        if matches:
            return raw[matches[-1].end():].strip()
    return raw


def extract_booking_guest_tax_number(text, reservation_id=""):
    guest_text = extract_booking_guest_message(text)
    tax_id = extract_tax_number(guest_text, allow_loose=False)
    if tax_id and tax_id != clean_tax_number(reservation_id):
        return tax_id

    # In Booking guest messages the actual request often starts after "X pisze:".
    # Only in that guest-authored part do we allow a loose Polish NIP pattern.
    for match in re.finditer(r"\b(\d{3}[- ]?\d{3}[- ]?\d{2}[- ]?\d{2})\b", guest_text):
        candidate = clean_tax_number(match.group(1))
        if candidate == clean_tax_number(reservation_id):
            continue
        if is_plausible_polish_tax_number(candidate):
            return candidate
    return ""


def extract_reservation_id(text):
    patterns = [
        r"Numer potwierdzenia rezerwacji\s*:?\s*(\d{6,12})",
        r"Numer rezerwacji\s*-\s*Previo\s*:?\s*(\d{6,12})",
        r"Numer rezerwacji\s*-\s*Portalu\s*:?\s*(\d{6,12})",
        r"Numer rezerwacji\s*:?\s*(\d{6,12})",
        r"reservation (?:number|id)\s*:?\s*(\d{6,12})",
        r"confirmation (?:number|id)\s*:?\s*(\d{6,12})",
        r"\b(\d{8,12})\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text or "", flags=re.IGNORECASE)
        if match:
            return match.group(1)
    return ""


def is_previo_new_reservation_email(subject, text):
    norm = normalize_text(f"{subject}\n{text}")
    return (
        "nowa rezerwacja" in norm
        and (
            "system zarzadzania obiektem previo" in norm
            or "numer rezerwacji - previo" in norm
            or "informacje dodatkowe" in norm
        )
    )


def classify_invoice_request(text):
    norm = normalize_text(text)
    if "affiliation:" in norm and ("numbertype=vat" in norm or "type=company" in norm):
        return "TAK"
    return ""


def classify_booking_invoice_request(text):
    norm = normalize_text(text)
    negative_patterns = (
        "bez faktur",
        "bez faktury",
        "nie potrzebuje faktur",
        "nie chce faktur",
        "no invoice",
        "invoice not needed",
        "without invoice",
    )
    if any(pattern in norm for pattern in negative_patterns):
        return ""
    positive_patterns = (
        "faktur",
        "invoice",
        "vat invoice",
        "nip",
        "tax id",
        "vat id",
        "rechnung",
        "factura",
        "fattura",
        "facture",
    )
    return "TAK" if any(pattern in norm for pattern in positive_patterns) else ""


def classify_airbnb_invoice_request(text):
    norm = normalize_text(text)
    if re.search(r"bez faktur|nie potrzebuje faktur|nie chce faktur|no invoice|invoice not needed|without (an )?invoice|don.?t need (an )?invoice", norm):
        return ""
    pattern = (
        r"\bfaktur|\binvoice|\bnip\b|\bvat\b|\btax id\b|\bvat id\b|"
        r"\brechnung|\bfactura|\bfattura|\bfacture"
    )
    return "TAK" if re.search(pattern, norm) else ""


def classify_guest_request(text):
    norm = normalize_text(text)
    has_crib = any(
        pattern in norm
        for pattern in (
            "lozeczk",
            "baby cot",
            "crib",
            "travel cot",
            "infant cot",
            "kinderbett",
            "babybett",
            "reisebett",
            "lit bebe",
            "lit parapluie",
        )
    )
    has_chair = any(
        pattern in norm
        for pattern in (
            "krzeselk",
            "high chair",
            "baby chair",
            "feeding chair",
            "hochstuhl",
            "kinderstuhl",
            "seggiolone",
            "chaise haute",
        )
    )
    return {"crib": has_crib, "chair": has_chair}


def extract_affiliation_company(text):
    match = re.search(
        r"affiliation:\s*name=(.*?)\s+number=",
        text or "",
        flags=re.IGNORECASE | re.DOTALL,
    )
    return " ".join(match.group(1).split()) if match else ""


def infer_invoice_source(text):
    return "Gmail / Previo confirmation"


def infer_booking_invoice_source(text):
    return "Gmail / Booking guest message"


def invoice_priority(source, status, tax_id=""):
    src = normalize_text(source)
    st = normalize_text(status)
    has_tax_id = bool(clean_tax_number(tax_id))
    if not st:
        return 0
    if has_tax_id and ("previo confirmation" in src or "previo note" in src or "affiliation" in src or "log" in src):
        return 6
    if has_tax_id and ("booking guest" in src or "airbnb guest" in src or "booking.com email" in src):
        return 5
    if has_tax_id and ("gmail" in src or "mail" in src):
        return 4
    if "previo confirmation" in src or "previo note" in src or "affiliation" in src or "log" in src:
        return 4
    if "booking guest" in src or "airbnb guest" in src or "booking.com email" in src:
        return 3
    if "gmail" in src or "mail" in src:
        return 2
    if "recznie" in src or "company/name" in src:
        return 2
    return 1


def gmail_service():
    creds = Credentials(
        token=None,
        refresh_token=INVOICE_GMAIL_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=INVOICE_GMAIL_CLIENT_ID,
        client_secret=INVOICE_GMAIL_CLIENT_SECRET,
        scopes=GMAIL_SCOPES,
    )
    creds.refresh(Request())
    return build("gmail", "v1", credentials=creds)


def sheets_service():
    creds_dict = json.loads(SERVICE_ACCOUNT_JSON)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=SHEETS_SCOPES,
    )
    return build("sheets", "v4", credentials=creds).spreadsheets()


def decode_base64url(data):
    if not data:
        return ""
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode((data + padding).encode()).decode("utf-8", errors="ignore")


def payload_to_text(payload):
    chunks = []

    def walk(part):
        mime = part.get("mimeType", "")
        body = part.get("body", {})
        data = body.get("data", "")
        if data and mime in ("text/plain", "text/html"):
            chunks.append(decode_base64url(data))
        for child in part.get("parts", []) or []:
            walk(child)

    walk(payload or {})
    text = "\n".join(chunks)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def payload_to_plain_text(payload):
    """text/plain body with line breaks preserved (fallback: HTML converted to lines)."""
    plain = []
    html = []

    def walk(part):
        mime = part.get("mimeType", "")
        data = (part.get("body", {}) or {}).get("data", "")
        if data and mime == "text/plain":
            plain.append(decode_base64url(data))
        elif data and mime == "text/html":
            html.append(decode_base64url(data))
        for child in part.get("parts", []) or []:
            walk(child)

    walk(payload or {})
    if plain:
        return "\n".join(plain)
    text = "\n".join(html)
    text = re.sub(r"(?i)<(br|/p|/div|/tr|/td|/h\d)[^>]*>", "\n", text)
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"[ \t]+", " ", text)


# ---------------------------------------------------------------------------
# Airbnb guest messages
# ---------------------------------------------------------------------------

AIRBNB_GUEST_ROLES = (
    "osoba rezerwujaca",
    "gosc",
    "wspolpodrozny",
    "guest",
    "booker",
    "co-traveler",
    "co-traveller",
)
AIRBNB_HOST_ROLES = (
    "wspolgospodarz",
    "gospodarz",
    "superhost",
    "host",
    "co-host",
    "cohost",
)
AIRBNB_BLOCK_END_MARKERS = (
    "odpowiedz",
    "reply",
    "mozesz rowniez odpowiedziec",
    "you can also reply",
)
AIRBNB_AUTOMATED_END_MARKERS = (
    "czeka na wiadomo",
    "is waiting",
    "akceptuj",
    "wstepnie akceptuj",
    "pre-approve",
    "accept",
    "odpowiedz",
    "reply",
    "masz 24 godziny",
)
AIRBNB_AUTOMATED_SKIP_MARKERS = (
    "uzytkownik airbnb od",
    "on airbnb since",
    "joined in",
    "recenzj",
    "review",
    "tozsamosc zweryfikowana",
    "identity verified",
)

POLISH_MONTHS = {
    "stycz": 1, "sty": 1, "lut": 2, "mar": 3, "kwi": 4, "maj": 5, "maja": 5,
    "czerw": 6, "cze": 6, "lip": 7, "sierp": 8, "sie": 8, "wrz": 9,
    "pazdz": 10, "paz": 10, "listop": 11, "lis": 11, "grud": 12, "gru": 12,
    "jan": 1, "feb": 2, "apr": 4, "may": 5, "jun": 6, "jul": 7, "aug": 8,
    "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}


def month_from_word(word):
    w = normalize_text(word).strip(". ,")
    if not w:
        return 0
    for prefix in sorted(POLISH_MONTHS, key=len, reverse=True):
        if w.startswith(prefix):
            return POLISH_MONTHS[prefix]
    return 0


def is_airbnb_sender(sender):
    return "airbnb.com" in normalize_text(sender)


def extract_airbnb_guest_blocks(plain_text):
    """
    Returns (guest_messages, guest_names) from an Airbnb message e-mail.
    Host / co-host blocks are skipped so our own replies never trigger requests.
    """
    lines = [ln.strip() for ln in str(plain_text or "").splitlines()]
    norm_lines = [normalize_text(ln) for ln in lines]

    role_idx = []
    for i, nl in enumerate(norm_lines):
        nl_clean = nl.strip(" :")
        is_guest = nl_clean in AIRBNB_GUEST_ROLES
        is_host = nl_clean in AIRBNB_HOST_ROLES
        if not (is_guest or is_host):
            continue
        j = i - 1
        while j >= 0 and not lines[j]:
            j -= 1
        if j < 0 or len(lines[j]) > 40 or lines[j].startswith(("http", "[")):
            continue
        role_idx.append((i, j, is_guest))

    messages = []
    names = []
    if role_idx:
        for k, (i, name_idx, is_guest) in enumerate(role_idx):
            end = role_idx[k + 1][1] if k + 1 < len(role_idx) else len(lines)
            body = []
            for m in range(i + 1, end):
                nl = norm_lines[m]
                if any(nl.startswith(marker) for marker in AIRBNB_BLOCK_END_MARKERS):
                    break
                if nl.startswith(("przetlumaczone automatycznie", "translated automatically", "automatically translated")):
                    body.append("/")
                    continue
                if lines[m]:
                    body.append(lines[m])
            if is_guest:
                names.append(lines[name_idx])
                if body:
                    messages.append(" ".join(body))
        return messages, names

    # Automated templates: booking confirmation / request to book / inquiry.
    start = None
    for i, nl in enumerate(norm_lines):
        if "tozsamosc zweryfikowana" in nl or "identity verified" in nl or "uzytkownik airbnb od" in nl:
            start = i
    if start is None:
        return [], []
    body = []
    for m in range(start + 1, len(lines)):
        nl = norm_lines[m]
        if not lines[m]:
            continue
        if lines[m].startswith(("http", "[")):
            if body:
                break
            continue
        if any(marker in nl for marker in AIRBNB_AUTOMATED_END_MARKERS):
            break
        if any(marker in nl for marker in AIRBNB_AUTOMATED_SKIP_MARKERS):
            continue
        body.append(lines[m])
    if body:
        messages.append(" ".join(body))
    return messages, names


def extract_airbnb_guest_names(subject, plain_text):
    names = []
    patterns = [
        (subject, r"Rezerwacja potwierdzona\s*-\s*(.+?)\s+przyjeżdża"),
        (subject, r"Reservation confirmed\s*-\s*(.+?)\s+arrives"),
        (plain_text, r"(?:prośbę|prosbe|pytanie|wiadomość|wiadomosc)\s+od:\s*([^\n]+)"),
        (plain_text, r"(?:request|inquiry|message)\s+from:?\s*([^\n]+)"),
        (plain_text, r"przywitać gościa\s+([^\n.]+)"),
    ]
    for source, pattern in patterns:
        match = re.search(pattern, source or "", flags=re.IGNORECASE)
        if match:
            names.append(match.group(1).strip())
    return names


def extract_airbnb_codes(text):
    return list(dict.fromkeys(re.findall(r"\b(HM[A-Z0-9]{8})\b", text or "")))


def _safe_date(y, m, d):
    try:
        return datetime(int(y), int(m), int(d)).date()
    except Exception:
        return None


def _infer_year(month, day, email_dt):
    base = email_dt.date() if email_dt else datetime.now().date()
    candidate = _safe_date(base.year, month, day)
    if candidate and (base - candidate).days > 180:
        candidate = _safe_date(base.year + 1, month, day)
    return candidate


def extract_airbnb_stay_dates(subject, plain_text, email_dt=None):
    """Returns (check_in, check_out) as date objects, or (None, None)."""
    text = plain_text or ""
    # Use the stay details section (after "Zameldowanie Wymeldowanie"), not dates the guest typed.
    sections = list(re.finditer(r"(?im)^\s*(zameldowanie\s+wymeldowanie|check-?\s?in\s+check-?\s?out)\s*$", text))
    if sections:
        text = text[sections[-1].start():]
    # Message e-mails: "21 października 2026 25 października 2026"
    full = re.findall(r"\b(\d{1,2})\s+([A-Za-zÀ-ž]{3,}\.?)\s+(\d{4})\b", text)
    parsed = []
    for d, mon, y in full:
        month = month_from_word(mon)
        if month:
            dt = _safe_date(y, month, d)
            if dt:
                parsed.append(dt)
    if len(parsed) >= 2 and parsed[1] > parsed[0]:
        return parsed[0], parsed[1]

    # Subject: "w terminie 21–25 paź", "30 paź–1 lis 2026", "9–15 sie 2026"
    match = re.search(
        r"(\d{1,2})(?:\s+([A-Za-zÀ-ž]{3,}\.?))?\s*[–\-]\s*(\d{1,2})\s+([A-Za-zÀ-ž]{3,}\.?)(?:\s+(\d{4}))?",
        subject or "",
    )
    if match:
        d1, m1, d2, m2, year = match.groups()
        month2 = month_from_word(m2)
        month1 = month_from_word(m1) if m1 else month2
        if month1 and month2:
            if year:
                co = _safe_date(year, month2, d2)
                ci = _safe_date(int(year) - (1 if month1 > month2 else 0), month1, d1)
            else:
                ci = _infer_year(month1, d1, email_dt)
                co = _safe_date(ci.year + (1 if month2 < month1 else 0), month2, d2) if ci else None
            if ci and co and co > ci:
                return ci, co

    # Automated templates: "pt., 30 paź niedz., 1 lis"
    short = re.findall(r"\b(\d{1,2})\s+([A-Za-zÀ-ž]{3,})\b", text)
    dates = []
    for d, mon in short:
        month = month_from_word(mon)
        if month:
            dt = _infer_year(month, d, email_dt)
            if dt and dt not in dates:
                dates.append(dt)
        if len(dates) == 2:
            break
    if len(dates) == 2:
        ci, co = dates
        if co < ci:
            co = _safe_date(co.year + 1, co.month, co.day)
        if co and co > ci:
            return ci, co
    return None, None


def name_tokens(value):
    return {tok for tok in re.split(r"[^a-z0-9]+", normalize_text(value)) if len(tok) >= 2}


def build_airbnb_index(reservations):
    """(check_in, check_out) -> list of row infos for Airbnb reservations."""
    index = {}
    seen_rows = set()
    for row_info in reservations.values():
        if row_info["row"] in seen_rows:
            continue
        seen_rows.add(row_info["row"])
        row = row_info["values"]
        if "airbnb" not in normalize_text(row[7]):
            continue
        key = (str(row[3])[:10], str(row[4])[:10])
        index.setdefault(key, []).append(row_info)
    return index


def match_airbnb_reservation(reservations, airbnb_index, codes, names, check_in, check_out):
    for code in codes:
        if code in reservations:
            return reservations[code], f"kod {code}"
    if not check_in or not check_out:
        return None, "brak dat"
    candidates = airbnb_index.get((check_in.isoformat(), check_out.isoformat()), [])
    active = [c for c in candidates if str(c["values"][10]).strip() not in ("3", "4")] or candidates
    if not active:
        return None, f"brak rezerwacji Airbnb {check_in}–{check_out}"
    wanted = set()
    for name in names:
        wanted |= name_tokens(name)
    if wanted:
        hits = [c for c in active if name_tokens(c["values"][11]) & wanted]
        if len(hits) == 1:
            return hits[0], "daty + imię"
        if len(hits) > 1:
            return None, f"niejednoznaczne ({len(hits)} rezerwacje)"
        return None, f"imię {sorted(wanted)} nie pasuje"
    if len(active) == 1:
        return active[0], "daty"
    return None, f"niejednoznaczne ({len(active)} rezerwacje)"


def get_header(headers, name):
    name_l = name.lower()
    for header in headers or []:
        if str(header.get("name", "")).lower() == name_l:
            return header.get("value", "")
    return ""


def parse_gmail_retry_delay(exc, attempt):
    text = str(exc or "")
    match = re.search(r"Retry after ([0-9T:\-\.]+Z)", text)
    if match:
        try:
            retry_at = datetime.fromisoformat(match.group(1).replace("Z", "+00:00"))
            now_utc = datetime.now(retry_at.tzinfo)
            seconds = max(int((retry_at - now_utc).total_seconds()) + 1, 1)
            return min(seconds, 180)
        except Exception:
            pass
    return min(2 ** attempt, 180)


def execute_gmail_request(request, label="", max_retries=6):
    for attempt in range(max_retries):
        try:
            return request.execute()
        except HttpError as exc:
            status = getattr(getattr(exc, "resp", None), "status", None)
            text = str(exc or "")
            is_rate_limit = status == 429 or "rateLimitExceeded" in text or "userRateLimitExceeded" in text
            if not is_rate_limit or attempt == max_retries - 1:
                raise
            wait_seconds = parse_gmail_retry_delay(exc, attempt + 1)
            print(
                f"Gmail rate limit for {label or 'request'} "
                f"(attempt {attempt + 1}/{max_retries}), retry in {wait_seconds}s..."
            )
            time.sleep(wait_seconds)


def ensure_state_sheet(sheet_service):
    meta = sheet_service.get(spreadsheetId=SHEET_ID).execute()
    sheets = [s["properties"]["title"] for s in meta.get("sheets", [])]
    if STATE_SHEET_NAME in sheets:
        return
    sheet_service.batchUpdate(
        spreadsheetId=SHEET_ID,
        body={
            "requests": [
                {"addSheet": {"properties": {"title": STATE_SHEET_NAME, "hidden": True}}},
            ]
        },
    ).execute()


def read_sync_state(sheet_service):
    ensure_state_sheet(sheet_service)
    try:
        resp = sheet_service.values().get(
            spreadsheetId=SHEET_ID,
            range=f"{STATE_SHEET_NAME}!A:B",
        ).execute()
    except Exception:
        return {}
    state = {}
    for row in resp.get("values", []):
        if len(row) >= 2 and str(row[0]).strip():
            state[str(row[0]).strip()] = str(row[1]).strip()
    return state


def write_sync_state(sheet_service, state):
    ensure_state_sheet(sheet_service)
    values = [[key, value] for key, value in sorted(state.items())]
    sheet_service.values().clear(
        spreadsheetId=SHEET_ID,
        range=f"{STATE_SHEET_NAME}!A:B",
    ).execute()
    sheet_service.values().update(
        spreadsheetId=SHEET_ID,
        range=f"{STATE_SHEET_NAME}!A1",
        valueInputOption="RAW",
        body={"values": values or [[STATE_KEY_LAST_INTERNAL_MS, ""]]},
    ).execute()


def build_incremental_query(base_query, state, key=STATE_KEY_LAST_INTERNAL_MS, lookback_days=INITIAL_LOOKBACK_DAYS):
    last_internal_ms = int(str(state.get(key, "") or "0") or "0")
    if last_internal_ms > 0:
        after_seconds = max(0, (last_internal_ms // 1000) - GMAIL_QUERY_OVERLAP_SECONDS)
        return f"after:{after_seconds} {base_query}"
    return f"newer_than:{lookback_days}d {base_query}"


def list_gmail_messages(service, query):
    messages = []
    page_token = None
    while True:
        request = service.users().messages().list(
            userId="me",
            q=query,
            maxResults=100,
            pageToken=page_token,
        )
        resp = execute_gmail_request(request, label=f"messages.list q={query[:40]}")
        messages.extend(resp.get("messages", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return messages


def read_previos_by_reservation(sheet_service):
    resp = sheet_service.values().get(
        spreadsheetId=SHEET_ID,
        range=f"{SHEET_NAME}!A2:Y",
    ).execute()
    rows = resp.get("values", [])
    by_reservation = {}
    for idx, row in enumerate(rows, start=2):
        padded = row + [""] * (25 - len(row))
        res_id = str(padded[0]).strip()
        voucher = str(padded[1]).strip()
        for key in (res_id, voucher):
            if key:
                by_reservation[key] = {"row": idx, "values": padded}
    return by_reservation


def update_invoice_rows(sheet_service, updates):
    if not updates:
        return

    data_all = [
        {
            "range": f"{SHEET_NAME}!Q{row_number}:U{row_number}",
            "values": [cells],
        }
        for row_number, cells in sorted(updates.items())
    ]
    chunk_size = 40
    for start in range(0, len(data_all), chunk_size):
        chunk = data_all[start : start + chunk_size]
        for attempt in range(5):
            try:
                sheet_service.values().batchUpdate(
                    spreadsheetId=SHEET_ID,
                    body={
                        "valueInputOption": "RAW",
                        "data": chunk,
                    },
                ).execute()
                break
            except (HttpError, OSError) as exc:
                if attempt == 4:
                    raise
                wait_seconds = 2 ** attempt
                print(
                    f"Sheets batch update failed for rows chunk {start + 1}-{start + len(chunk)} "
                    f"(attempt {attempt + 1}/5): {exc}. Retrying in {wait_seconds}s..."
                )
                time.sleep(wait_seconds)


def update_guest_request_rows(sheet_service, updates):
    if not updates:
        return

    data_all = [
        {
            "range": f"{SHEET_NAME}!V{row_number}:Y{row_number}",
            "values": [cells],
        }
        for row_number, cells in sorted(updates.items())
    ]
    chunk_size = 40
    for start in range(0, len(data_all), chunk_size):
        chunk = data_all[start : start + chunk_size]
        for attempt in range(5):
            try:
                sheet_service.values().batchUpdate(
                    spreadsheetId=SHEET_ID,
                    body={
                        "valueInputOption": "RAW",
                        "data": chunk,
                    },
                ).execute()
                break
            except (HttpError, OSError) as exc:
                if attempt == 4:
                    raise
                wait_seconds = 2 ** attempt
                print(
                    f"Sheets guest request update failed for rows chunk {start + 1}-{start + len(chunk)} "
                    f"(attempt {attempt + 1}/5): {exc}. Retrying in {wait_seconds}s..."
                )
                time.sleep(wait_seconds)


def shorten_message(text):
    return re.sub(r"\s+", " ", str(text or "")).strip()[:500]


def merge_message(current, incoming, limit=500):
    current = shorten_message(current)
    incoming = shorten_message(incoming)
    if not current:
        return incoming
    if not incoming or incoming in current:
        return current
    return shorten_message(f"{current} | {incoming}")[:limit]


def build_invoice_cells(row, status, company, tax_id, source, message_text):
    current_status = row[16]
    current_company = row[17]
    current_tax_id = row[18]
    current_source = row[19]
    current_message = row[20]

    can_replace = invoice_priority(source, status, tax_id) > invoice_priority(
        current_source,
        current_status,
        current_tax_id,
    )
    can_enrich_tax_id = bool(tax_id and not current_tax_id)
    can_enrich_message = bool(message_text and message_text not in current_message)
    if not can_replace and not can_enrich_tax_id and not can_enrich_message:
        return None

    enriched_status = (
        "TAK"
        if can_enrich_tax_id and "wymaga" in normalize_text(current_status)
        else current_status
    )
    return [
        status if can_replace else enriched_status,
        company if can_replace and company else (current_company or company),
        tax_id if can_replace else (current_tax_id or tax_id),
        source if can_replace else (current_source or source),
        message_text if can_replace else merge_message(current_message, message_text),
    ]


def main():
    gmail = gmail_service()
    sheets = sheets_service()
    state = read_sync_state(sheets)
    reservations = read_previos_by_reservation(sheets)
    invoice_query = build_incremental_query(GMAIL_QUERY_BASE, state)
    booking_invoice_query = build_incremental_query(BOOKING_INVOICE_QUERY_BASE, state)
    guest_request_query = build_incremental_query(BOOKING_GUEST_REQUEST_QUERY_BASE, state)
    airbnb_query = build_incremental_query(
        AIRBNB_GUEST_MESSAGE_QUERY_BASE,
        state,
        key=STATE_KEY_AIRBNB_LAST_INTERNAL_MS,
        lookback_days=AIRBNB_INITIAL_LOOKBACK_DAYS,
    )

    print(f"Invoice Gmail query: {invoice_query}")
    print(f"Booking invoice query: {booking_invoice_query}")
    print(f"Guest request query: {guest_request_query}")
    print(f"Airbnb guest message query: {airbnb_query}")

    invoice_messages = list_gmail_messages(gmail, invoice_query)
    booking_invoice_messages = list_gmail_messages(gmail, booking_invoice_query)
    guest_request_messages = list_gmail_messages(gmail, guest_request_query)
    airbnb_messages = list_gmail_messages(gmail, airbnb_query)
    max_internal_ms = int(str(state.get(STATE_KEY_LAST_INTERNAL_MS, "") or "0") or "0")

    matched = 0
    updated = 0
    skipped_no_match = 0
    pending_updates = {}
    updated_reservations = set()

    for msg_ref in invoice_messages:
        request = gmail.users().messages().get(
            userId="me",
            id=msg_ref["id"],
            format="full",
        )
        msg = execute_gmail_request(request, label=f"messages.get invoice {msg_ref['id']}")
        max_internal_ms = max(max_internal_ms, int(msg.get("internalDate", "0") or "0"))
        payload = msg.get("payload", {})
        subject = get_header(payload.get("headers", []), "Subject")
        body = payload_to_text(payload)
        text = f"{subject}\n{body}"

        if not is_previo_new_reservation_email(subject, body):
            continue

        status_raw = classify_invoice_request(text)
        if not status_raw:
            continue

        reservation_id = extract_reservation_id(text)
        if not reservation_id or reservation_id not in reservations:
            skipped_no_match += 1
            continue
        if reservation_id in updated_reservations:
            continue

        matched += 1
        row_info = reservations[reservation_id]
        row = row_info["values"]

        tax_id = extract_tax_number(text, allow_loose=True)
        status = "NIE" if status_raw == "NIE" else ("TAK" if tax_id else "WYMAGA DANYCH")
        company = extract_affiliation_company(text)
        source = infer_invoice_source(text)
        message_text = shorten_message(body)

        cells = build_invoice_cells(row, status, company, tax_id, source, message_text)
        if not cells:
            continue

        pending_updates[row_info["row"]] = cells
        row[16:21] = cells
        updated_reservations.add(reservation_id)
        updated += 1

    booking_invoice_matched = 0
    booking_invoice_updated = 0
    booking_invoice_skipped_no_match = 0

    for msg_ref in booking_invoice_messages:
        request = gmail.users().messages().get(
            userId="me",
            id=msg_ref["id"],
            format="full",
        )
        msg = execute_gmail_request(request, label=f"messages.get booking-invoice {msg_ref['id']}")
        max_internal_ms = max(max_internal_ms, int(msg.get("internalDate", "0") or "0"))
        payload = msg.get("payload", {})
        headers = payload.get("headers", [])
        subject = get_header(headers, "Subject")
        sender = get_header(headers, "From")
        body = payload_to_text(payload)
        text = f"{subject}\n{body}"

        if "guest.booking.com" not in normalize_text(sender):
            continue

        status_raw = classify_booking_invoice_request(text)
        if not status_raw:
            continue

        reservation_id = extract_reservation_id(text)
        if not reservation_id or reservation_id not in reservations:
            booking_invoice_skipped_no_match += 1
            continue

        booking_invoice_matched += 1
        row_info = reservations[reservation_id]
        row = row_info["values"]
        tax_id = extract_booking_guest_tax_number(text, reservation_id)
        status = "TAK" if tax_id else "WYMAGA DANYCH"
        company = extract_affiliation_company(text)
        source = infer_booking_invoice_source(text)
        message_text = shorten_message(body)

        cells = build_invoice_cells(row, status, company, tax_id, source, message_text)
        if not cells:
            continue

        pending_updates[row_info["row"]] = cells
        row[16:21] = cells
        booking_invoice_updated += 1

    # --- Airbnb guest messages: invoice requests + crib / high chair requests ---
    airbnb_index = build_airbnb_index(reservations)
    airbnb_max_internal_ms = int(str(state.get(STATE_KEY_AIRBNB_LAST_INTERNAL_MS, "") or "0") or "0")
    airbnb_invoice_updated = 0
    airbnb_request_found = 0
    airbnb_skipped_no_match = 0
    airbnb_request_items = []

    for msg_ref in airbnb_messages:
        request = gmail.users().messages().get(
            userId="me",
            id=msg_ref["id"],
            format="full",
        )
        msg = execute_gmail_request(request, label=f"messages.get airbnb {msg_ref['id']}")
        internal_ms = int(msg.get("internalDate", "0") or "0")
        airbnb_max_internal_ms = max(airbnb_max_internal_ms, internal_ms)
        payload = msg.get("payload", {})
        headers = payload.get("headers", [])
        subject = get_header(headers, "Subject")
        sender = get_header(headers, "From")
        if not is_airbnb_sender(sender):
            continue

        plain = payload_to_plain_text(payload)
        guest_messages, block_names = extract_airbnb_guest_blocks(plain)
        guest_text = " | ".join(m for m in guest_messages if m)
        if not guest_text:
            continue

        is_invoice = bool(classify_airbnb_invoice_request(guest_text))
        guest_request = classify_guest_request(guest_text)
        if not is_invoice and not guest_request["crib"] and not guest_request["chair"]:
            continue

        email_dt = datetime.fromtimestamp(internal_ms / 1000) if internal_ms else None
        check_in, check_out = extract_airbnb_stay_dates(subject, plain, email_dt)
        names = block_names + extract_airbnb_guest_names(subject, plain)
        codes = extract_airbnb_codes(f"{subject}\n{plain}")
        row_info, how = match_airbnb_reservation(
            reservations, airbnb_index, codes, names, check_in, check_out
        )
        if not row_info:
            airbnb_skipped_no_match += 1
            print(f"Airbnb message not matched ({how}): {subject[:90]}")
            continue

        row = row_info["values"]
        print(f"Airbnb message matched by {how}: row {row_info['row']} {row[6]} {row[11]}")

        if is_invoice:
            tax_id = extract_booking_guest_tax_number(guest_text, "")
            status = "TAK" if tax_id else "WYMAGA DANYCH"
            cells = build_invoice_cells(row, status, "", tax_id, AIRBNB_GUEST_SOURCE, shorten_message(guest_text))
            if cells:
                pending_updates[row_info["row"]] = cells
                row[16:21] = cells
                airbnb_invoice_updated += 1

        if guest_request["crib"] or guest_request["chair"]:
            airbnb_request_found += 1
            airbnb_request_items.append((row_info, guest_request, guest_text))

    update_invoice_rows(sheets, pending_updates)
    for row_number in sorted(pending_updates):
        print(f"Updated invoice info at row {row_number}")

    guest_matched = 0
    guest_updated = 0
    guest_skipped_no_match = 0
    guest_pending_updates = {}

    for msg_ref in guest_request_messages:
        request = gmail.users().messages().get(
            userId="me",
            id=msg_ref["id"],
            format="full",
        )
        msg = execute_gmail_request(request, label=f"messages.get guest-request {msg_ref['id']}")
        max_internal_ms = max(max_internal_ms, int(msg.get("internalDate", "0") or "0"))
        payload = msg.get("payload", {})
        headers = payload.get("headers", [])
        subject = get_header(headers, "Subject")
        sender = get_header(headers, "From")
        body = payload_to_text(payload)
        text = f"{subject}\n{body}"

        if "guest.booking.com" not in normalize_text(sender):
            continue

        request = classify_guest_request(text)
        if not request["crib"] and not request["chair"]:
            continue

        reservation_id = extract_reservation_id(text)
        if not reservation_id or reservation_id not in reservations:
            guest_skipped_no_match += 1
            continue

        guest_matched += 1
        row_info = reservations[reservation_id]
        row = row_info["values"]
        current_crib = row[21]
        current_chair = row[22]
        current_source = row[23]
        current_message = row[24]

        cells = [
            "TAK" if request["crib"] or current_crib else "",
            "TAK" if request["chair"] or current_chair else "",
            current_source or "Gmail / Booking guest message",
            merge_message(current_message, body),
        ]
        if cells == [current_crib, current_chair, current_source, current_message]:
            continue

        guest_pending_updates[row_info["row"]] = cells
        row[21:25] = cells
        guest_updated += 1

    for row_info, guest_request, guest_text in airbnb_request_items:
        row = row_info["values"]
        current_crib = row[21]
        current_chair = row[22]
        current_source = row[23]
        current_message = row[24]
        cells = [
            "TAK" if guest_request["crib"] or current_crib else "",
            "TAK" if guest_request["chair"] or current_chair else "",
            current_source or AIRBNB_GUEST_SOURCE,
            merge_message(current_message, guest_text),
        ]
        if cells == [current_crib, current_chair, current_source, current_message]:
            continue
        guest_pending_updates[row_info["row"]] = cells
        row[21:25] = cells
        guest_updated += 1

    update_guest_request_rows(sheets, guest_pending_updates)
    for row_number in sorted(guest_pending_updates):
        print(f"Updated guest request info at row {row_number}")

    if airbnb_max_internal_ms:
        state[STATE_KEY_AIRBNB_LAST_INTERNAL_MS] = str(airbnb_max_internal_ms)
    if max_internal_ms:
        state[STATE_KEY_LAST_INTERNAL_MS] = str(max_internal_ms)
    if max_internal_ms or airbnb_max_internal_ms:
        write_sync_state(sheets, state)
        print(f"Saved Gmail sync cursor: {max_internal_ms}, airbnb: {airbnb_max_internal_ms}")

    print(
        f"Done. invoice_messages={len(invoice_messages)}, invoice_matched={matched}, "
        f"invoice_updated={updated}, invoice_skipped_no_match={skipped_no_match}, "
        f"booking_invoice_messages={len(booking_invoice_messages)}, "
        f"booking_invoice_matched={booking_invoice_matched}, "
        f"booking_invoice_updated={booking_invoice_updated}, "
        f"booking_invoice_skipped_no_match={booking_invoice_skipped_no_match}, "
        f"guest_messages={len(guest_request_messages)}, guest_matched={guest_matched}, "
        f"guest_updated={guest_updated}, guest_skipped_no_match={guest_skipped_no_match}, "
        f"airbnb_messages={len(airbnb_messages)}, airbnb_invoice_updated={airbnb_invoice_updated}, "
        f"airbnb_requests={airbnb_request_found}, airbnb_skipped_no_match={airbnb_skipped_no_match}, "
        f"at={datetime.now().isoformat(timespec='seconds')}"
    )


if __name__ == "__main__":
    main()

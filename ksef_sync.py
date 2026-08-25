#!/usr/bin/env python3
print("KSEF_SYNC_BUILD_2026-08-25_17-30")

"""
ksef_sync.py
------------
Pobiera faktury zakupowe z KSeF API i zapisuje do Google Sheets (zakładka 'KSeF').
Uruchamiany przez GitHub Actions raz dziennie.

Wymagane secrets w GitHub (repo previo-sync):
  KSEF_TOKEN              — token wygenerowany w Aplikacji Podatnika KSeF 2.0
  KSEF_SPREADSHEET_ID     — ID arkusza Google Sheets (może być ten sam co Previo)
  GS_SA_JSON_B64          — Service Account JSON (base64) z uprawnieniami do zapisu

Zakładka 'KSeF' w arkuszu będzie zawierać kolumny:
  NumerKSeF | DataWystawienia | Sprzedawca | NIP Sprzedawcy | Netto | VAT | Brutto | TerminPlatnosci | DniDoPlatnosci | Alert
"""

import os
import json
import base64
import re
import time
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, date, timedelta

# ── CONFIG ──────────────────────────────────────────────────────────────────
NIP                  = "6793324449"
KSEF_API_BASE        = "https://api.ksef.mf.gov.pl/api/v2"
SHEET_NAME           = "KSeF"
KSEF_PAID_SHEET_NAME = "KsefPaid"
SPREADSHEET_ID       = os.environ["KSEF_SPREADSHEET_ID"]
KSEF_TOKEN           = os.environ["KSEF_TOKEN"]
GS_SA_JSON_B64       = os.environ.get("GS_SA_JSON_B64", "")
ALERT_DAYS           = 7
XML_FETCH_DELAY_SEC  = 6
QUERY_CHUNK_DAYS     = 7
KSEF_HISTORY_DAYS    = 730
KSEF_SHEET_MAX_ROWS  = 10000
KSEF_QUERY_RETRIES   = 6
KSEF_QUERY_BASE_WAIT = 20
KSEF_CHUNK_PAUSE_SEC = 1.5


def ksef_get_access_token():
    try:
        from ksef_client import KsefClient, KsefClientOptions, KsefEnvironment, models as m
        from ksef_client.services import AuthCoordinator
    except ImportError:
        raise ImportError("Zainstaluj: pip install ksef-client")

    options = KsefClientOptions(base_url=KsefEnvironment.PROD.value)
    with KsefClient(options) as client:
        token_cert_pem = client.security.get_public_key_certificate_pem(
            m.PublicKeyCertificateUsage.KSEFTOKENENCRYPTION,
        )
        auth = AuthCoordinator(client.auth).authenticate_with_ksef_token(
            token=KSEF_TOKEN,
            public_certificate=token_cert_pem,
            context_identifier_type="nip",
            context_identifier_value=NIP,
        )
        access_token = auth.access_token
        print("AccessToken uzyskany przez ksef-client SDK.")
        return access_token


def ksef_terminate_session(access_token):
    try:
        requests.delete(
            f"{KSEF_API_BASE}/sessions/current",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=20,
        )
        print("Sesja KSeF zamknięta.")
    except Exception:
        pass


def _parse_ksef_day(value):
    if isinstance(value, date):
        return value
    if isinstance(value, datetime):
        return value.date()
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    return date.fromisoformat(raw[:10])


def _format_ksef_datetime_start(value):
    day_value = _parse_ksef_day(value)
    if not day_value:
        raise ValueError(f"Nieprawidłowa data początkowa KSeF: {value!r}")
    return f"{day_value.isoformat()}T00:00:00Z"


def _format_ksef_datetime_end(value):
    day_value = _parse_ksef_day(value)
    if not day_value:
        raise ValueError(f"Nieprawidłowa data końcowa KSeF: {value!r}")
    return f"{day_value.isoformat()}T23:59:59Z"


def _ksef_query_invoices_chunk(access_token, date_from, date_to):
    from ksef_client import KsefClient, KsefClientOptions, KsefEnvironment
    from ksef_client.models import InvoiceQuerySubjectType, InvoiceQueryDateType

    all_invoices = []
    page_offset = 0
    page_size = 100
    range_from = _format_ksef_datetime_start(date_from)
    range_to = _format_ksef_datetime_end(date_to)

    date_type_candidates = [InvoiceQueryDateType.ISSUE]
    if hasattr(InvoiceQueryDateType, "PERMANENT_STORAGE"):
        date_type_candidates.append(InvoiceQueryDateType.PERMANENT_STORAGE)

    with KsefClient(KsefClientOptions(base_url=KsefEnvironment.PROD.value)) as client:
        last_error = None
        for date_type in date_type_candidates:
            try:
                page_offset = 0
                all_invoices = []
                while True:
                    resp = None
                    last_page_error = None
                    for attempt in range(1, KSEF_QUERY_RETRIES + 1):
                        try:
                            resp = client.invoices.query_invoice_metadata_by_date_range(
                                subject_type=InvoiceQuerySubjectType.SUBJECT2,
                                date_type=date_type,
                                date_from=range_from,
                                date_to=range_to,
                                access_token=access_token,
                                page_offset=page_offset,
                                page_size=page_size,
                            )
                            last_page_error = None
                            break
                        except Exception as e:
                            last_page_error = e
                            error_text = str(e)
                            if "429" not in error_text and "Too Many Requests" not in error_text:
                                raise
                            if attempt >= KSEF_QUERY_RETRIES:
                                raise
                            wait_seconds = KSEF_QUERY_BASE_WAIT * attempt
                            print(
                                f"⏳ KSeF rate limit dla zakresu {range_from} -> {range_to}, "
                                f"strona offset {page_offset}. Próba {attempt}/{KSEF_QUERY_RETRIES}, "
                                f"czekam {wait_seconds}s...",
                                flush=True,
                            )
                            time.sleep(wait_seconds)
                    if last_page_error and resp is None:
                        raise last_page_error
                    batch = resp.invoices or []
                    all_invoices.extend(batch)
                    if len(batch) < page_size or not getattr(resp, 'has_more', False):
                        return all_invoices
                    page_offset += page_size
                    time.sleep(0.3)
            except Exception as e:
                print(
                    f"⚠️ KSeF odrzucił query dla date_type={getattr(date_type, 'value', date_type)} "
                    f"i zakresu {range_from} -> {range_to}: {e}",
                    flush=True,
                )
                last_error = e

        if last_error:
            raise last_error

    return all_invoices


def ksef_query_invoices(access_token, date_from=None, date_to=None):
    if not date_from:
        date_from = date.today() - timedelta(days=KSEF_HISTORY_DAYS)
    if not date_to:
        date_to = date.today()

    start_day = _parse_ksef_day(date_from)
    end_day = _parse_ksef_day(date_to)
    if not start_day or not end_day:
        raise ValueError(f"Nieprawidłowy zakres dat KSeF: {date_from!r} - {date_to!r}")
    if start_day > end_day:
        raise ValueError(f"Zakres dat KSeF odwrócony: {start_day} > {end_day}")

    all_invoices = []
    seen_numbers = set()
    current_start = start_day

    while current_start <= end_day:
        current_end = min(current_start + timedelta(days=QUERY_CHUNK_DAYS - 1), end_day)
        print(f"Pobieranie listy faktur: {current_start.isoformat()} -> {current_end.isoformat()}", flush=True)
        try:
            batch = _ksef_query_invoices_chunk(
                access_token,
                date_from=current_start,
                date_to=current_end,
            )
        except Exception as e:
            print(f"❌ Błąd zapytania KSeF dla zakresu {current_start.isoformat()} -> {current_end.isoformat()}: {e}")
            raise

        for inv in batch:
            if hasattr(inv, "ksef_number"):
                ksef_number = (inv.ksef_number or "").strip()
            else:
                ksef_number = str(inv.get("ksefReferenceNumber") or inv.get("ksefNumber", "")).strip()

            key = ksef_number or json.dumps(inv, sort_keys=True, default=str)
            if key in seen_numbers:
                continue
            seen_numbers.add(key)
            all_invoices.append(inv)

        current_start = current_end + timedelta(days=1)
        time.sleep(KSEF_CHUNK_PAUSE_SEC)

    print(f"Znaleziono {len(all_invoices)} faktur zakupowych")
    return all_invoices


def ksef_get_invoice_xml(access_token, ksef_number):
    from ksef_client import KsefClient, KsefClientOptions, KsefEnvironment

    with KsefClient(KsefClientOptions(base_url=KsefEnvironment.PROD.value)) as client:
        return client.invoices.download_invoice(ksef_number=ksef_number, access_token=access_token)


def parse_invoice_xml(xml_bytes):
    if isinstance(xml_bytes, bytes):
        root = ET.fromstring(xml_bytes)
    else:
        root = ET.fromstring(xml_bytes.encode("utf-8"))

    def local_name(tag):
        return tag.split("}", 1)[1] if "}" in tag else tag

    def first_text(*names):
        for el in root.iter():
            if local_name(el.tag) in names and el.text:
                value = el.text.strip()
                if value:
                    return value
        return ""

    def normalize_date_text(value):
        if not value:
            return ""
        text = str(value).strip()
        if not text:
            return ""
        patterns = [
            (r"(\d{4}-\d{2}-\d{2})", lambda m: m.group(1)),
            (r"(\d{4}/\d{2}/\d{2})", lambda m: m.group(1).replace("/", "-")),
            (r"(\d{1,2})\.(\d{1,2})\.(\d{4})", lambda m: f"{int(m.group(3)):04d}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"),
            (r"(\d{1,2})/(\d{1,2})/(\d{4})", lambda m: f"{int(m.group(3)):04d}-{int(m.group(2)):02d}-{int(m.group(1)):02d}"),
        ]
        for pattern, formatter in patterns:
            match = re.search(pattern, text)
            if not match:
                continue
            normalized = formatter(match)
            try:
                date.fromisoformat(normalized)
                return normalized
            except Exception:
                continue
        return ""

    def find_date_in_element(el):
        direct = normalize_date_text(el.text or "")
        if direct:
            return direct
        for child in el.iter():
            candidate = normalize_date_text(child.text or "")
            if candidate:
                return candidate
        return ""

    def parse_iso_date(value):
        if not value:
            return None
        try:
            return date.fromisoformat(str(value)[:10])
        except Exception:
            return None

    def find_payment_due_date():
        strict_names = {
            "TerminPlatnosci",
            "TerminPłatnosci",
            "TerminPłatności",
            "TerminZaplaty",
            "TerminZapłaty",
            "DataPlatnosci",
            "DataPłatnosci",
            "DataPłatności",
            "PaymentDueDate",
            "DueDate",
        }
        for el in root.iter():
            tag_name = local_name(el.tag)
            lower_name = tag_name.lower()
            if tag_name not in strict_names and not (
                "termin" in lower_name and ("plat" in lower_name or "zaplat" in lower_name or "due" in lower_name)
            ):
                continue
            candidate = find_date_in_element(el)
            if candidate:
                return candidate
        for el in root.iter():
            lower_name = local_name(el.tag).lower()
            if not any(token in lower_name for token in ("platn", "płatn", "zaplat", "zapl", "payment", "due")):
                continue
            candidate = find_date_in_element(el)
            if candidate:
                return candidate
        return ""

    def find_paid_date():
        for el in root.iter():
            if local_name(el.tag) == "DataZaplaty":
                value = normalize_date_text(el.text or "")
                if value:
                    return value
        return ""

    def find_partial_payment_completion_date():
        partial_dates = []
        partial_payment_flag = ""
        for el in root.iter():
            name = local_name(el.tag)
            text = (el.text or "").strip()
            if not text:
                continue
            if name == "ZnacznikZaplatyCzesciowej" and not partial_payment_flag:
                partial_payment_flag = text
            elif name == "DataZaplatyCzesciowej":
                parsed = parse_iso_date(text)
                if parsed:
                    partial_dates.append(parsed)
        if partial_payment_flag == "2" and partial_dates:
            return max(partial_dates).isoformat()
        return ""

    def is_marked_as_paid():
        for el in root.iter():
            if local_name(el.tag) == "Zaplacono" and (el.text or "").strip() == "1":
                return True
        return False

    def find_due_date_from_description(issue_date_text):
        base_date = parse_iso_date(issue_date_text)
        if not base_date:
            return ""
        for el in root.iter():
            if local_name(el.tag) != "TerminPlatnosci":
                continue
            quantity = ""
            unit = ""
            event = ""
            for child in el.iter():
                child_name = local_name(child.tag)
                child_text = (child.text or "").strip()
                if not child_text:
                    continue
                if child_name == "Ilosc" and not quantity:
                    quantity = child_text
                elif child_name == "Jednostka" and not unit:
                    unit = child_text.lower()
                elif child_name == "ZdarzeniePoczatkowe" and not event:
                    event = child_text.lower()
            if not quantity or not unit:
                continue
            try:
                amount = int(quantity)
            except ValueError:
                continue
            if event and not any(phrase in event for phrase in ("wystaw", "invoice issue", "issue of the invoice", "data faktury", "invoice date")):
                continue
            if "day" in unit or "dni" in unit or "dzie" in unit:
                return (base_date + timedelta(days=amount)).isoformat()
            if "week" in unit or "tygod" in unit:
                return (base_date + timedelta(weeks=amount)).isoformat()
            if "month" in unit or "miesi" in unit:
                month_index = base_date.month - 1 + amount
                year = base_date.year + month_index // 12
                month = month_index % 12 + 1
                month_lengths = [31, 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
                day = min(base_date.day, month_lengths[month - 1])
                return date(year, month, day).isoformat()
        return ""

    sprzedawca_nip = first_text("NIP")
    sprzedawca_nazwa = first_text("PelnaNazwa", "Nazwa")
    p1 = first_text("P_1", "P1")
    p13 = first_text("P_13_1", "P13_1", "P_13")
    p15 = first_text("P_15", "P15")
    p16 = first_text("P_16", "P16")

    termin = find_payment_due_date() or first_text("P_22", "P22")
    if not termin:
        termin = find_due_date_from_description(p1)
    if not termin:
        termin = find_partial_payment_completion_date()
    if not termin:
        termin = find_paid_date()
    if not termin and is_marked_as_paid():
        termin = p1

    return {
        "data_wystawienia": p1,
        "sprzedawca_nip": sprzedawca_nip,
        "sprzedawca_nazwa": sprzedawca_nazwa,
        "termin_platnosci": termin,
        "netto": p13 or p15,
        "vat": p16,
        "brutto": p15,
    }


def get_sheets_token():
    if not GS_SA_JSON_B64:
        raise ValueError("GS_SA_JSON_B64 nie ustawiony")
    sa_json = json.loads(base64.b64decode(GS_SA_JSON_B64))

    from cryptography.hazmat.primitives import serialization, hashes
    from cryptography.hazmat.primitives.asymmetric import padding

    header = base64.urlsafe_b64encode(json.dumps({"alg": "RS256", "typ": "JWT"}).encode()).rstrip(b"=")
    now = int(time.time())
    claim = base64.urlsafe_b64encode(json.dumps({
        "iss": sa_json["client_email"],
        "scope": "https://www.googleapis.com/auth/spreadsheets",
        "aud": "https://oauth2.googleapis.com/token",
        "iat": now,
        "exp": now + 3600,
    }).encode()).rstrip(b"=")
    signing_input = header + b"." + claim

    private_key = serialization.load_pem_private_key(
        sa_json["private_key"].encode(),
        password=None,
    )
    signature = private_key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    jwt = signing_input + b"." + base64.urlsafe_b64encode(signature).rstrip(b"=")

    resp = requests.post("https://oauth2.googleapis.com/token", data={
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": jwt.decode(),
    })
    resp.raise_for_status()
    return resp.json()["access_token"]


def parse_money_value(value):
    if value is None:
        return 0.0
    raw = str(value).replace(" ", "").replace("\xa0", "").replace(",", ".")
    cleaned = "".join(ch for ch in raw if ch.isdigit() or ch in ".-")
    try:
        return float(cleaned)
    except Exception:
        return 0.0


def normalize_ksef_paid_entry(entry, brutto_value):
    if not isinstance(entry, dict):
        return {"payments": [], "updatedAt": ""}
    payments = entry.get("payments")
    if not isinstance(payments, list):
        payments = []
    if not payments and entry.get("paidDate"):
        fallback_amount = parse_money_value(entry.get("paidAmount")) or max(brutto_value, 0)
        payments = [{"date": str(entry.get("paidDate") or "").strip(), "amount": fallback_amount}]
    normalized = []
    for payment in payments:
        if not isinstance(payment, dict):
            continue
        payment_date = str(payment.get("date") or "").strip()
        payment_amount = max(parse_money_value(payment.get("amount")), 0)
        if payment_date and payment_amount > 0:
            normalized.append({"date": payment_date, "amount": payment_amount})
    normalized.sort(key=lambda item: item["date"])
    return {"payments": normalized, "updatedAt": str(entry.get("updatedAt") or "").strip()}


def build_ksef_paid_summary(entry, brutto_value):
    normalized = normalize_ksef_paid_entry(entry, brutto_value)
    paid_amount = sum(item["amount"] for item in normalized["payments"])
    remaining_amount = max((brutto_value or 0) - paid_amount, 0)
    latest_paid_date = normalized["payments"][-1]["date"] if normalized["payments"] else ""
    status = ""
    if paid_amount > 0:
        status = "TAK" if remaining_amount <= 0.009 else "CZESCIOWO"
    return {
        "status": status,
        "latestPaidDate": latest_paid_date,
        "paidAmount": paid_amount,
        "remainingAmount": remaining_amount,
    }


def read_ksef_paid_map(token):
    hdrs = {"Authorization": f"Bearer {token}"}
    url_range = requests.utils.quote(f"{KSEF_PAID_SHEET_NAME}!A2")
    resp = requests.get(
        f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}/values/{url_range}",
        headers=hdrs,
    )
    if not resp.ok:
        print(f"Nie udało się wczytać {KSEF_PAID_SHEET_NAME}: {resp.status_code} {resp.text[:300]}")
        return {}
    values = resp.json().get("values", [])
    raw = values[0][0] if values and values[0] else ""
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        return data if isinstance(data, dict) else {}
    except Exception as e:
        print(f"Nie udało się sparsować JSON z {KSEF_PAID_SHEET_NAME}: {e}")
        return {}


def write_to_sheets(rows_data):
    token = get_sheets_token()
    hdrs = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    base_url = f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}"
    paid_map = read_ksef_paid_map(token)

    meta = requests.get(base_url, headers=hdrs, params={"fields": "sheets.properties.title"})
    meta.raise_for_status()
    sheets = [s["properties"]["title"] for s in meta.json().get("sheets", [])]
    if SHEET_NAME not in sheets:
        requests.post(f"{base_url}:batchUpdate", headers=hdrs, json={
            "requests": [{"addSheet": {"properties": {"title": SHEET_NAME}}}]
        }).raise_for_status()
        print(f"Utworzono zakładkę '{SHEET_NAME}'")

    header_row = [
        "Nr KSeF", "Data wystawienia", "Sprzedawca", "NIP sprzedawcy",
        "Netto", "VAT", "Brutto", "Termin płatności", "Dni do płatności", "Alert", "Aktualizacja",
        "Oplacona", "Data oplaćenia", "Kwota oplacona", "Kwota pozostala"
    ]

    rows = [header_row]
    for row in rows_data:
        ksef_number = str(row[0] or "").strip()
        brutto_value = parse_money_value(row[6] if len(row) > 6 else 0)
        summary = build_ksef_paid_summary(paid_map.get(ksef_number), brutto_value)
        rows.append(row + [
            summary["status"],
            summary["latestPaidDate"],
            summary["paidAmount"] if summary["paidAmount"] > 0 else "",
            summary["remainingAmount"] if brutto_value > 0 else "",
        ])

    requests.post(
        f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}/values:batchClear",
        headers=hdrs,
        json={"ranges": [f"{SHEET_NAME}!A1:O{KSEF_SHEET_MAX_ROWS}"]}
    )
    resp = requests.post(
        f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}/values:batchUpdate",
        headers=hdrs,
        json={
            "valueInputOption": "RAW",
            "data": [{"range": f"{SHEET_NAME}!A1:O{KSEF_SHEET_MAX_ROWS}", "values": rows}]
        }
    )
    if not resp.ok:
        print(f"Sheets error {resp.status_code}: {resp.text[:500]}")
    resp.raise_for_status()
    print(f"✅ Zapisano {len(rows_data)} faktur do arkusza '{SHEET_NAME}'")


def main():
    print("=== KSeF Sync ===")
    print(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"NIP: {NIP}")

    print("Inicjowanie sesji KSeF...")
    access_token = ksef_get_access_token()
    print("Sesja aktywna.")

    try:
        date_from = date.today() - timedelta(days=KSEF_HISTORY_DAYS)
        date_to = date.today()
        invoices = ksef_query_invoices(access_token, date_from=date_from, date_to=date_to)

        existing = {}
        try:
            token_s = get_sheets_token()
            hdrs_s = {"Authorization": f"Bearer {token_s}"}
            url_range_read = requests.utils.quote(f"{SHEET_NAME}!A2:K{KSEF_SHEET_MAX_ROWS}")
            r_read = requests.get(
                f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}/values/{url_range_read}",
                headers=hdrs_s
            )
            if r_read.ok:
                for row in r_read.json().get("values", []):
                    if row and len(row) >= 8:
                        existing[row[0]] = row
            print(f"Wczytano {len(existing)} istniejących wierszy z arkusza")
        except Exception as e:
            print(f"Nie udało się wczytać cache: {e}")

        xml_fetched = 0
        rows = []
        today = date.today()

        for inv in invoices:
            if hasattr(inv, "ksef_number"):
                ksef_number = inv.ksef_number or ""
                inv_date = (inv.issue_date or "")[:10]
                brutto_meta = str(inv.gross_amount or "")
                netto_meta = str(inv.net_amount or "")
                seller_obj = getattr(inv, "seller", None)
                sprzedawca_meta = getattr(seller_obj, "name", "") if seller_obj else ""
                nip_meta = getattr(seller_obj, "nip", "") if seller_obj else ""
            else:
                ksef_number = inv.get("ksefReferenceNumber") or inv.get("ksefNumber", "")
                inv_date = (inv.get("issueDate") or "")[:10]
                brutto_meta = str(inv.get("grossValue", "") or "")
                netto_meta = str(inv.get("netAmount", "") or "")
                sprzedawca_meta = inv.get("subjectName", "")
                nip_meta = ""

            termin_z_cache = ""
            if ksef_number in existing:
                cached = existing[ksef_number]
                termin_z_cache = cached[7] if len(cached) > 7 else ""

            parsed = {}
            if termin_z_cache:
                parsed = {"termin_platnosci": termin_z_cache}
                cached = existing.get(ksef_number, [])
                if len(cached) >= 7:
                    parsed["sprzedawca_nazwa"] = cached[2] if len(cached) > 2 else ""
                    parsed["sprzedawca_nip"] = cached[3] if len(cached) > 3 else ""
                    parsed["netto"] = cached[4] if len(cached) > 4 else ""
                    parsed["vat"] = cached[5] if len(cached) > 5 else ""
                    parsed["brutto"] = cached[6] if len(cached) > 6 else ""
            elif ksef_number:
                try:
                    time.sleep(XML_FETCH_DELAY_SEC)
                    xml_bytes = ksef_get_invoice_xml(access_token, ksef_number)
                    parsed = parse_invoice_xml(xml_bytes)
                    xml_fetched += 1
                    termin_log = parsed.get("termin_platnosci", "") or "BRAK"
                    print(f"  XML {xml_fetched}: {ksef_number[-12:]} | termin: {termin_log[:10]}")
                except Exception as e:
                    print(f"⚠️ Brak XML {ksef_number[-12:]}: {e}")

            sprzedawca = parsed.get("sprzedawca_nazwa") or sprzedawca_meta
            nip_sp = parsed.get("sprzedawca_nip", "") or nip_meta
            netto = parsed.get("netto", "") or netto_meta
            vat = parsed.get("vat", "")
            brutto = parsed.get("brutto", "") or brutto_meta
            termin_str = parsed.get("termin_platnosci", "")

            dni_do = ""
            alert = "NIE"
            if termin_str:
                try:
                    termin_date = date.fromisoformat(termin_str[:10])
                    dni_do = (termin_date - today).days
                    if dni_do <= ALERT_DAYS:
                        alert = "TAK"
                except Exception:
                    pass

            rows.append([
                ksef_number,
                inv_date,
                sprzedawca,
                nip_sp,
                netto,
                vat,
                brutto,
                termin_str,
                dni_do,
                alert,
                datetime.now().strftime("%Y-%m-%d %H:%M"),
            ])

        if not rows:
            print("Brak faktur zakupowych — nic do zapisania.")
            return

        rows.sort(key=lambda r: ((r[7] or "9999-99-99"), (r[2] or ""), (r[0] or "")))
        write_to_sheets(rows)
        print("Gotowe.")
    finally:
        ksef_terminate_session(access_token)


if __name__ == "__main__":
    main()

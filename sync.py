#!/usr/bin/env python3
"""
Previo API -> Google Sheets sync script
Runs via GitHub Actions every hour
"""
import json
import os
import re
import unicodedata
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ── CONFIG ──────────────────────────────────────────────
PREVIO_URL = "https://api.previo.app/x1/hotel/searchReservations"
PREVIO_LOGIN = os.environ["PREVIO_LOGIN"]
PREVIO_PASS = os.environ["PREVIO_PASS"]
PREVIO_HOT_ID = os.environ.get("PREVIO_HOT_ID", "762331")

SHEET_ID = os.environ["GOOGLE_SHEET_ID"]
SHEET_NAME = "Previo"
SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT"]

# Fetch reservations by checkout date starting from 2025-01-01.
DATE_FROM = "2025-01-01"
DATE_TO = (datetime.now() + timedelta(days=90)).strftime("%Y-%m-%d")
RESERVATION_LIMIT = 2000
INVOICE_HEADERS = [
    "Faktura status",
    "Firma do faktury",
    "NIP/VAT",
    "Źródło faktury",
    "Wiadomość fakturowa",
]
GUEST_REQUEST_HEADERS = [
    "Łóżeczko",
    "Krzesełko",
    "Źródło próśb",
    "Wiadomość próśb",
]


def normalize_text(value):
    text = unicodedata.normalize("NFD", str(value or ""))
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return text.lower()


def clean_tax_number(value):
    digits = re.sub(r"\D", "", str(value or ""))
    return digits[:10] if len(digits) >= 10 else ""


def is_valid_tax_number(value):
    return bool(clean_tax_number(value))


def is_ignored_company_marker(value):
    compact = re.sub(r"[^a-z0-9]+", "", normalize_text(value))
    return compact in {"kasuj", "skasuj", "usun", "usunac", "delete", "remove"}


def extract_tax_number(text):
    if not text:
        return ""

    patterns = [
        r"number\s*=\s*([A-Za-z0-9\- ]{6,30})\s+numbertype\s*=\s*vat",
        r"(?:nip|tax\s*id|vat\s*id)\s*[:=]?\s*([A-Za-z0-9\- ]{6,30})",
        r"\b(\d{3}[- ]?\d{3}[- ]?\d{2}[- ]?\d{2})\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            tax_number = clean_tax_number(match.group(1))
            if tax_number:
                return tax_number
    return ""


def extract_affiliation_company(note):
    match = re.search(
        r"affiliation:\s*name=(.*?)\s+number=",
        note or "",
        flags=re.IGNORECASE | re.DOTALL,
    )
    if match:
        return " ".join(match.group(1).split())
    return ""


def invoice_priority(info):
    source = normalize_text(info.get("source", ""))
    status = normalize_text(info.get("status", ""))
    has_tax_id = is_valid_tax_number(info.get("tax_id", ""))
    if not status:
        return 0
    if has_tax_id and ("gmail" in source or "mail" in source or "booking.com email" in source):
        return 6
    if has_tax_id and ("previo note" in source or "log" in source or "affiliation" in source):
        return 5
    if "previo note" in source or "log" in source or "affiliation" in source:
        return 4
    if "gmail" in source or "mail" in source or "booking.com email" in source:
        return 3
    if "recznie" in source or "ręcznie" in source or "company/name" in source:
        return 2
    return 1


def is_ignored_invoice_info(info):
    info = info or {}
    source = normalize_text(info.get("source", ""))
    return (
        ("company/name" in source or "recznie" in source or "ręcznie" in source)
        and is_ignored_company_marker(info.get("company", ""))
    )


def merge_invoice_info(primary, fallback):
    """Keep higher-priority source, but enrich missing company/NIP/message from fallback."""
    primary = primary or {}
    fallback = fallback or {}
    if is_ignored_invoice_info(primary):
        primary = {}
    if is_ignored_invoice_info(fallback):
        fallback = {}
    if invoice_priority(fallback) > invoice_priority(primary):
        primary, fallback = fallback, primary

    merged = dict(primary)
    if merged.get("tax_id") and not is_valid_tax_number(merged.get("tax_id")):
        merged["tax_id"] = ""
    for key in ("company", "tax_id", "message"):
        if key == "tax_id" and fallback.get(key) and not is_valid_tax_number(fallback.get(key)):
            continue
        if not merged.get(key) and fallback.get(key):
            merged[key] = fallback[key]
    return merged


def invoice_info_to_cells(info):
    return [
        info.get("status", ""),
        info.get("company", ""),
        info.get("tax_id", ""),
        info.get("source", ""),
        info.get("message", ""),
    ]


def cells_to_invoice_info(cells):
    cells = (cells or []) + [""] * 5
    tax_id = str(cells[2] or "").strip()
    if tax_id and not is_valid_tax_number(tax_id):
        tax_id = ""
    info = {
        "status": str(cells[0] or "").strip(),
        "company": str(cells[1] or "").strip(),
        "tax_id": tax_id,
        "source": str(cells[3] or "").strip(),
        "message": str(cells[4] or "").strip(),
    }
    if is_ignored_invoice_info(info):
        return {"status": "", "company": "", "tax_id": "", "source": "", "message": ""}
    return info


def guest_request_info_to_cells(info):
    info = info or {}
    return [
        info.get("crib", ""),
        info.get("chair", ""),
        info.get("source", ""),
        info.get("message", ""),
    ]


def cells_to_guest_request_info(cells):
    cells = (cells or []) + [""] * 4
    return {
        "crib": str(cells[0] or "").strip(),
        "chair": str(cells[1] or "").strip(),
        "source": str(cells[2] or "").strip(),
        "message": str(cells[3] or "").strip(),
    }


def local_tag_name(element):
    return str(element.tag or "").split("}", 1)[-1]


def int_from_text(value):
    text = str(value or "").strip()
    return int(text) if re.fullmatch(r"\d{1,3}", text) else 0


def extract_guest_count_from_note(note):
    text = normalize_text(note)
    patterns = [
        r"(?:liczba\s+osob|osoby|osob|goscie|gosci|guests|persons|people|pax)\s*[:=\-]?\s*(\d{1,2})",
        r"(\d{1,2})\s*(?:osob|osoby|gosci|guests|persons|people|pax)\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            value = int_from_text(match.group(1))
            if value:
                return value
    adults = 0
    children = 0
    adult_match = re.search(r"(?:dorosli|adults?)\s*[:=\-]?\s*(\d{1,2})", text)
    child_match = re.search(r"(?:dzieci|children|kids?)\s*[:=\-]?\s*(\d{1,2})", text)
    if adult_match:
        adults = int_from_text(adult_match.group(1))
    if child_match:
        children = int_from_text(child_match.group(1))
    return adults + children


def extract_guest_count(res, note=""):
    candidate_names = {
        "guestcount",
        "guestscount",
        "personcount",
        "personscount",
        "peoplecount",
        "pax",
        "paxcount",
        "occupancy",
        "numberofguests",
        "numberofpersons",
        "numberofpeople",
        "totalguests",
        "totalpersons",
        "totalpeople",
        "guestnumber",
        "personnumber",
    }
    adult_names = {"adults", "adult", "adultcount", "adultsnumber", "numberofadults"}
    child_names = {"children", "child", "childcount", "childrennumber", "numberofchildren", "kids"}

    adults = 0
    children = 0
    repeated_people = 0

    for element in res.iter():
        name = re.sub(r"[^a-z0-9]", "", normalize_text(local_tag_name(element)))
        value = int_from_text(element.text)
        if value:
            if name in candidate_names:
                return value
            if name in adult_names:
                adults += value
            elif name in child_names:
                children += value

        has_identity = any(
            re.sub(r"[^a-z0-9]", "", normalize_text(local_tag_name(child))) in {"name", "firstname", "lastname", "surname"}
            for child in list(element)
        )
        if name in {"person", "guest"} and has_identity:
            repeated_people += 1

    if adults or children:
        return adults + children
    if repeated_people > 1:
        return repeated_people
    return extract_guest_count_from_note(note)


def collect_guest_count_debug_tags(res):
    matches = []
    keywords = ("guest", "person", "pax", "adult", "child", "occup")
    for element in res.iter():
        raw_name = local_tag_name(element)
        name = normalize_text(raw_name)
        if not any(keyword in name for keyword in keywords):
            continue
        text = " ".join(str(element.text or "").split())
        child_count = len(list(element))
        matches.append(f"{raw_name}={text[:40] or '<children:'+str(child_count)+'>'}")
        if len(matches) >= 20:
            break
    return ", ".join(matches)


def extract_invoice_info(note, company_name):
    note = note or ""
    company_name = (company_name or "").strip()
    if is_ignored_company_marker(company_name):
        company_name = ""
    norm = normalize_text(note)

    company = extract_affiliation_company(note)
    tax_id = extract_tax_number(note)
    has_affiliation = "affiliation:" in norm
    has_company_flag = "type=company" in norm or "numbertype=vat" in norm

    if has_affiliation or has_company_flag or tax_id:
        return {
            "status": "TAK" if tax_id else "TAK - BRAK NIP",
            "company": company or company_name,
            "tax_id": tax_id,
            "source": "Previo note / affiliation",
            "message": note[:500],
        }

    negative_patterns = [
        "bez faktur",
        "nie potrzebuje faktur",
        "nie chce faktur",
        "nie prosze o faktur",
        "no invoice",
        "invoice not needed",
        "without invoice",
    ]
    if any(pattern in norm for pattern in negative_patterns):
        return {
            "status": "NIE",
            "company": "",
            "tax_id": "",
            "source": "Previo note",
            "message": note[:500],
        }

    invoice_patterns = [
        "faktur",
        "invoice",
        "vat invoice",
        "nip",
        "na firme",
        "dane do faktur",
        "company invoice",
        "billing details",
    ]
    if any(pattern in norm for pattern in invoice_patterns):
        return {
            "status": "TAK" if tax_id else "WYMAGA DANYCH",
            "company": company or company_name,
            "tax_id": tax_id,
            "source": "Previo note",
            "message": note[:500],
        }

    if company_name:
        return {
            "status": "TAK - BRAK NIP",
            "company": company_name,
            "tax_id": "",
            "source": "Previo ręcznie / company/name",
            "message": "",
        }

    return {"status": "", "company": "", "tax_id": "", "source": "", "message": ""}


def fetch_reservations():
    xml_body = f"""<?xml version="1.0" encoding="utf-8"?>
<request>
  <login>{PREVIO_LOGIN}</login>
  <password>{PREVIO_PASS}</password>
  <hotId>{PREVIO_HOT_ID}</hotId>
  <term>
    <from>{DATE_FROM}</from>
    <to>{DATE_TO}</to>
    <termType>check-out</termType>
  </term>
  <limit>{RESERVATION_LIMIT}</limit>
</request>"""

    resp = requests.post(
        PREVIO_URL,
        data=xml_body.encode("utf-8"),
        headers={"Content-Type": "application/xml"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.content


def parse_reservations(xml_bytes):
    root = ET.fromstring(xml_bytes)
    rows = []
    missing_guest_debug = []

    for res in root.findall(".//reservation"):
        def t(tag, default=""):
            el = res.find(tag)
            return el.text.strip() if el is not None and el.text else default

        note = t("note")
        guest_count = extract_guest_count(res, note)
        if not guest_count and len(missing_guest_debug) < 3:
            missing_guest_debug.append(f"{t('resId') or t('voucher')}: {collect_guest_count_debug_tags(res)}")
        channel = "Własna"
        note_lower = note.lower()
        if "airbnb" in note_lower:
            channel = "Airbnb"
        elif "booking" in note_lower:
            channel = "Booking.com"
        elif "expedia" in note_lower:
            channel = "Expedia"
        elif "profitroom" in note_lower:
            channel = "Profitroom"

        date_from_full = t("term/from")[:16] if t("term/from") else ""
        date_to_full = t("term/to")[:16] if t("term/to") else ""
        date_from = date_from_full[:10]
        date_to = date_to_full[:10]

        nights = 0
        try:
            d1 = datetime.strptime(date_from, "%Y-%m-%d")
            d2 = datetime.strptime(date_to, "%Y-%m-%d")
            nights = (d2 - d1).days
        except Exception:
            pass

        company_name = t("company/name").strip()
        invoice_info = extract_invoice_info(note, company_name)

        rows.append(
            [
                t("resId"),
                t("voucher"),
                t("created")[:10],
                date_from_full,
                date_to_full,
                nights,
                t("object/name"),
                channel,
                t("price"),
                t("currency/code"),
                t("status/statusId"),
                t("guest/name"),
                t("guest/countryCode"),
                t("contactPerson/phone"),
                company_name,
                datetime.now().strftime("%Y-%m-%d %H:%M"),
                *invoice_info_to_cells(invoice_info),
                *guest_request_info_to_cells({}),
                guest_count or "",
            ]
        )

    if missing_guest_debug:
        print("Could not detect guest count for sample reservations:")
        for item in missing_guest_debug:
            print(f"  {item}")

    return rows


def get_sheets_service():
    creds_dict = json.loads(SERVICE_ACCOUNT_JSON)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds).spreadsheets()


def ensure_sheet_exists(service):
    meta = service.get(spreadsheetId=SHEET_ID).execute()
    sheets = [s["properties"]["title"] for s in meta["sheets"]]

    if SHEET_NAME not in sheets:
        service.batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": SHEET_NAME}}}]},
        ).execute()
        print(f"Created sheet: {SHEET_NAME}")


def read_existing_maps(service):
    try:
        result = service.values().get(
            spreadsheetId=SHEET_ID,
            range=f"{SHEET_NAME}!A2:Z",
        ).execute()
    except Exception:
        return {}, {}

    invoice_map = {}
    request_map = {}
    for row in result.get("values", []):
        row = row + [""] * 26
        res_id = str(row[0] or "").strip()
        voucher = str(row[1] or "").strip()
        info = cells_to_invoice_info(row[16:21])
        request_info = cells_to_guest_request_info(row[21:25])
        for key in (res_id, voucher):
            if key and info.get("status"):
                invoice_map[key] = info
            if key and (request_info.get("crib") or request_info.get("chair")):
                request_map[key] = request_info
    return invoice_map, request_map


def apply_existing_data(rows, existing_invoice_map, existing_request_map):
    merged_rows = []
    for row in rows:
        row = row + [""] * (26 - len(row))
        current = cells_to_invoice_info(row[16:21])
        existing = existing_invoice_map.get(str(row[0]).strip()) or existing_invoice_map.get(str(row[1]).strip())
        merged = merge_invoice_info(current, existing)
        request = existing_request_map.get(str(row[0]).strip()) or existing_request_map.get(str(row[1]).strip()) or {}
        merged_rows.append(row[:16] + invoice_info_to_cells(merged) + guest_request_info_to_cells(request) + [row[25]])
    return merged_rows


def write_to_sheets(service, rows):
    headers = [
        "ID Rezerwacji",
        "Voucher",
        "Data rezerwacji",
        "Data od",
        "Data do",
        "Noce",
        "Apartament",
        "Kanał",
        "Cena",
        "Waluta",
        "Status",
        "Gość",
        "Kraj",
        "Telefon",
        "Firma",
        "Ostatnia aktualizacja",
        *INVOICE_HEADERS,
        *GUEST_REQUEST_HEADERS,
        "Osoby",
    ]

    service.values().clear(
        spreadsheetId=SHEET_ID,
        range=f"{SHEET_NAME}!A:Z",
    ).execute()

    service.values().update(
        spreadsheetId=SHEET_ID,
        range=f"{SHEET_NAME}!A1",
        valueInputOption="RAW",
        body={"values": [headers] + rows},
    ).execute()

    print(f"Written {len(rows)} reservations to Google Sheets")


def main():
    print(f"Fetching reservations {DATE_FROM} - {DATE_TO}...")
    xml_data = fetch_reservations()
    rows = parse_reservations(xml_data)
    print(f"Parsed {len(rows)} reservations")

    service = get_sheets_service()
    ensure_sheet_exists(service)
    existing_invoice_map, existing_request_map = read_existing_maps(service)
    rows = apply_existing_data(rows, existing_invoice_map, existing_request_map)
    write_to_sheets(service, rows)
    print("Done!")


if __name__ == "__main__":
    main()

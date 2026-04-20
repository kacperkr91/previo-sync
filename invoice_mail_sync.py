#!/usr/bin/env python3
"""
Gmail -> Google Sheets invoice request sync.

Scans Gmail messages for invoice requests, matches them to Previo reservations
by reservation/voucher number, and updates Q:U in the "Previo" sheet.
"""
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

GMAIL_QUERY = (
    'newer_than:365d '
    '("Nowa rezerwacja" OR "System zarządzania obiektem PREVIO" OR PREVIO) '
    '(affiliation OR numbertype OR "Informacje dodatkowe")'
)


def normalize_text(value):
    text = unicodedata.normalize("NFD", str(value or ""))
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return text.lower()


def clean_tax_number(value):
    digits = re.sub(r"\D", "", str(value or ""))
    return digits[:10] if len(digits) >= 10 else ""


def extract_tax_number(text):
    patterns = [
        r"number\s*=\s*([A-Za-z0-9\- ]{6,30})\s+numbertype\s*=\s*vat",
        r"(?:nip|vat|tax\s*id|vat\s*id)\s*[:=]?\s*([A-Za-z0-9\- ]{6,30})",
        r"\b(\d{3}[- ]?\d{3}[- ]?\d{2}[- ]?\d{2})\b",
    ]
    for pattern in patterns:
        match = re.search(pattern, text or "", flags=re.IGNORECASE)
        if match:
            tax_id = clean_tax_number(match.group(1))
            if tax_id:
                return tax_id
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


def extract_affiliation_company(text):
    match = re.search(
        r"affiliation:\s*name=(.*?)\s+number=",
        text or "",
        flags=re.IGNORECASE | re.DOTALL,
    )
    return " ".join(match.group(1).split()) if match else ""


def infer_invoice_source(text):
    return "Gmail / Previo confirmation"


def invoice_priority(source, status, tax_id=""):
    src = normalize_text(source)
    st = normalize_text(status)
    has_tax_id = bool(clean_tax_number(tax_id))
    if not st:
        return 0
    if has_tax_id and ("gmail" in src or "booking.com email" in src or "mail" in src):
        return 6
    if has_tax_id and ("previo note" in src or "affiliation" in src or "log" in src):
        return 5
    if "previo note" in src or "affiliation" in src or "log" in src:
        return 4
    if "gmail" in src or "booking.com email" in src or "mail" in src:
        return 3
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


def get_header(headers, name):
    name_l = name.lower()
    for header in headers or []:
        if str(header.get("name", "")).lower() == name_l:
            return header.get("value", "")
    return ""


def list_gmail_messages(service):
    messages = []
    page_token = None
    while True:
        resp = service.users().messages().list(
            userId="me",
            q=GMAIL_QUERY,
            maxResults=100,
            pageToken=page_token,
        ).execute()
        messages.extend(resp.get("messages", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return messages


def read_previos_by_reservation(sheet_service):
    resp = sheet_service.values().get(
        spreadsheetId=SHEET_ID,
        range=f"{SHEET_NAME}!A2:U",
    ).execute()
    rows = resp.get("values", [])
    by_reservation = {}
    for idx, row in enumerate(rows, start=2):
        padded = row + [""] * (21 - len(row))
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


def shorten_message(text):
    return re.sub(r"\s+", " ", str(text or "")).strip()[:500]


def main():
    gmail = gmail_service()
    sheets = sheets_service()
    reservations = read_previos_by_reservation(sheets)
    messages = list_gmail_messages(gmail)

    matched = 0
    updated = 0
    skipped_no_match = 0
    pending_updates = {}
    updated_reservations = set()

    for msg_ref in messages:
        msg = gmail.users().messages().get(
            userId="me",
            id=msg_ref["id"],
            format="full",
        ).execute()
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
        current_status = row[16]
        current_company = row[17]
        current_tax_id = row[18]
        current_source = row[19]
        current_message = row[20]

        tax_id = extract_tax_number(text)
        status = "NIE" if status_raw == "NIE" else ("TAK" if tax_id else "WYMAGA DANYCH")
        company = extract_affiliation_company(text)
        source = infer_invoice_source(text)
        message_text = shorten_message(body)

        can_replace = invoice_priority(source, status, tax_id) > invoice_priority(
            current_source,
            current_status,
            current_tax_id,
        )
        can_enrich_tax_id = bool(tax_id and not current_tax_id)
        can_enrich_message = bool(message_text and not current_message)
        if not can_replace and not can_enrich_tax_id and not can_enrich_message:
            continue

        enriched_status = (
            "TAK"
            if can_enrich_tax_id and "wymaga" in normalize_text(current_status)
            else current_status
        )
        cells = [
            status if can_replace else enriched_status,
            company if can_replace and company else (current_company or company),
            tax_id if can_replace else (current_tax_id or tax_id),
            source if can_replace else current_source,
            message_text if can_replace else (current_message or message_text),
        ]
        pending_updates[row_info["row"]] = cells
        row[16:21] = cells
        updated_reservations.add(reservation_id)
        updated += 1

    update_invoice_rows(sheets, pending_updates)
    for row_number in sorted(pending_updates):
        print(f"Updated invoice info at row {row_number}")

    print(
        f"Done. Gmail messages={len(messages)}, matched={matched}, "
        f"updated={updated}, skipped_no_match={skipped_no_match}, at={datetime.now().isoformat(timespec='seconds')}"
    )


if __name__ == "__main__":
    main()

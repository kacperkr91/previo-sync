#!/usr/bin/env python3
"""
Previo API -> Google Sheets sync script
Runs via GitHub Actions every hour
"""
import json
import os
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

    for res in root.findall(".//reservation"):
        def t(tag, default=""):
            el = res.find(tag)
            return el.text.strip() if el is not None and el.text else default

        note = t("note")
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
                t("company/name").strip(),
                datetime.now().strftime("%Y-%m-%d %H:%M"),
            ]
        )

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
    ]

    service.values().clear(
        spreadsheetId=SHEET_ID,
        range=f"{SHEET_NAME}!A:P",
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
    write_to_sheets(service, rows)
    print("Done!")


if __name__ == "__main__":
    main()

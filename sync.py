#!/usr/bin/env python3
"""
Previo API -> Google Sheets sync script
Runs via GitHub Actions every hour
"""
import os
import xml.etree.ElementTree as ET
import requests
import json
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ── CONFIG ──────────────────────────────────────────────
PREVIO_URL    = "https://api.previo.app/x1/hotel/searchReservations"
PREVIO_LOGIN  = os.environ["PREVIO_LOGIN"]
PREVIO_PASS   = os.environ["PREVIO_PASS"]
PREVIO_HOT_ID = os.environ.get("PREVIO_HOT_ID", "762331")

SHEET_ID      = os.environ["GOOGLE_SHEET_ID"]
SHEET_NAME    = "Previo"   # Tab name in Google Sheets
SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT"]

# Fetch month by month to overcome 300 limit
FETCH_START = "2025-01-01"
FETCH_END   = "2026-12-31" 

# ── FETCH FROM PREVIO ────────────────────────────────────
def fetch_reservations():
    import xml.etree.ElementTree as ET2
    from datetime import date
    
    # Fetch month by month to overcome Previo's 300 reservation limit
    all_rows = []
    start = date.fromisoformat(FETCH_START)
    end   = date.fromisoformat(FETCH_END)
    
    current = start.replace(day=1)
    while current <= end:
        # Last day of month
        if current.month == 12:
            next_month = current.replace(year=current.year+1, month=1, day=1)
        else:
            next_month = current.replace(month=current.month+1, day=1)
        month_end = next_month - timedelta(days=1)
        
        xml_body = f"""<?xml version="1.0" encoding="utf-8"?>
<request>
  <login>{PREVIO_LOGIN}</login>
  <password>{PREVIO_PASS}</password>
  <hotId>{PREVIO_HOT_ID}</hotId>
  <term>
    <from>{current.isoformat()}</from>
    <to>{month_end.isoformat()}</to>
    <termType>check-out</termType>
  </term>
  <limit>300</limit>
</request>"""
        resp = requests.post(
            PREVIO_URL,
            data=xml_body.encode("utf-8"),
            headers={"Content-Type": "application/xml"},
            timeout=30
        )
        resp.raise_for_status()
        root = ET2.fromstring(resp.content)
        month_rows = root.findall(".//reservation")
        all_rows.extend(month_rows)
        print(f"  {current.strftime('%Y-%m')}: {len(month_rows)} reservations")
        
        current = next_month
    
    print(f"Total: {len(all_rows)} reservations")
    combined = ET2.Element("reservations")
    for r in all_rows:
        combined.append(r)
    return ET2.tostring(combined, encoding="utf-8")

def parse_reservations(xml_bytes):
    root = ET.fromstring(xml_bytes)
    rows = []
    
    for res in root.findall(".//reservation"):
        def t(tag, default=""):
            el = res.find(tag)
            return el.text.strip() if el is not None and el.text else default
        
        # Extract channel from <note> field
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
        
        # Parse dates
        date_from = t("term/from")[:10] if t("term/from") else ""
        date_to   = t("term/to")[:10]   if t("term/to")   else ""
        
        # Calculate nights
        nights = 0
        try:
            d1 = datetime.strptime(date_from, "%Y-%m-%d")
            d2 = datetime.strptime(date_to,   "%Y-%m-%d")
            nights = (d2 - d1).days
        except:
            pass
        
        rows.append([
            t("resId"),                          # A: ID rezerwacji
            t("voucher"),                        # B: Numer voucher
            t("created")[:10],                   # C: Data rezerwacji
            date_from,                           # D: Data od
            date_to,                             # E: Data do
            nights,                              # F: Liczba nocy
            t("object/name"),                    # G: Apartament
            channel,                             # H: Kanał
            t("price"),                          # I: Cena
            t("currency/code"),                  # J: Waluta
            t("status/statusId"),                # K: Status ID
            t("guest/name"),                     # L: Gość
            t("guest/countryCode"),              # M: Kraj
            t("contactPerson/phone"),            # N: Telefon
            datetime.now().strftime("%Y-%m-%d %H:%M"),  # O: Ostatnia aktualizacja
        ])
    
    return rows

# ── GOOGLE SHEETS ────────────────────────────────────────
def get_sheets_service():
    creds_dict = json.loads(SERVICE_ACCOUNT_JSON)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds).spreadsheets()

def ensure_sheet_exists(service):
    """Create 'Previo' tab if it doesn't exist"""
    meta = service.get(spreadsheetId=SHEET_ID).execute()
    sheets = [s["properties"]["title"] for s in meta["sheets"]]
    
    if SHEET_NAME not in sheets:
        service.batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": SHEET_NAME}}}]}
        ).execute()
        print(f"Created sheet: {SHEET_NAME}")

def write_to_sheets(service, rows):
    # Headers
    headers = [
        "ID Rezerwacji", "Voucher", "Data rezerwacji", "Data od", "Data do",
        "Noce", "Apartament", "Kanał", "Cena", "Waluta", "Status",
        "Gość", "Kraj", "Telefon", "Ostatnia aktualizacja"
    ]
    
    # Clear and rewrite
    service.values().clear(
        spreadsheetId=SHEET_ID,
        range=f"{SHEET_NAME}!A:O"
    ).execute()
    
    service.values().update(
        spreadsheetId=SHEET_ID,
        range=f"{SHEET_NAME}!A1",
        valueInputOption="RAW",
        body={"values": [headers] + rows}
    ).execute()
    
    print(f"Written {len(rows)} reservations to Google Sheets")

# ── MAIN ─────────────────────────────────────────────────
def main():
    print(f"Fetching reservations {FETCH_START} - {FETCH_END} month by month...")
    
    xml_data = fetch_reservations()
    rows = parse_reservations(xml_data)
    print(f"Parsed {len(rows)} reservations")
    
    service = get_sheets_service()
    ensure_sheet_exists(service)
    write_to_sheets(service, rows)
    print("Done!")

if __name__ == "__main__":
    main()

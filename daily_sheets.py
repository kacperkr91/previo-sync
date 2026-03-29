#!/usr/bin/env python3
"""
Previo API -> Daily Sheets sync
Runs via GitHub Actions every day at 7:00 AM
Fills today's tab in the daily reservations spreadsheet
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

DAILY_SHEET_ID = os.environ["DAILY_SHEET_ID"]
SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT"]

# Today's date
TODAY = datetime.now()
TODAY_STR     = TODAY.strftime("%Y-%m-%d")
TAB_NAME      = TODAY.strftime("%-d.%m")   # e.g. "24.03"
PREV_TAB_NAME = (TODAY - timedelta(days=1)).strftime("%-d.%m")  # e.g. "23.03"

AIRBNB_COMMISSION = 0.155  # 15.5%

# ── CHANNEL MAPPING ──────────────────────────────────────
def map_partner(raw):
    r = raw.lower()
    if "airbnb" in r:   return "AirBnBXML2"
    if "booking" in r:  return "Booking.com XML"
    if "profitroom" in r: return "ProfitRoomXML"
    if "expedia" in r:  return "Expedia"
    return raw

def calc_price(raw_price, partner_raw):
    """For Airbnb, gross up from net price"""
    price = float(raw_price or 0)
    if "airbnb" in partner_raw.lower():
        price = round(price / (1 - AIRBNB_COMMISSION), 2)
    return price

# ── FETCH FROM PREVIO ────────────────────────────────────
def fetch_today_reservations():
    xml_body = f"""<?xml version="1.0" encoding="utf-8"?>
<request>
  <login>{PREVIO_LOGIN}</login>
  <password>{PREVIO_PASS}</password>
  <hotId>{PREVIO_HOT_ID}</hotId>
  <term>
    <from>{TODAY_STR}</from>
    <to>{TODAY_STR}</to>
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
    return resp.content

def parse_reservations(xml_bytes):
    root = ET.fromstring(xml_bytes)
    rows = []

    for res in root.findall(".//reservation"):
        def t(tag, default=""):
            el = res.find(tag)
            return el.text.strip() if el is not None and el.text else default

        partner_raw = t("objectKind/name") or t("note")
        # Try to get partner from note field
        note = t("note")
        if "AirBnB" in note or "Airbnb" in note:
            partner_raw = "AirBnBXML2"
        elif "Booking" in note:
            partner_raw = "Booking.com XML"
        elif "ProfitRoom" in note or "Profitroom" in note:
            partner_raw = "ProfitRoomXML"

        # Status
        status_id = t("status/statusId")
        status = "S" if status_id in ("3","4") else "P"

        # Dates
        date_from = t("term/from")[:10]
        date_to   = t("term/to")[:10]
        created   = t("created")[:10]

        # Nights
        nights = 0
        try:
            d1 = datetime.strptime(date_from, "%Y-%m-%d")
            d2 = datetime.strptime(date_to,   "%Y-%m-%d")
            nights = (d2 - d1).days
        except:
            pass

        # Persons
        persons = 0
        for gc in res.findall(".//guestCategory"):
            try:
                persons += int(gc.find("guaId").text or 0)
            except:
                pass
        # fallback
        if persons == 0:
            try:
                persons = int(t("guest/guestCategory/guaId") or 1)
            except:
                persons = 1

        # Price
        raw_price = t("price")
        price = calc_price(raw_price, partner_raw)
        price_fmt = f"{price:.2f} zł".replace(".", ",")

        # Apartment
        apt = t("object/name")

        # Guest name
        guest = t("guest/name") or t("contactPerson/name")

        # Voucher / reservation number
        voucher = t("voucher") or t("resId")

        rows.append({
            "dataRez":  created,        # A
            "dataOd":   date_from,      # B
            "dataDo":   date_to,        # C
            "noce":     nights,         # D
            "osoby":    persons,        # E
            "gosc":     guest,          # F
            "nr":       voucher,        # G
            "partner":  map_partner(partner_raw),  # H
            "status":   status,         # I
            "apt":      apt,            # J
            "cena":     price,          # K (numeric for formatting)
            "cena_fmt": price_fmt,      # K display
        })

    # Sort by apartment name
    rows.sort(key=lambda r: r["apt"])
    return rows

# ── GOOGLE SHEETS ────────────────────────────────────────
def get_service():
    creds_dict = json.loads(SERVICE_ACCOUNT_JSON)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds).spreadsheets()

def get_sheet_id(service, tab_name):
    """Get sheetId for a tab by name"""
    meta = service.get(spreadsheetId=DAILY_SHEET_ID).execute()
    for s in meta["sheets"]:
        if s["properties"]["title"] == tab_name:
            return s["properties"]["sheetId"]
    return None

def copy_tab_from_previous(service):
    """Copy previous day's tab as template for today"""
    prev_id = get_sheet_id(service, PREV_TAB_NAME)
    today_id = get_sheet_id(service, TAB_NAME)

    if today_id is not None:
        print(f"Tab '{TAB_NAME}' already exists")
        return today_id

    if prev_id is None:
        print(f"Previous tab '{PREV_TAB_NAME}' not found, creating blank tab")
        service.batchUpdate(
            spreadsheetId=DAILY_SHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": TAB_NAME}}}]}
        ).execute()
        return get_sheet_id(service, TAB_NAME)

    # Duplicate previous tab
    resp = service.sheets().copyTo(
        spreadsheetId=DAILY_SHEET_ID,
        sheetId=prev_id,
        body={"destinationSpreadsheetId": DAILY_SHEET_ID}
    ).execute()

    new_sheet_id = resp["sheetId"]

    # Rename to today
    service.batchUpdate(
        spreadsheetId=DAILY_SHEET_ID,
        body={"requests": [{
            "updateSheetProperties": {
                "properties": {"sheetId": new_sheet_id, "title": TAB_NAME},
                "fields": "title"
            }
        }]}
    ).execute()

    print(f"Created tab '{TAB_NAME}' by copying '{PREV_TAB_NAME}'")
    return new_sheet_id

def clear_data_rows(service):
    """Clear data rows (3 onwards) but keep headers and formatting"""
    # Clear columns A-M from row 3 onwards (keep headers in row 2)
    service.values().clear(
        spreadsheetId=DAILY_SHEET_ID,
        range=f"'{TAB_NAME}'!A3:M200"
    ).execute()

def write_reservations(service, rows):
    """Write reservation data to sheet"""
    values = []
    for i, r in enumerate(rows):
        row_num = i + 3  # starts from row 3
        values.append([
            r["dataRez"],           # A - Data rezerwacji
            r["dataOd"],            # B - Data od
            r["dataDo"],            # C - Data do
            r["noce"],              # D - Noce
            r["osoby"],             # E - Osoby
            r["gosc"],              # F - Goście
            r["nr"],                # G - Nr rezerwacji
            r["partner"],           # H - Partner
            r["status"],            # I - Status
            r["apt"],               # J - Apartament
            r["cena"],              # K - Cena z systemu (numeric)
            "",                     # L - Dopłata (manual)
            f"=K{row_num}+L{row_num}",  # M - Cena całkowita = K + L
        ])

    if not values:
        print("No reservations to write")
        return

    service.values().update(
        spreadsheetId=DAILY_SHEET_ID,
        range=f"'{TAB_NAME}'!A3",
        valueInputOption="USER_ENTERED",
        body={"values": values}
    ).execute()

    print(f"Written {len(values)} reservations to tab '{TAB_NAME}'")

def update_tab_date(service):
    """Update cell A1 with today's date"""
    service.values().update(
        spreadsheetId=DAILY_SHEET_ID,
        range=f"'{TAB_NAME}'!A1",
        valueInputOption="USER_ENTERED",
        body={"values": [[TODAY_STR]]}
    ).execute()

# ── MAIN ─────────────────────────────────────────────────
def main():
    print(f"Daily sync for {TAB_NAME} ({TODAY_STR})")

    # Fetch from Previo
    print("Fetching from Previo API...")
    xml_data = fetch_today_reservations()
    rows = parse_reservations(xml_data)
    print(f"Found {len(rows)} reservations checking out today")

    # Google Sheets
    service = get_service()

    # Create today's tab (copy from yesterday)
    copy_tab_from_previous(service)

    # Clear old data rows
    clear_data_rows(service)

    # Write today's date in A1
    update_tab_date(service)

    # Write reservations
    write_reservations(service, rows)

    print(f"Done! Tab '{TAB_NAME}' updated.")

if __name__ == "__main__":
    main()

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
import unicodedata
from datetime import datetime, timedelta
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ── CONFIG ──────────────────────────────────────────────
PREVIO_URL    = "https://api.previo.app/x1/hotel/searchReservations"
PREVIO_LOGIN  = os.environ["PREVIO_LOGIN"]
PREVIO_PASS   = os.environ["PREVIO_PASS"]
PREVIO_HOT_ID = os.environ.get("PREVIO_HOT_ID", "762331")

DAILY_SHEET_ID = os.environ["DAILY_SHEET_ID"]
PREVIO_SHEET_ID = os.environ.get("GOOGLE_SHEET_ID", "")
PREVIO_SHEET_NAME = "Previo"
SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT"]

# Today's date
TODAY = datetime.now()
TODAY_STR     = TODAY.strftime("%Y-%m-%d")
TAB_NAME      = TODAY.strftime("%d.%m")   # e.g. "01.04" with leading zero
PREV_TAB_NAME = (TODAY - timedelta(days=1)).strftime("%d.%m")  # e.g. "31.03"

AIRBNB_COMMISSION = 0.155  # 15.5%
DEBUG_TARGET_RESERVATION = os.environ.get("DEBUG_TARGET_RESERVATION", "HM5XP4K488").strip()


def normalize_text(value):
    text = str(value or "").replace("ł", "l").replace("Ł", "L")
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return text.lower().strip()

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


def extract_market_codes(res):
    codes = []
    for el in res.findall(".//marketCodeList/marketCode"):
        text = (el.text or "").strip()
        if text:
            codes.append(text)
    return codes

# ── FETCH FROM PREVIO ────────────────────────────────────
def fetch_today_reservations():
    # Fetch ±1 day range and filter manually — termType=check-out unreliable
    FETCH_FROM = (TODAY - timedelta(days=1)).strftime("%Y-%m-%d")
    FETCH_TO   = (TODAY + timedelta(days=1)).strftime("%Y-%m-%d")
    xml_body = f"""<?xml version="1.0" encoding="utf-8"?>
<request>
  <login>{PREVIO_LOGIN}</login>
  <password>{PREVIO_PASS}</password>
  <hotId>{PREVIO_HOT_ID}</hotId>
  <term>
    <from>{FETCH_FROM}</from>
    <to>{FETCH_TO}</to>
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
        date_to   = t("term/to")[:10].strip("'")

        # Filter: only checkouts TODAY
        if date_to != TODAY_STR:
            continue
        created   = t("created")[:10]

        # Nights
        nights = 0
        try:
            d1 = datetime.strptime(date_from, "%Y-%m-%d")
            d2 = datetime.strptime(date_to,   "%Y-%m-%d")
            nights = (d2 - d1).days
        except:
            pass

        # Persons — count guestCategory entries (each = 1 person)
        persons = len(res.findall(".//guestCategory"))
        if persons == 0:
            persons = 1
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

        market_codes = extract_market_codes(res)
        market_norm = [normalize_text(code) for code in market_codes]
        has_doplata = any("doplata" in code for code in market_norm)
        has_nota = any(code == "nota" or code.startswith("nota ") for code in market_norm)
        has_faktura_imienna = any("faktura imienna" in code or code == "fp" for code in market_norm)
        has_faktura = any(code == "faktura" or code.startswith("faktura ") for code in market_norm)

        cena_system = price
        doplata_marker = "Dopłata" if has_doplata else ""
        rachunek_marker = " / ".join([label for label, cond in (("Nota", has_nota), ("Faktura", has_faktura)) if cond])

        # Apartment
        apt = t("object/name")

        # Guest name
        guest = t("guest/name") or t("contactPerson/name")

        # Voucher / reservation number
        voucher = t("voucher") or t("resId")

        # Company / KASUJ
        company = t("company/name").strip()
        notatka = "Kasuj" if company.lower() == "kasuj" else ""

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
            "cena":     cena_system,    # K (numeric for formatting)
            "cena_fmt": price_fmt,      # K display
            "doplata_marker": doplata_marker,  # L - marker dopłaty
            "rachunek_marker": rachunek_marker,  # N - Nota / Faktura
            "pozycja_marker": " / ".join([label for label in ([notatka] if notatka else []) + (["FP"] if has_faktura_imienna else []) if label]),  # P
            "notatka":  notatka,        # compat
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

TEMPLATE_TAB = "_SZABLON"  # Always copy from this template tab

def copy_tab_from_previous(service):
    """Copy from template tab for today"""
    today_id = get_sheet_id(service, TAB_NAME)

    if today_id is not None:
        print(f"Tab '{TAB_NAME}' already exists")
        return today_id

    # Try template first, fall back to previous day
    template_id = get_sheet_id(service, TEMPLATE_TAB)
    source_id   = template_id or get_sheet_id(service, PREV_TAB_NAME)
    source_name = TEMPLATE_TAB if template_id else PREV_TAB_NAME

    if source_id is None:
        print(f"No template or previous tab found, creating blank tab")
        service.batchUpdate(
            spreadsheetId=DAILY_SHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": TAB_NAME}}}]}
        ).execute()
        return get_sheet_id(service, TAB_NAME)

    print(f"Copying from '{source_name}'")
    # Duplicate source tab
    resp = service.sheets().copyTo(
        spreadsheetId=DAILY_SHEET_ID,
        sheetId=source_id,
        body={"destinationSpreadsheetId": DAILY_SHEET_ID}
    ).execute()

    new_sheet_id = resp["sheetId"]

    # Rename to today AND move to first position
    service.batchUpdate(
        spreadsheetId=DAILY_SHEET_ID,
        body={"requests": [
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": new_sheet_id, "title": TAB_NAME},
                    "fields": "title"
                }
            },
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": new_sheet_id, "index": 0},
                    "fields": "index"
                }
            }
        ]}
    ).execute()

    print(f"Created tab '{TAB_NAME}' by copying '{PREV_TAB_NAME}'")
    return new_sheet_id

def clear_data_rows(service):
    """Clear data rows (3 onwards) but keep headers and formatting"""
    # Clear columns A-P from row 3 onwards (keep headers in row 2)
    service.values().clear(
        spreadsheetId=DAILY_SHEET_ID,
        range=f"'{TAB_NAME}'!A3:P200"
    ).execute()


def read_previo_markers(service):
    """Read markers from the main Previo sheet for today's checkouts."""
    if not PREVIO_SHEET_ID:
        print("Previo marker source missing: GOOGLE_SHEET_ID is empty for daily_sheets.py")
        return {}
    try:
        result = service.values().get(
            spreadsheetId=PREVIO_SHEET_ID,
            range=f"{PREVIO_SHEET_NAME}!A2:AD"
        ).execute()
    except Exception as exc:
        print(f"Could not read markers from main Previo sheet: {exc}")
        return {}

    marker_map = {}
    values = result.get("values", [])
    print(f"Previo rows fetched from main sheet: {len(values)}")
    date_match_count = 0
    marker_candidate_count = 0
    for row in values:
        padded = row + [""] * (30 - len(row))
        res_id = str(padded[0] or "").strip()
        voucher = str(padded[1] or "").strip()
        date_to = str(padded[4] or "").strip()[:10]
        if date_to != TODAY_STR:
            continue
        date_match_count += 1
        invoice_status = str(padded[16] or "").strip()  # Q - Faktura status
        raw_market_codes = str(padded[26] or "").strip()  # AA
        raw_market_norm = normalize_text(raw_market_codes)
        inferred_doplata = "Dopłata" if "doplata" in raw_market_norm else ""
        inferred_rachunek = " / ".join(
            [label for label, cond in (("Nota", "nota" in raw_market_norm), ("Faktura", "faktura" in raw_market_norm and "faktura imienna" not in raw_market_norm)) if cond]
        )
        if invoice_status:
            inferred_rachunek = merge_labels(inferred_rachunek, "Faktura")
        inferred_pozycja = "FP" if ("faktura imienna" in raw_market_norm or "| fp" in f"| {raw_market_norm}") else ""
        payload = {
            "doplata_marker": str(padded[27] or "").strip() or inferred_doplata,  # AB fallback from AA
            "rachunek_marker": str(padded[28] or "").strip() or inferred_rachunek,  # AC fallback from AA
            "pozycja_marker": str(padded[29] or "").strip() or inferred_pozycja,  # AD fallback from AA
        }
        if DEBUG_TARGET_RESERVATION and DEBUG_TARGET_RESERVATION in {voucher, res_id}:
            print(
                "Previo target row:",
                {
                    "res_id": res_id,
                    "voucher": voucher,
                    "date_to": date_to,
                    "invoice_status": invoice_status,
                    "market_codes": raw_market_codes,
                    "payload": payload,
                },
            )
        if payload["doplata_marker"] or payload["rachunek_marker"] or payload["pozycja_marker"]:
            marker_candidate_count += 1
        for key in (voucher, res_id):
            if key:
                marker_map[key] = payload
    print(f"Previo rows matching today's checkout: {date_match_count}")
    print(f"Previo rows with any marker for today: {marker_candidate_count}")
    return marker_map


def apply_previo_markers(rows, marker_map):
    for row in rows:
        marker = marker_map.get(str(row.get("nr", "")).strip())
        if DEBUG_TARGET_RESERVATION and str(row.get("nr", "")).strip() == DEBUG_TARGET_RESERVATION:
            print(
                "Daily target row before marker apply:",
                {
                    "nr": row.get("nr", ""),
                    "apt": row.get("apt", ""),
                    "current_l": row.get("doplata_marker", ""),
                    "current_n": row.get("rachunek_marker", ""),
                    "current_p": row.get("pozycja_marker", ""),
                    "marker_found": marker or {},
                },
            )
        if not marker:
            continue
        if marker.get("doplata_marker"):
            row["doplata_marker"] = marker["doplata_marker"]
        row["rachunek_marker"] = merge_labels(row.get("rachunek_marker", ""), marker.get("rachunek_marker", ""))
        row["pozycja_marker"] = merge_labels(row.get("pozycja_marker", ""), marker.get("pozycja_marker", ""))
        if DEBUG_TARGET_RESERVATION and str(row.get("nr", "")).strip() == DEBUG_TARGET_RESERVATION:
            print(
                "Daily target row after marker apply:",
                {
                    "nr": row.get("nr", ""),
                    "l": row.get("doplata_marker", ""),
                    "n": row.get("rachunek_marker", ""),
                    "p": row.get("pozycja_marker", ""),
                },
            )
    return rows

def merge_labels(existing, incoming):
    existing = str(existing or "").strip()
    incoming = str(incoming or "").strip()
    if not existing:
        return incoming
    if not incoming:
        return existing
    existing_parts = [p.strip() for p in existing.split("/") if p.strip()]
    incoming_parts = [p.strip() for p in incoming.split("/") if p.strip()]
    merged = []
    for part in existing_parts + incoming_parts:
        if part and part not in merged:
            merged.append(part)
    return " / ".join(merged)


def read_existing_overrides(service):
    """Read existing daily sheet values so sync can preserve manual edits."""
    try:
        result = service.values().get(
            spreadsheetId=DAILY_SHEET_ID,
            range=f"'{TAB_NAME}'!G3:P200"
        ).execute()
    except Exception:
        return {}

    overrides = {}
    for row in result.get("values", []):
        padded = row + [""] * (10 - len(row))
        reservation_number = str(padded[0] or "").strip()  # G
        if not reservation_number:
            continue
        overrides[reservation_number] = {
            "l": padded[5],  # L
            "n": padded[7],  # N
            "o": padded[8],  # O
            "p": padded[9],  # P
        }
    return overrides


def apply_existing_overrides(rows, overrides):
    for row in rows:
        existing = overrides.get(str(row.get("nr", "")).strip())
        if not existing:
            continue

        existing_l = str(existing.get("l", "")).strip()
        existing_n = str(existing.get("n", "")).strip()
        existing_o = str(existing.get("o", "")).strip()
        existing_p = str(existing.get("p", "")).strip()

        # Existing daily-sheet values are the source of truth for manual edits,
        # including intentional deletions back to blank.
        row["doplata_marker"] = existing_l
        row["rachunek_marker"] = existing_n
        row["payment_code"] = existing_o
        row["pozycja_marker"] = existing_p
    return rows


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
            r.get("doplata_marker", ""),  # L - marker Dopłata
            f'=IF(ISNUMBER(L{row_num});K{row_num}+L{row_num};K{row_num})',  # M - dodaj L tylko jeśli jest liczbą
            r.get("rachunek_marker", ""),  # N - Nota / Faktura
            r.get("payment_code", ""),  # O - kod płatności / zachowany wpis
            r.get("pozycja_marker", ""),   # P - FP / Kasuj
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

def has_reservation_data(service):
    """Check if today's tab already has reservation data (col A = dataRez)"""
    try:
        result = service.values().get(
            spreadsheetId=DAILY_SHEET_ID,
            range=f"'{TAB_NAME}'!A3:A10"
        ).execute()
        values = result.get('values', [])
        return any(row and row[0].strip() for row in values)
    except:
        return False

# ── MAIN ─────────────────────────────────────────────────
def main():
    print(f"Daily sync for {TAB_NAME} ({TODAY_STR})")

    # Google Sheets
    service = get_service()

    # Create today's tab (copy from yesterday) if not exists
    tab_existed = get_sheet_id(service, TAB_NAME) is not None
    copy_tab_from_previous(service)

    # Always clear col N (Nr rachunku) after copy — it gets copied from previous day
    # User will re-enter manually for today
    if not tab_existed:
        service.values().clear(
            spreadsheetId=DAILY_SHEET_ID,
            range=f"'{TAB_NAME}'!N3:N200"
        ).execute()
        service.values().clear(
            spreadsheetId=DAILY_SHEET_ID,
            range=f"'{TAB_NAME}'!P3:P200"
        ).execute()
        print(f"Cleared col N (rachunki) and col P (notatki) from copied template")

        # Set Q11 to red NIEGOTOWE status
        service.values().update(
            spreadsheetId=DAILY_SHEET_ID,
            range=f"'{TAB_NAME}'!Q11",
            valueInputOption="RAW",
            body={"values": [["🔴 NIEGOTOWE — NIE NABIJAJ"]]}
        ).execute()

        # Set red tab color and red Q11 formatting
        sheet_id = get_sheet_id(service, TAB_NAME)
        service.batchUpdate(
            spreadsheetId=DAILY_SHEET_ID,
            body={"requests": [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet_id,
                            "tabColorStyle": {"rgbColor": {"red": 0.06, "green": 0.73, "blue": 0.37}}
                        },
                        "fields": "tabColorStyle"
                    }
                },
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": 10,
                            "endRowIndex": 11,
                            "startColumnIndex": 16,
                            "endColumnIndex": 18
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": {"red": 0.83, "green": 0.0, "blue": 0.0},
                                "textFormat": {
                                    "foregroundColor": {"red": 1.0, "green": 1.0, "blue": 1.0},
                                    "bold": True,
                                    "fontSize": 10
                                },
                                "horizontalAlignment": "CENTER"
                            }
                        },
                        "fields": "userEnteredFormat"
                    }
                }
            ]}
        ).execute()
        print(f"Set red NIEGOTOWE status in Q11 and red tab color")

    # Fetch from Previo
    print("Fetching from Previo API...")
    xml_data = fetch_today_reservations()

    # Debug: show raw count from XML
    import xml.etree.ElementTree as _ET
    _root = _ET.fromstring(xml_data)
    _all = _root.findall(".//reservation")
    print(f"Raw reservations in XML: {len(_all)}")
    for _r in _all[:5]:
        _to = _r.find("term/to")
        _apt = _r.find("object/name")
        print(f"  Sample: apt={_apt.text if _apt is not None else '?'} dataDo={_to.text if _to is not None else '?'}")
    print(f"Looking for checkout date: '{TODAY_STR}'")

    rows = parse_reservations(xml_data)
    print(f"Found {len(rows)} reservations checking out today ({TODAY_STR})")

    previo_markers = read_previo_markers(service)
    print(f"Previo markers loaded: {len(previo_markers)}")
    if previo_markers:
        rows = apply_previo_markers(rows, previo_markers)
        print(f"Applied main Previo markers for {len(previo_markers)} reservations")

    existing_overrides = read_existing_overrides(service)
    if existing_overrides:
        rows = apply_existing_overrides(rows, existing_overrides)
        print(f"Preserved manual/previous values for {len(existing_overrides)} reservation rows")

    # Clear old data rows (A-P)
    clear_data_rows(service)

    # Write reservations
    write_reservations(service, rows)

    # Write payment formula in column O only for rows that do not already
    # have a preserved manual/existing value in the current daily tab.
    if rows:
        formulas = []
        for i, row in enumerate(rows):
            row_num = i + 3
            if str(row.get("payment_code", "")).strip():
                formulas.append([row["payment_code"]])
            else:
                formulas.append([
                    f'=IF($H{row_num}="Booking.com XML";"B";IF($H{row_num}="AirBnBXML2";"A";IF($H{row_num}="ProfitRoomXML";"PP";"")))'
                ])
        service.values().update(
            spreadsheetId=DAILY_SHEET_ID,
            range=f"'{TAB_NAME}'!O3",
            valueInputOption="USER_ENTERED",
            body={"values": formulas}
        ).execute()

    print(f"Done! Tab '{TAB_NAME}' updated.")

if __name__ == "__main__":
    main()

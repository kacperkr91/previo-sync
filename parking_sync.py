#!/usr/bin/env python3
"""
parking_sync.py
---------------
Pobiera ostatni mail "Stany kart" z Gmaila,
parsuje listę kart parkingowych i zapisuje do Google Sheets.

Uruchamiany przez GitHub Actions raz w tygodniu (lub ręcznie).

Wymagane secrets w GitHub:
  - GMAIL_REFRESH_TOKEN   — refresh token OAuth2 do Gmaila
  - GMAIL_CLIENT_ID       — client_id z Google Cloud Console
  - GMAIL_CLIENT_SECRET   — client_secret z Google Cloud Console
  - PARKING_SPREADSHEET_ID — ID arkusza Google Sheets
  - GS_API_KEY_WRITE      — Service Account JSON (base64) z uprawnieniami do zapisu
"""

import os
import re
import json
import base64
import requests
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build

# ── CONFIG ──────────────────────────────────────────────
PARKING_SHEET_NAME = "Parking"
PARKING_SPREADSHEET_ID = os.environ["PARKING_SPREADSHEET_ID"]
THRESHOLD_DAYS = 6

# Gmail OAuth2
GMAIL_CLIENT_ID     = os.environ["GMAIL_CLIENT_ID"]
GMAIL_CLIENT_SECRET = os.environ["GMAIL_CLIENT_SECRET"]
GMAIL_REFRESH_TOKEN = os.environ["GMAIL_REFRESH_TOKEN"]

# Google Sheets write access (Service Account JSON base64)
GS_SA_JSON_B64 = os.environ.get("GOOGLE_SERVICE_ACCOUNT", "")

# ── GMAIL AUTH ───────────────────────────────────────────
def get_gmail_access_token():
    """Wymień refresh token na access token."""
    resp = requests.post("https://oauth2.googleapis.com/token", data={
        "client_id":     GMAIL_CLIENT_ID,
        "client_secret": GMAIL_CLIENT_SECRET,
        "refresh_token": GMAIL_REFRESH_TOKEN,
        "grant_type":    "refresh_token",
    })
    resp.raise_for_status()
    return resp.json()["access_token"]

def gmail_search(access_token, query, max_results=1):
    resp = requests.get(
        "https://gmail.googleapis.com/gmail/v1/users/me/messages",
        headers={"Authorization": f"Bearer {access_token}"},
        params={"q": query, "maxResults": max_results},
    )
    resp.raise_for_status()
    messages = resp.json().get("messages", [])
    # Gmail REST API zwraca "id", nie "messageId" — normalizuj
    for m in messages:
        if "id" in m and "messageId" not in m:
            m["messageId"] = m["id"]
    return messages

def gmail_get_body(access_token, message_id):
    resp = requests.get(
        f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{message_id}",
        headers={"Authorization": f"Bearer {access_token}"},
        params={"format": "full"},
    )
    resp.raise_for_status()
    msg = resp.json()

    # Extract plain text body
    def extract_text(payload):
        if payload.get("mimeType") == "text/plain":
            data = payload.get("body", {}).get("data", "")
            return base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")
        for part in payload.get("parts", []):
            result = extract_text(part)
            if result:
                return result
        return ""

    return extract_text(msg.get("payload", {}))

# ── PARSER ───────────────────────────────────────────────
def parse_parking_email(text):
    """
    Parsuje treść maila z kartami parkingowymi.
    Format: LP \t NumerBiletu \t Data \t 23:59 \t Dni \t x24h \t + \t H \t h \t [zapas|miesięczny]
    """
    results = []
    lines = text.split("\n")
    lp = 1

    for line in lines:
        raw = line.replace(">", "").strip()
        if not raw:
            continue

        # Numer biletu (10–15 cyfr)
        bilet_m = re.search(r"\b(\d{10,15})\b", raw)
        if not bilet_m:
            continue
        bilet = bilet_m.group(1)

        # Data ważności DD.MM.YYYY
        data_m = re.search(r"(\d{1,2})\.(\d{2})\.(\d{4})", raw)
        if not data_m:
            continue
        data_str = f"{data_m.group(1).zfill(2)}.{data_m.group(2)}.{data_m.group(3)}"

        # Miesięczny — brak godzin
        if "miesięczny" in raw:
            results.append({
                "lp": lp, "bilet": bilet, "data_waznosci": data_str,
                "dni": 999, "h": 0, "total_h": 999*24,
                "godziny": "miesięczny", "typ": "miesięczny", "alert": False,
            })
            lp += 1
            continue

        # Parsuj dni i godziny
        parts = re.split(r"[\t\s]+", raw)
        parts = [p for p in parts if p]

        dni = 0
        h = 0
        found = False

        # Format: "8 x24h + 0 h" (tabulatory) lub "8x24h+0h" (sklejony)
        for i, part in enumerate(parts):
            if part == "x24h":
                dni = int(parts[i-1]) if i > 0 else 0
                plus_idx = parts.index("+", i) if "+" in parts[i:] else -1
                if plus_idx != -1 and plus_idx + 1 < len(parts):
                    h = int(parts[plus_idx + 1]) if parts[plus_idx + 1].isdigit() else 0
                found = True
                break
            elif re.match(r"^\d+x24h\+\d+h$", part):
                m2 = re.match(r"(\d+)x24h\+(\d+)h", part)
                if m2:
                    dni = int(m2.group(1))
                    h = int(m2.group(2))
                found = True
                break

        if not found:
            continue

        typ = "zapas" if "zapas" in raw else "normalny"
        alert = dni < THRESHOLD_DAYS  # wszystkie typy

        results.append({
            "lp": lp, "bilet": bilet, "data_waznosci": data_str,
            "dni": dni, "h": h, "total_h": dni * 24 + h,
            "godziny": f"{dni}x24h+{h}h", "typ": typ, "alert": alert,
        })
        lp += 1

    return results

# ── GOOGLE SHEETS WRITE ──────────────────────────────────
def get_sheets_service():
    """Uzyskaj service do zapisu przez Service Account — tak samo jak daily_sheets.py."""
    creds_dict = json.loads(GS_SA_JSON_B64)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds).spreadsheets()

def write_to_sheets(cards, mail_date, alert_count, service):
    """Dopisz nowy zestaw kart do historii w arkuszu Google Sheets."""

    # Sprawdź czy zakładka Parking istnieje, utwórz jeśli nie
    meta = service.get(spreadsheetId=PARKING_SPREADSHEET_ID, fields="sheets.properties.title").execute()
    sheets = [s["properties"]["title"] for s in meta.get("sheets", [])]

    if PARKING_SHEET_NAME not in sheets:
        service.batchUpdate(spreadsheetId=PARKING_SPREADSHEET_ID, body={
            "requests": [{"addSheet": {"properties": {"title": PARKING_SHEET_NAME}}}]
        }).execute()
        print(f"Utworzono zakładkę '{PARKING_SHEET_NAME}'")
        # Nagłówki
        service.values().update(
            spreadsheetId=PARKING_SPREADSHEET_ID,
            range=f"'{PARKING_SHEET_NAME}'!A1",
            valueInputOption="RAW",
            body={"values": [["Data maila", "L.P.", "Nr biletu", "Ważny do", "Godziny", "Dni", "H", "Typ", "Alert", "Mail ID"]]},
        ).execute()

    # Sprawdź ile wierszy jest już w arkuszu
    existing = service.values().get(
        spreadsheetId=PARKING_SPREADSHEET_ID,
        range=f"'{PARKING_SHEET_NAME}'!A:A"
    ).execute()
    next_row = len(existing.get("values", [])) + 1

    # Dopisz nowe wiersze na dół
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    rows = []
    for c in cards:
        rows.append([
            mail_date,
            c["lp"],
            c["bilet"],
            c["data_waznosci"],
            c["godziny"],
            c["dni"],
            c["h"],
            c["typ"],
            "TAK" if c["alert"] else "NIE",
            "",  # Mail ID zapisujemy osobno w K1
        ])

    service.values().update(
        spreadsheetId=PARKING_SPREADSHEET_ID,
        range=f"'{PARKING_SHEET_NAME}'!A{next_row}",
        valueInputOption="RAW",
        body={"values": rows},
    ).execute()
    print(f"✅ Dopisano {len(cards)} kart od {mail_date} (wiersze {next_row}–{next_row+len(cards)-1})")
    print(f"🚨 Kart do doładowania: {alert_count}")
    return next_row  # zwróć numer pierwszego wiersza tego zestawu

# ── MAIN ─────────────────────────────────────────────────
def get_synced_msg_ids(service):
    """Pobierz wszystkie już przetworzone ID maili z kolumny J."""
    try:
        result = service.values().get(
            spreadsheetId=PARKING_SPREADSHEET_ID,
            range=f"'{PARKING_SHEET_NAME}'!J:J"
        ).execute()
        vals = result.get("values", [])
        return set(str(v[0]).strip() for v in vals if v and str(v[0]).strip())
    except:
        return set()

def save_msg_id(service, msg_id, row):
    """Zapisz ID maila do kolumny J przy pierwszym wierszu tego zestawu."""
    service.values().update(
        spreadsheetId=PARKING_SPREADSHEET_ID,
        range=f"'{PARKING_SHEET_NAME}'!J{row}",
        valueInputOption="RAW",
        body={"values": [[msg_id]]},
    ).execute()

def main():
    print("=== Parking Sync ===")
    print(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    # 1. Pobierz token Gmail
    print("Pobieranie tokenu Gmail...")
    access_token = get_gmail_access_token()

    # 2. Znajdź ostatni mail "stany kart"
    print("Szukanie maila 'stany kart'...")
    messages = gmail_search(access_token, "subject:stany kart", max_results=1)
    if not messages:
        print("❌ Nie znaleziono maila 'stany kart'")
        return

    msg_id = messages[0]["messageId"]
    print(f"Znaleziono mail: {msg_id}")

    # 3. Sprawdź czy ten mail był już przetworzony
    service = get_sheets_service()
    synced_ids = get_synced_msg_ids(service)
    if msg_id in synced_ids:
        print(f"✅ Mail {msg_id} już przetworzony — brak zmian, pomijam zapis.")
        return

    # 4. Pobierz treść
    body = gmail_get_body(access_token, msg_id)
    if not body:
        print("❌ Pusta treść maila")
        return

    # 5. Parsuj
    cards = parse_parking_email(body)
    if not cards:
        print("❌ Nie udało się sparsować danych")
        return

    alerts = [c for c in cards if c["alert"]]
    print(f"Sparsowano {len(cards)} kart, {len(alerts)} wymaga doładowania")

    if alerts:
        print("\n🚨 Karty do doładowania:")
        for c in alerts:
            print(f"  ...{c['bilet'][-6:]} — {c['dni']}d {c['h']}h ({c['typ']})")

    # 6. Najpierw zarezerwuj ID maila (zapobiega duplikatom przy równoległych uruchomieniach)
    save_msg_id(service, msg_id)
    print(f"Zarezerwowano ID maila: {msg_id}")

    # 7. Zapisz dane do Sheets
    mail_date = datetime.now().strftime("%Y-%m-%d")
    write_to_sheets(cards, mail_date, len(alerts), service)

if __name__ == "__main__":
    main()

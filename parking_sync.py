#!/usr/bin/env python3
"""
parking_sync.py
---------------
Pobiera ostatni mail "Stany kart" z Gmaila,
parsuje listę kart parkingowych i dopisuje do historii w Google Sheets.

Logika deduplikacji:
  - Kolumna J przechowuje ID maila dla każdego zestawu (tylko przy pierwszym wierszu)
  - Przed zapisem sprawdza czy to ID już istnieje — jeśli tak, pomija
  - Przy starcie automatycznie czyści duplikaty z poprzednich błędnych uruchomień

Wymagane secrets w GitHub:
  GMAIL_CLIENT_ID, GMAIL_CLIENT_SECRET, GMAIL_REFRESH_TOKEN
  PARKING_SPREADSHEET_ID
  GOOGLE_SERVICE_ACCOUNT
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
PARKING_SHEET_NAME     = "Parking"
PARKING_SPREADSHEET_ID = os.environ["PARKING_SPREADSHEET_ID"]
THRESHOLD_DAYS         = 6

GMAIL_CLIENT_ID     = os.environ["GMAIL_CLIENT_ID"]
GMAIL_CLIENT_SECRET = os.environ["GMAIL_CLIENT_SECRET"]
GMAIL_REFRESH_TOKEN = os.environ["GMAIL_REFRESH_TOKEN"]
GS_SA_JSON          = os.environ["GOOGLE_SERVICE_ACCOUNT"]

HEADER_ROW = ["Data maila", "L.P.", "Nr biletu", "Wazny do",
              "Pozostalo", "Dni", "H", "Typ", "Alert", "Mail ID"]

# ── GMAIL AUTH ───────────────────────────────────────────
def get_gmail_token():
    missing = [
        name for name, value in (
            ("GMAIL_CLIENT_ID", GMAIL_CLIENT_ID),
            ("GMAIL_CLIENT_SECRET", GMAIL_CLIENT_SECRET),
            ("GMAIL_REFRESH_TOKEN", GMAIL_REFRESH_TOKEN),
        )
        if not str(value or "").strip()
    ]
    if missing:
        raise RuntimeError(f"Brakuje sekretow Gmail OAuth: {', '.join(missing)}")

    resp = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id":     GMAIL_CLIENT_ID,
            "client_secret": GMAIL_CLIENT_SECRET,
            "refresh_token": GMAIL_REFRESH_TOKEN,
            "grant_type":    "refresh_token",
        },
        timeout=30,
    )
    if not resp.ok:
        try:
            err = resp.json()
        except Exception:
            err = {"raw": resp.text[:500]}
        print("Gmail token error:", json.dumps(err, ensure_ascii=False))
        hint = {
            "invalid_grant": "Refresh token jest niewazny/cofniety albo wygasl. Wygeneruj nowy GMAIL_REFRESH_TOKEN.",
            "invalid_client": "GMAIL_CLIENT_ID lub GMAIL_CLIENT_SECRET nie pasuje do refresh tokena.",
            "invalid_request": "Brakuje parametru OAuth albo jeden z sekretow jest pusty.",
        }.get(err.get("error"))
        if hint:
            print("Wskazowka:", hint)
        resp.raise_for_status()
    return resp.json()["access_token"]

def gmail_search(token, query, max_results=1):
    resp = requests.get(
        "https://gmail.googleapis.com/gmail/v1/users/me/messages",
        headers={"Authorization": f"Bearer {token}"},
        params={"q": query, "maxResults": max_results},
    )
    resp.raise_for_status()
    return resp.json().get("messages", [])

def gmail_get_body(token, msg_id):
    resp = requests.get(
        f"https://gmail.googleapis.com/gmail/v1/users/me/messages/{msg_id}",
        headers={"Authorization": f"Bearer {token}"},
        params={"format": "full"},
    )
    resp.raise_for_status()

    def extract(payload):
        if payload.get("mimeType") == "text/plain":
            data = payload.get("body", {}).get("data", "")
            return base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")
        for part in payload.get("parts", []):
            r = extract(part)
            if r:
                return r
        return ""

    return extract(resp.json().get("payload", {}))

# ── PARSER ───────────────────────────────────────────────
def parse_email(text):
    results = []
    lp = 1
    for line in text.split("\n"):
        raw = line.replace(">", "").strip()
        if not raw:
            continue

        bilet_m = re.search(r"\b(\d{10,15})\b", raw)
        if not bilet_m:
            continue
        bilet = bilet_m.group(1)

        data_m = re.search(r"(\d{1,2})\.(\d{2})\.(\d{4})", raw)
        if not data_m:
            continue
        data_str = f"{data_m.group(1).zfill(2)}.{data_m.group(2)}.{data_m.group(3)}"

        if "miesiecz" in raw.lower() or "miesięcz" in raw.lower():
            results.append({
                "lp": lp, "bilet": bilet, "data_waznosci": data_str,
                "dni": 999, "h": 0, "pozostalo": "miesięczny",
                "typ": "miesięczny", "alert": False,
            })
            lp += 1
            continue

        parts = re.split(r"[\t\s]+", raw)
        parts = [p for p in parts if p]
        dni = h = 0
        found = False

        for i, part in enumerate(parts):
            if part == "x24h":
                dni = int(parts[i - 1]) if i > 0 and parts[i - 1].isdigit() else 0
                pi = next((j for j in range(i, len(parts)) if parts[j] == "+"), -1)
                if pi != -1 and pi + 1 < len(parts) and parts[pi + 1].isdigit():
                    h = int(parts[pi + 1])
                found = True
                break
            m2 = re.match(r"^(\d+)x24h\+(\d+)h$", part)
            if m2:
                dni, h = int(m2.group(1)), int(m2.group(2))
                found = True
                break

        if not found:
            continue

        typ = "zapas" if "zapas" in raw else "normalny"
        results.append({
            "lp": lp, "bilet": bilet, "data_waznosci": data_str,
            "dni": dni, "h": h, "pozostalo": f"{dni} dni {h}h",
            "typ": typ, "alert": dni < THRESHOLD_DAYS,
        })
        lp += 1

    return results

# ── GOOGLE SHEETS ────────────────────────────────────────
def get_service():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(GS_SA_JSON),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds).spreadsheets()

def ensure_sheet(service):
    meta = service.get(
        spreadsheetId=PARKING_SPREADSHEET_ID,
        fields="sheets.properties.title"
    ).execute()
    sheets = [s["properties"]["title"] for s in meta.get("sheets", [])]

    if PARKING_SHEET_NAME not in sheets:
        service.batchUpdate(
            spreadsheetId=PARKING_SPREADSHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": PARKING_SHEET_NAME}}}]}
        ).execute()
        service.values().update(
            spreadsheetId=PARKING_SPREADSHEET_ID,
            range=f"'{PARKING_SHEET_NAME}'!A1",
            valueInputOption="RAW",
            body={"values": [HEADER_ROW]},
        ).execute()
        print(f"Utworzono zakladke '{PARKING_SHEET_NAME}'")

def get_all_rows(service):
    result = service.values().get(
        spreadsheetId=PARKING_SPREADSHEET_ID,
        range=f"'{PARKING_SHEET_NAME}'!A:J"
    ).execute()
    return result.get("values", [])

def clean_duplicates(service):
    """Usun zduplikowane zestawy — zostaw tylko jeden zestaw na Mail ID."""
    all_rows = get_all_rows(service)
    if len(all_rows) <= 1:
        return

    header = all_rows[0]
    data_rows = all_rows[1:]

    # Znajdz unikalne zestawy po Mail ID
    seen_ids = set()
    clean = [header]
    current_id = None
    skip_current = False

    for row in data_rows:
        mail_id = str(row[9]).strip() if len(row) > 9 else ""

        if mail_id:
            # Nowy zestaw
            if mail_id in seen_ids:
                skip_current = True
            else:
                seen_ids.add(mail_id)
                current_id = mail_id
                skip_current = False

        if not skip_current:
            clean.append(row)

    removed = len(all_rows) - len(clean)
    if removed > 0:
        print(f"Usuwam {removed} zduplikowanych wierszy...")
        service.values().clear(
            spreadsheetId=PARKING_SPREADSHEET_ID,
            range=f"'{PARKING_SHEET_NAME}'!A1:J5000"
        ).execute()
        service.values().update(
            spreadsheetId=PARKING_SPREADSHEET_ID,
            range=f"'{PARKING_SHEET_NAME}'!A1",
            valueInputOption="RAW",
            body={"values": clean},
        ).execute()
        print(f"Gotowe — {len(clean)-1} unikalnych wierszy")
    else:
        print("Brak duplikatow w arkuszu")

def get_processed_ids(service):
    all_rows = get_all_rows(service)
    ids = set()
    for row in all_rows[1:]:
        if len(row) > 9 and str(row[9]).strip():
            ids.add(str(row[9]).strip())
    return ids

def append_cards(service, cards, mail_date, msg_id):
    existing = service.values().get(
        spreadsheetId=PARKING_SPREADSHEET_ID,
        range=f"'{PARKING_SHEET_NAME}'!A:A"
    ).execute()
    next_row = len(existing.get("values", [])) + 1

    rows = []
    for i, c in enumerate(cards):
        rows.append([
            mail_date,
            c["lp"],
            c["bilet"],
            c["data_waznosci"],
            c["pozostalo"],
            c["dni"] if c["dni"] != 999 else "",
            c["h"] if c["dni"] != 999 else "",
            c["typ"],
            "TAK" if c["alert"] else "NIE",
            msg_id if i == 0 else "",
        ])

    service.values().update(
        spreadsheetId=PARKING_SPREADSHEET_ID,
        range=f"'{PARKING_SHEET_NAME}'!A{next_row}",
        valueInputOption="RAW",
        body={"values": rows},
    ).execute()
    print(f"Dopisano {len(cards)} kart (wiersze {next_row}-{next_row+len(cards)-1})")

# ── MAIN ─────────────────────────────────────────────────
def main():
    print("=== Parking Sync ===")
    print(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

    # 1. Arkusz
    service = get_service()
    ensure_sheet(service)
    clean_duplicates(service)

    # 2. Gmail
    print("Pobieranie tokenu Gmail...")
    token = get_gmail_token()

    print("Szukanie maila 'stany kart'...")
    messages = gmail_search(token, "subject:stany kart", max_results=1)
    if not messages:
        print("Nie znaleziono maila 'stany kart'")
        return

    msg_id = str(messages[0].get("id") or messages[0].get("messageId", ""))
    print(f"Znaleziono mail: {msg_id}")

    # 3. Deduplikacja
    processed = get_processed_ids(service)
    if msg_id in processed:
        print(f"Mail {msg_id} juz w arkuszu — koniec.")
        return

    # 4. Parsuj
    body = gmail_get_body(token, msg_id)
    if not body:
        print("Pusta tresc maila")
        return

    cards = parse_email(body)
    if not cards:
        print("Nie udalo sie sparsowac danych")
        return

    alerts = [c for c in cards if c["alert"]]
    print(f"Sparsowano {len(cards)} kart, {len(alerts)} wymaga doladowania")
    if alerts:
        print("Karty do doladowania:")
        for c in alerts:
            print(f"  ...{c['bilet'][-6:]} — {c['pozostalo']} ({c['typ']})")

    # 5. Zapisz
    mail_date = datetime.now().strftime("%Y-%m-%d")
    append_cards(service, cards, mail_date, msg_id)
    print(f"Gotowe! Mail ID: {msg_id}")

if __name__ == "__main__":
    main()

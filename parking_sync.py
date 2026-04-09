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
    return resp.json().get("messages", [])

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
def get_sheets_token():
    """Uzyskaj token do zapisu przez Service Account."""
    if not GS_SA_JSON_B64:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT nie ustawiony")

    sa_json = json.loads(base64.b64decode(GS_SA_JSON_B64))

    import time
    import hmac
    import hashlib

    # JWT dla Google OAuth2
    header = base64.urlsafe_b64encode(json.dumps({"alg": "RS256", "typ": "JWT"}).encode()).rstrip(b"=")
    now = int(time.time())
    claim = base64.urlsafe_b64encode(json.dumps({
        "iss": sa_json["client_email"],
        "scope": "https://www.googleapis.com/auth/spreadsheets",
        "aud": "https://oauth2.googleapis.com/token",
        "iat": now,
        "exp": now + 3600,
    }).encode()).rstrip(b"=")

    from cryptography.hazmat.primitives import serialization, hashes
    from cryptography.hazmat.primitives.asymmetric import padding

    private_key = serialization.load_pem_private_key(
        sa_json["private_key"].encode(), password=None
    )
    signature = private_key.sign(
        header + b"." + claim,
        padding.PKCS1v15(),
        hashes.SHA256(),
    )
    sig_b64 = base64.urlsafe_b64encode(signature).rstrip(b"=")
    jwt_token = header + b"." + claim + b"." + sig_b64

    resp = requests.post("https://oauth2.googleapis.com/token", data={
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": jwt_token.decode(),
    })
    resp.raise_for_status()
    return resp.json()["access_token"]

def write_to_sheets(cards, mail_date, alert_count):
    """Zapisz dane kart do arkusza Google Sheets."""
    token = get_sheets_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    base_url = f"https://sheets.googleapis.com/v4/spreadsheets/{PARKING_SPREADSHEET_ID}"

    # Sprawdź czy zakładka Parking istnieje, utwórz jeśli nie
    meta = requests.get(base_url, headers=headers, params={"fields": "sheets.properties.title"})
    meta.raise_for_status()
    sheets = [s["properties"]["title"] for s in meta.json().get("sheets", [])]

    if PARKING_SHEET_NAME not in sheets:
        requests.post(f"{base_url}:batchUpdate", headers=headers, json={
            "requests": [{"addSheet": {"properties": {"title": PARKING_SHEET_NAME}}}]
        }).raise_for_status()
        print(f"Utworzono zakładkę '{PARKING_SHEET_NAME}'")

    # Nagłówki
    header_row = ["Data maila", "L.P.", "Nr biletu", "Ważny do", "Godziny", "Dni", "H", "Typ", "Alert", "Aktualizacja"]
    
    # Dane
    rows = [header_row]
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
            datetime.now().strftime("%Y-%m-%d %H:%M"),
        ])
    
    # Wyczyść zakładkę i wpisz nowe dane
    enc_range = requests.utils.quote(f"'{PARKING_SHEET_NAME}'!A1:J500")
    requests.delete(
        f"https://sheets.googleapis.com/v4/spreadsheets/{PARKING_SPREADSHEET_ID}/values/{enc_range}",
        headers=headers,
    )

    resp = requests.put(
        f"https://sheets.googleapis.com/v4/spreadsheets/{PARKING_SPREADSHEET_ID}/values/{enc_range}",
        headers=headers,
        params={"valueInputOption": "RAW"},
        json={"range": f"'{PARKING_SHEET_NAME}'!A1", "values": rows},
    )
    resp.raise_for_status()
    print(f"✅ Zapisano {len(cards)} kart do arkusza '{PARKING_SHEET_NAME}'")
    print(f"🚨 Kart do doładowania: {alert_count}")

# ── MAIN ─────────────────────────────────────────────────
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

    # 3. Pobierz treść
    body = gmail_get_body(access_token, msg_id)
    if not body:
        print("❌ Pusta treść maila")
        return

    # 4. Parsuj
    cards = parse_parking_email(body)
    if not cards:
        print("❌ Nie udało się sparsować danych")
        return

    alerts = [c for c in cards if c["alert"]]
    print(f"Sparsowano {len(cards)} kart, {len(alerts)} wymaga doładowania")

    # Wydrukuj podsumowanie
    if alerts:
        print("\n🚨 Karty do doładowania:")
        for c in alerts:
            print(f"  ...{c['bilet'][-6:]} — {c['dni']}d {c['h']}h ({c['typ']})")

    # 5. Zapisz do Sheets
    mail_date = datetime.now().strftime("%Y-%m-%d")
    write_to_sheets(cards, mail_date, len(alerts))

if __name__ == "__main__":
    main()

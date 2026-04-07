#!/usr/bin/env python3
"""
Hot Terms Krakow — daily event search via Anthropic API
Saves results to Google Sheets tab 'HotTerminy'
"""
import os
import json
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from google.oauth2 import service_account
from googleapiclient.discovery import build

ANTHROPIC_API_KEY    = os.environ["ANTHROPIC_API_KEY"]
GOOGLE_SHEET_ID      = os.environ["GOOGLE_SHEET_ID"]
SERVICE_ACCOUNT_JSON = os.environ["GOOGLE_SERVICE_ACCOUNT"]
SHEET_NAME           = "HotTerminy"
WARSAW               = ZoneInfo("Europe/Warsaw")
TODAY                = datetime.now(WARSAW).strftime("%Y-%m-%d")
DATE_TO              = (datetime.now(WARSAW) + timedelta(days=90)).strftime("%Y-%m-%d")

# ── Ask Claude with web search ──────────────────────────
def fetch_events():
    print(f"Asking Claude to find Krakow events {TODAY} – {DATE_TO}...")
    resp = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "anthropic-beta": "interleaved-thinking-2025-05-14",
            "content-type": "application/json",
        },
        json={
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 2000,
            "tools": [{"type": "web_search_20250305", "name": "web_search"}],
            "system": """Jesteś ekspertem od rynku noclegowego w Krakowie. Twoje zadanie to znaleźć WSZYSTKIE ważne wydarzenia które generują duże zapotrzebowanie na noclegi.

Zwróć TYLKO poprawny JSON bez żadnego innego tekstu, bez markdown, bez komentarzy.
Format odpowiedzi:
{"updated":"YYYY-MM-DD","events":[{"name":"string","date_from":"YYYY-MM-DD","date_to":"YYYY-MM-DD","category":"koncert|sport|maraton|konferencja|festiwal|targi|inne","impact":"wysoki|sredni|niski","venue":"string","description":"string max 100 znaków","expected_visitors":"string lub null"}]}

Zasady:
- impact=wysoki: ponad 5000 uczestników lub gwiazda światowej sławy lub znany maraton
- impact=sredni: 1000-5000 uczestników lub znany artysta krajowy
- impact=niski: 500-1000 uczestników
- Zawsze szukaj konkretnych dat w internecie przed odpowiedzią
- Sortuj chronologicznie
- Jeśli nie znasz dokładnej daty zakończenia użyj tej samej co rozpoczęcia""",
            "messages": [{
                "role": "user",
                "content": f"""Znajdź hot terminy w Krakowie które generują duże zapotrzebowanie na noclegi. Dzisiaj: {TODAY}. Szukaj wydarzeń do {DATE_TO}.

Sprawdź KONIECZNIE te kategorie i wyszukaj konkretne daty:
1. MARATONY I BIEGI - Kraków Maraton, Wings for Life, PKO Maraton, biegi uliczne
2. KONCERTY - Tauron Arena Kraków, ICE Kraków, Klub Studio, koncerty plenerowe (Eric Clapton, inne gwiazdy)
3. SPORT - mecze Wisły Kraków, Cracovii, zawody sportowe, turnieje
4. FESTIWALE - Wianki, Kraków Live Festival, Off Festival, Unsound, Sacrum Profanum
5. TARGI I KONFERENCJE - targi w EXPO Kraków, Centrum Targowe
6. ŚWIĘTA I IMPREZY MIEJSKIE - Lajkonik, Juwenalia, Sylwester miejski
7. WYDARZENIA KULTURALNE - Festiwal Filmowy, Opera, Filharmonia (premiery)

Szukaj w internecie konkretnych dat dla każdej kategorii. Zwróć tylko JSON bez żadnego tekstu przed lub po."""
            }]
        },
        timeout=120
    )
    resp.raise_for_status()
    data = resp.json()

    # Extract text block
    text = ""
    for block in data.get("content", []):
        if block.get("type") == "text":
            text = block["text"].strip()
            break

    # Clean markdown
    text = text.replace("```json", "").replace("```", "").strip()

    parsed = json.loads(text)
    print(f"Found {len(parsed.get('events', []))} events")
    return parsed

# ── Google Sheets ───────────────────────────────────────
def get_service():
    creds = service_account.Credentials.from_service_account_info(
        json.loads(SERVICE_ACCOUNT_JSON),
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds).spreadsheets()

def ensure_sheet(service):
    meta = service.get(spreadsheetId=GOOGLE_SHEET_ID).execute()
    sheets = [s["properties"]["title"] for s in meta["sheets"]]
    if SHEET_NAME not in sheets:
        service.batchUpdate(
            spreadsheetId=GOOGLE_SHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": SHEET_NAME}}}]}
        ).execute()
        print(f"Created sheet: {SHEET_NAME}")

def write_events(service, data):
    headers = ["Nazwa", "Data od", "Data do", "Kategoria", "Impact", "Miejsce", "Opis", "Goście", "Zaktualizowano"]
    rows = []
    for e in data.get("events", []):
        rows.append([
            e.get("name", ""),
            e.get("date_from", ""),
            e.get("date_to", ""),
            e.get("category", ""),
            e.get("impact", ""),
            e.get("venue", ""),
            e.get("description", ""),
            e.get("expected_visitors", "") or "",
            data.get("updated", TODAY),
        ])

    service.values().clear(
        spreadsheetId=GOOGLE_SHEET_ID,
        range=f"{SHEET_NAME}!A:I"
    ).execute()

    service.values().update(
        spreadsheetId=GOOGLE_SHEET_ID,
        range=f"{SHEET_NAME}!A1",
        valueInputOption="RAW",
        body={"values": [headers] + rows}
    ).execute()

    print(f"Written {len(rows)} events to {SHEET_NAME}")

def main():
    data = fetch_events()
    service = get_service()
    ensure_sheet(service)
    write_events(service, data)
    print("Done!")

if __name__ == "__main__":
    main()

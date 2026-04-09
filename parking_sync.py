#!/usr/bin/env python3
"""
get_refresh_token.py
--------------------
Uruchom JEDNORAZOWO lokalnie żeby uzyskać refresh token do Gmaila.
Wynik (refresh_token) wklej do GitHub Secrets.

Użycie:
  pip install requests
  python get_refresh_token.py

Będziesz musiał otworzyć link w przeglądarce i zatwierdzić dostęp.
"""

import requests
import json
import webbrowser
from urllib.parse import urlencode, urlparse, parse_qs

# ── WKLEJ TUTAJ SWOJE DANE Z GOOGLE CLOUD ──
CLIENT_ID     = "WKLEJ_SWOJ_CLIENT_ID"
CLIENT_SECRET = "WKLEJ_SWOJ_CLIENT_SECRET"
# ───────────────────────────────────────────

REDIRECT_URI = "urn:ietf:wg:oauth:2.0:oob"  # tryb desktop — kod pojawi się w przeglądarce
SCOPE = "https://www.googleapis.com/auth/gmail.readonly"

def main():
    # Krok 1 — otwórz URL autoryzacji
    auth_url = "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode({
        "client_id":     CLIENT_ID,
        "redirect_uri":  REDIRECT_URI,
        "response_type": "code",
        "scope":         SCOPE,
        "access_type":   "offline",
        "prompt":        "consent",  # wymusza zwrócenie refresh_token
    })

    print("=" * 60)
    print("Otwórz ten link w przeglądarce:")
    print()
    print(auth_url)
    print()
    print("=" * 60)
    print("Po zalogowaniu i zatwierdzeniu dostępu Google pokaże Ci")
    print("kod autoryzacji. Skopiuj go i wklej poniżej.")
    print()

    # Spróbuj otworzyć automatycznie
    try:
        webbrowser.open(auth_url)
    except:
        pass

    code = input("Wklej kod autoryzacji: ").strip()

    # Krok 2 — wymień kod na tokeny
    resp = requests.post("https://oauth2.googleapis.com/token", data={
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code":          code,
        "redirect_uri":  REDIRECT_URI,
        "grant_type":    "authorization_code",
    })

    if not resp.ok:
        print(f"\n❌ Błąd: {resp.text}")
        return

    tokens = resp.json()

    print()
    print("=" * 60)
    print("✅ SUKCES! Oto Twoje dane do GitHub Secrets:")
    print("=" * 60)
    print()
    print(f"GMAIL_CLIENT_ID:      {CLIENT_ID}")
    print(f"GMAIL_CLIENT_SECRET:  {CLIENT_SECRET}")
    print(f"GMAIL_REFRESH_TOKEN:  {tokens.get('refresh_token', '❌ brak — uruchom ponownie')}")
    print()

    if "refresh_token" not in tokens:
        print("⚠️  Brak refresh_token — upewnij się że parametr prompt=consent jest w URL")
        print("    i że konto jest dodane jako użytkownik testowy w Google Cloud.")
    else:
        print("Wklej powyższe wartości do GitHub Secrets:")
        print("  repo previo-sync → Settings → Secrets and variables → Actions")
        print()
        # Zapisz do pliku dla pewności
        with open("tokens.json", "w") as f:
            json.dump({
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
                "refresh_token": tokens["refresh_token"],
            }, f, indent=2)
        print("Zapisano też do pliku tokens.json (nie wgrywaj go na GitHub!)")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
ksef_sync.py
------------
Pobiera faktury zakupowe z KSeF API i zapisuje do Google Sheets (zakładka 'KSeF').
Uruchamiany przez GitHub Actions raz dziennie.

Wymagane secrets w GitHub (repo previo-sync):
  KSEF_TOKEN              — token wygenerowany w Aplikacji Podatnika KSeF 2.0
  KSEF_SPREADSHEET_ID     — ID arkusza Google Sheets (może być ten sam co Previo)
  GS_SA_JSON_B64          — Service Account JSON (base64) z uprawnieniami do zapisu

Zakładka 'KSeF' w arkuszu będzie zawierać kolumny:
  NumerKSeF | DataWystawienia | Sprzedawca | NIP Sprzedawcy | Netto | VAT | Brutto | TerminPlatnosci | DniDoPlatnosci | Alert
"""

import os
import re
import json
import base64
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, date, timedelta

# ── CONFIG ──────────────────────────────────────────────────────────────────
NIP                  = "6793324449"
KSEF_API_BASE        = "https://api.ksef.mf.gov.pl/api/v2"    # produkcja KSeF 2.0
# KSEF_API_BASE      = "https://api-test.ksef.mf.gov.pl/api/v2"  # test
SHEET_NAME           = "KSeF"
SPREADSHEET_ID       = os.environ["KSEF_SPREADSHEET_ID"]
KSEF_TOKEN           = os.environ["KSEF_TOKEN"]
GS_SA_JSON_B64       = os.environ.get("GS_SA_JSON_B64", "")
ALERT_DAYS           = 7   # alert jeśli termin płatności za mniej niż 7 dni

# ── KSEF AUTH ────────────────────────────────────────────────────────────────
def ksef_get_access_token():
    """
    Autoryzacja KSeF 2.0 przez ksef-client SDK.
    pip install ksef-client
    """
    try:
        from ksef_client import KsefClient, KsefClientOptions, KsefEnvironment, models as m
        from ksef_client.services import AuthCoordinator
    except ImportError:
        raise ImportError("Zainstaluj: pip install ksef-client")

    options = KsefClientOptions(base_url=KsefEnvironment.PROD.value)
    with KsefClient(options) as client:
        # Pobierz certyfikat do szyfrowania tokenu
        token_cert_pem = client.security.get_public_key_certificate_pem(
            m.PublicKeyCertificateUsage.KSEFTOKENENCRYPTION,
        )
        auth = AuthCoordinator(client.auth).authenticate_with_ksef_token(
            token=KSEF_TOKEN,
            public_certificate=token_cert_pem,
            context_identifier_type="nip",
            context_identifier_value=NIP,
        )
        access_token = auth.access_token
        print("AccessToken uzyskany przez ksef-client SDK.")
        return access_token


def ksef_terminate_session(access_token):
    """Wyloguj z KSeF."""
    try:
        requests.delete(
            f"{KSEF_API_BASE}/auth/session",
            headers={"Authorization": f"Bearer {access_token}"}
        )
    except Exception:
        pass


# ── POBIERANIE FAKTUR ────────────────────────────────────────────────────────
def ksef_query_invoices(access_token, date_from=None, date_to=None):
    """
    Pobiera listę faktur zakupowych (jako nabywca) z KSeF 2.0.
    Endpoint: POST /api/v2/invoices/query/metadata
    """
    if not date_from:
        date_from = (date.today() - timedelta(days=60)).strftime("%Y-%m-%dT00:00:00.000Z")
    else:
        date_from = date_from + "T00:00:00.000Z"

    if not date_to:
        date_to = date.today().strftime("%Y-%m-%dT23:59:59.000Z")
    else:
        date_to = date_to + "T23:59:59.000Z"

    hdrs = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    all_invoices = []
    page_offset = 0
    page_size = 100

    while True:
        payload = {
            "dateFrom": date_from,
            "dateTo": date_to,
            "subjectType": "BUYER",
            "pageOffset": page_offset,
            "pageSize": page_size,
        }

        r = requests.post(
            f"{KSEF_API_BASE}/invoices/query/metadata",
            headers=hdrs, json=payload
        )
        if r.status_code == 404:
            break
        r.raise_for_status()

        data = r.json()
        invoices = data.get("invoices", [])
        all_invoices.extend(invoices)

        if len(invoices) < page_size:
            break
        page_offset += page_size

    print(f"Znaleziono {len(all_invoices)} faktur zakupowych")
    return all_invoices


def ksef_get_invoice_xml(access_token, ksef_number):
    """Pobiera XML faktury po numerze KSeF 2.0."""
    r = requests.get(
        f"{KSEF_API_BASE}/invoices/ksef/{ksef_number}",
        headers={"Authorization": f"Bearer {access_token}"}
    )
    r.raise_for_status()
    return r.content


def parse_invoice_xml(xml_bytes):
    """
    Parsuje XML FA(2)/FA(3) i wyciąga kluczowe pola.
    Zwraca słownik z danymi faktury.
    """
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return {}

    ns = {
        'fa': 'http://crd.gov.pl/wzor/2023/06/29/9781/',
        'fa2': 'http://crd.gov.pl/wzor/2021/08/06/11089/',
    }

    def find_text(xpath_list):
        for xp in xpath_list:
            for prefix, uri in ns.items():
                try:
                    el = root.find(xp.replace('{ns}', f'{{{uri}}}'))
                    if el is not None and el.text:
                        return el.text.strip()
                except Exception:
                    pass
        return ""

    # Sprzedawca
    sprzedawca_nip = find_text([
        './/{ns}Podmiot1/{ns}DaneIdentyfikacyjne/{ns}NIP',
        './/{ns}P1/{ns}NIP',
    ])
    sprzedawca_nazwa = find_text([
        './/{ns}Podmiot1/{ns}DaneIdentyfikacyjne/{ns}PelnaNazwa',
        './/{ns}Podmiot1/{ns}DaneIdentyfikacyjne/{ns}Nazwa',
    ])

    # Daty
    data_wystawienia = find_text([
        './/{ns}Fa/{ns}P1',
        './/{ns}P1',
    ])

    # Termin płatności — może być wiele, bierzemy pierwszy
    termin = find_text([
        './/{ns}Platnosc/{ns}TerminPlatnosci',
        './/{ns}P22',
    ])

    # Kwoty
    netto = find_text(['.//{ns}P15'])
    vat   = find_text(['.//{ns}P16'])
    brutto_candidates = [
        './/{ns}Fa/{ns}P15',
        './/{ns}P8A',
        './/{ns}WartoscFaktury',
    ]
    brutto = find_text(brutto_candidates)

    return {
        "data_wystawienia": data_wystawienia,
        "sprzedawca_nip": sprzedawca_nip,
        "sprzedawca_nazwa": sprzedawca_nazwa,
        "termin_platnosci": termin,
        "netto": netto,
        "vat": vat,
        "brutto": brutto,
    }


# ── GOOGLE SHEETS ────────────────────────────────────────────────────────────
def get_sheets_token():
    if not GS_SA_JSON_B64:
        raise ValueError("GS_SA_JSON_B64 nie ustawiony")
    sa_json = json.loads(base64.b64decode(GS_SA_JSON_B64))

    import time
    from cryptography.hazmat.primitives import serialization, hashes
    from cryptography.hazmat.primitives.asymmetric import padding

    header = base64.urlsafe_b64encode(
        json.dumps({"alg": "RS256", "typ": "JWT"}).encode()
    ).rstrip(b"=")
    now = int(time.time())
    claim = base64.urlsafe_b64encode(json.dumps({
        "iss": sa_json["client_email"],
        "scope": "https://www.googleapis.com/auth/spreadsheets",
        "aud": "https://oauth2.googleapis.com/token",
        "iat": now,
        "exp": now + 3600,
    }).encode()).rstrip(b"=")

    private_key = serialization.load_pem_private_key(
        sa_json["private_key"].encode(), password=None
    )
    sig = private_key.sign(header + b"." + claim, padding.PKCS1v15(), hashes.SHA256())
    sig_b64 = base64.urlsafe_b64encode(sig).rstrip(b"=")
    jwt = header + b"." + claim + b"." + sig_b64

    resp = requests.post("https://oauth2.googleapis.com/token", data={
        "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
        "assertion": jwt.decode(),
    })
    resp.raise_for_status()
    return resp.json()["access_token"]


def write_to_sheets(rows_data):
    token = get_sheets_token()
    hdrs = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    base_url = f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}"

    # Utwórz zakładkę jeśli nie istnieje
    meta = requests.get(base_url, headers=hdrs, params={"fields": "sheets.properties.title"})
    meta.raise_for_status()
    sheets = [s["properties"]["title"] for s in meta.json().get("sheets", [])]
    if SHEET_NAME not in sheets:
        requests.post(f"{base_url}:batchUpdate", headers=hdrs, json={
            "requests": [{"addSheet": {"properties": {"title": SHEET_NAME}}}]
        }).raise_for_status()
        print(f"Utworzono zakładkę '{SHEET_NAME}'")

    header_row = [
        "Nr KSeF", "Data wystawienia", "Sprzedawca", "NIP sprzedawcy",
        "Netto", "VAT", "Brutto", "Termin płatności", "Dni do płatności", "Alert", "Aktualizacja"
    ]

    rows = [header_row] + rows_data

    enc_range = requests.utils.quote(f"'{SHEET_NAME}'!A1:K2000")
    requests.delete(
        f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}/values/{enc_range}",
        headers=hdrs
    )
    resp = requests.put(
        f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}/values/{enc_range}",
        headers=hdrs,
        params={"valueInputOption": "RAW"},
        json={"range": f"'{SHEET_NAME}'!A1", "values": rows}
    )
    resp.raise_for_status()
    print(f"✅ Zapisano {len(rows_data)} faktur do arkusza '{SHEET_NAME}'")


# ── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    print("=== KSeF Sync ===")
    print(f"Data: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"NIP: {NIP}")

    # Sesja KSeF
    print("Inicjowanie sesji KSeF...")
    access_token = ksef_get_access_token()
    print("Sesja aktywna.")

    try:
        # Pobierz faktury (ostatnie 60 dni żeby nie ominąć niczego)
        date_from = (date.today() - timedelta(days=60)).strftime("%Y-%m-%d")
        invoices = ksef_query_invoices(access_token, date_from=date_from)

        rows = []
        today = date.today()

        for inv in invoices:
            ksef_number = inv.get("ksefReferenceNumber") or inv.get("ksefNumber", "")
            inv_date    = (inv.get("invoiceDate") or inv.get("issueDate", ""))[:10] if inv.get("invoiceDate") else ""

            # Pobierz XML dla szczegółów (termin płatności, kwoty, sprzedawca)
            parsed = {}
            if ksef_number:
                try:
                    xml_bytes = ksef_get_invoice_xml(access_token, ksef_number)
                    parsed = parse_invoice_xml(xml_bytes)
                except Exception as e:
                    print(f"⚠️ Błąd pobierania XML dla {ksef_number}: {e}")

            sprzedawca   = parsed.get("sprzedawca_nazwa") or inv.get("subjectName", "")
            nip_sp       = parsed.get("sprzedawca_nip", "")
            netto        = parsed.get("netto", "")
            vat          = parsed.get("vat", "")
            brutto       = parsed.get("brutto", "") or inv.get("grossValue", "")
            termin_str   = parsed.get("termin_platnosci", "")

            # Oblicz dni do płatności
            dni_do = ""
            alert  = "NIE"
            if termin_str:
                try:
                    # Format może być YYYY-MM-DD lub YYYY-MM-DDThh:mm:ss
                    termin_date = date.fromisoformat(termin_str[:10])
                    dni_do = (termin_date - today).days
                    if dni_do <= ALERT_DAYS:
                        alert = "TAK"
                except Exception:
                    pass

            rows.append([
                ksef_number,
                inv_date,
                sprzedawca,
                nip_sp,
                netto,
                vat,
                brutto,
                termin_str,
                dni_do,
                alert,
                datetime.now().strftime("%Y-%m-%d %H:%M"),
            ])

        # Sortuj po terminie płatności
        rows.sort(key=lambda r: str(r[7]) if r[7] else "9999")

        alerts = [r for r in rows if r[9] == "TAK"]
        if alerts:
            print(f"\n🚨 Faktury do opłacenia w ciągu {ALERT_DAYS} dni: {len(alerts)}")
            for r in alerts:
                print(f"  {r[2]} — termin: {r[7]} (za {r[8]} dni), kwota: {r[6]}")

        write_to_sheets(rows)

    finally:
        ksef_terminate_session(access_token)
        print("Sesja KSeF zamknięta.")


if __name__ == "__main__":
    main()

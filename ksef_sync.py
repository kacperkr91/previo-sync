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
    Pobiera listę faktur zakupowych (jako nabywca) z KSeF 2.0 przez SDK.
    """
    from ksef_client import KsefClient, KsefClientOptions, KsefEnvironment
    from ksef_client.models import InvoiceQuerySubjectType, InvoiceQueryDateType

    if not date_from:
        date_from = (date.today() - timedelta(days=60)).strftime("%Y-%m-%dT00:00:00")
    if not date_to:
        date_to = date.today().strftime("%Y-%m-%dT23:59:59")

    all_invoices = []
    page_offset = 0
    page_size = 100

    with KsefClient(KsefClientOptions(base_url=KsefEnvironment.PROD.value)) as client:
        while True:
            resp = client.invoices.query_invoice_metadata_by_date_range(
                subject_type=InvoiceQuerySubjectType.SUBJECT2,  # jako nabywca
                date_type=InvoiceQueryDateType.ISSUE,
                date_from=date_from,
                date_to=date_to,
                access_token=access_token,
                page_offset=page_offset,
                page_size=page_size,
            )
            batch = resp.invoices or []
            all_invoices.extend(batch)
            if len(batch) < page_size or not getattr(resp, 'has_more', False):
                break
            page_offset += page_size

    print(f"Znaleziono {len(all_invoices)} faktur zakupowych")
    return all_invoices


def ksef_get_invoice_xml(access_token, ksef_number):
    """Pobiera XML faktury po numerze KSeF 2.0."""
    from ksef_client import KsefClient, KsefClientOptions, KsefEnvironment
    with KsefClient(KsefClientOptions(base_url=KsefEnvironment.PROD.value)) as client:
        result = client.invoices.get_invoice_bytes(
            ksef_number,
            access_token=access_token,
        )
        return result.content


def parse_invoice_xml(xml_bytes):
    """
    Parsuje XML FA(2)/FA(3) i wyciąga kluczowe pola.
    Obsługuje namespace FA(3) i FA(2).
    """
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return {}

    # Wykryj namespace
    ns_uri = root.tag.split('}')[0].strip('{') if '}' in root.tag else ""
    ns = {"fa": ns_uri} if ns_uri else {}
    prefix = f"{{{ns_uri}}}" if ns_uri else ""

    def find(path):
        """Szukaj przez XPath z namespace."""
        try:
            # Zamień fa: na {ns_uri} poprawnie
            real_path = path.replace("fa:", f"{{{ns_uri}}}" if ns_uri else "")
            el = root.find(real_path)
            if el is not None and el.text:
                return el.text.strip()
        except Exception:
            pass
        return ""

    # Sprzedawca (Podmiot1)
    sprzedawca_nip   = find(".//fa:Podmiot1/fa:DaneIdentyfikacyjne/fa:NIP")
    sprzedawca_nazwa = (find(".//fa:Podmiot1/fa:DaneIdentyfikacyjne/fa:PelnaNazwa") or
                        find(".//fa:Podmiot1/fa:DaneIdentyfikacyjne/fa:Nazwa"))

    # FA(3) używa P_1, P_15 itp. (z podkreślnikiem), FA(2) bez podkreślnika
    p1   = find(".//fa:Fa/fa:P_1")   or find(".//fa:Fa/fa:P1")
    p15  = find(".//fa:Fa/fa:P_15")  or find(".//fa:Fa/fa:P15")
    p16  = find(".//fa:Fa/fa:P_16")  or find(".//fa:Fa/fa:P16")
    p13  = find(".//fa:Fa/fa:P_13_1") or find(".//fa:Fa/fa:P13_1")

    # Termin płatności — szukamy w Platnosc/TerminPlatnosci/Termin
    # Ścieżka może być w <Fa><Platnosc> lub bezpośrednio w <Platnosc>
    termin = (find(".//fa:Fa/fa:Platnosc/fa:TerminPlatnosci/fa:Termin") or
              find(".//fa:Platnosc/fa:TerminPlatnosci/fa:Termin"))
    if not termin:
        termin = find(".//fa:Fa/fa:P_22") or find(".//fa:Fa/fa:P22")

    return {
        "data_wystawienia": p1,
        "sprzedawca_nip":   sprzedawca_nip,
        "sprzedawca_nazwa": sprzedawca_nazwa,
        "termin_platnosci": termin,
        "netto":  p13 or p15,
        "vat":    p16,
        "brutto": p15,
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

    # Wyczyść arkusz przez batchClear, potem zapisz
    requests.post(
        f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}/values:batchClear",
        headers=hdrs,
        json={"ranges": [f"{SHEET_NAME}!A1:K2000"]}
    )
    resp = requests.post(
        f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}/values:batchUpdate",
        headers=hdrs,
        json={
            "valueInputOption": "RAW",
            "data": [{"range": f"{SHEET_NAME}!A1:K2000", "values": rows}]
        }
    )
    if not resp.ok:
        print(f"Sheets error {resp.status_code}: {resp.text[:500]}")
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

        import time

        # Wczytaj istniejące dane z arkusza (cache terminów)
        existing = {}
        try:
            token_s = get_sheets_token()
            hdrs_s = {"Authorization": f"Bearer {token_s}"}
            url_range_read = requests.utils.quote(f"{SHEET_NAME}!A2:K2000")
            r_read = requests.get(
                f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}/values/{url_range_read}",
                headers=hdrs_s
            )
            if r_read.ok:
                for row in r_read.json().get("values", []):
                    if row and len(row) >= 8:
                        existing[row[0]] = row  # ksef_number -> cały wiersz
            print(f"Wczytano {len(existing)} istniejących wierszy z arkusza")
        except Exception as e:
            print(f"Nie udało się wczytać cache: {e}")

        rows = []
        today = date.today()

        for inv in invoices:
            # Obsługa SDK dataclass
            if hasattr(inv, 'ksef_number'):
                ksef_number     = inv.ksef_number or ""
                inv_date        = (inv.issue_date or "")[:10]
                brutto_meta     = str(inv.gross_amount or "")
                netto_meta      = str(inv.net_amount or "")
                seller_obj      = getattr(inv, 'seller', None)
                sprzedawca_meta = getattr(seller_obj, 'name', "") if seller_obj else ""
                nip_meta        = getattr(seller_obj, 'nip', "") if seller_obj else ""
            else:
                ksef_number     = inv.get("ksefReferenceNumber") or inv.get("ksefNumber", "")
                inv_date        = (inv.get("issueDate") or "")[:10]
                brutto_meta     = str(inv.get("grossValue", "") or "")
                netto_meta      = str(inv.get("netAmount", "") or "")
                sprzedawca_meta = inv.get("subjectName", "")
                nip_meta        = ""

            # Jeśli faktura już w arkuszu i ma termin — użyj cache, nie pobieraj XML
            if ksef_number in existing:
                cached = existing[ksef_number]
                termin_cached = cached[7] if len(cached) > 7 else ""
                if termin_cached:
                    # Mamy dane — użyj z cache
                    rows.append(cached[:11] + [datetime.now().strftime("%Y-%m-%d %H:%M")])
                    continue

            # Pobierz XML — tylko dla nowych faktur lub bez terminu
            parsed = {}
            if ksef_number:
                for attempt in range(3):
                    try:
                        time.sleep(0.5)
                        xml_bytes = ksef_get_invoice_xml(access_token, ksef_number)
                        parsed = parse_invoice_xml(xml_bytes)
                        break
                    except Exception as e:
                        if '429' in str(e) and attempt < 2:
                            print(f"  429 retry {attempt+1} dla {ksef_number[-8:]}, czekam 5s...")
                            time.sleep(5)
                        else:
                            print(f"⚠️ Pominięto XML {ksef_number[-8:]}: {e}")
                            break

            sprzedawca = parsed.get("sprzedawca_nazwa") or sprzedawca_meta
            nip_sp     = parsed.get("sprzedawca_nip", "") or nip_meta
            netto      = parsed.get("netto", "") or netto_meta
            vat        = parsed.get("vat", "")
            brutto     = parsed.get("brutto", "") or brutto_meta
            termin_str = parsed.get("termin_platnosci", "")

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

        if not rows:
            print("Brak faktur zakupowych — nic do zapisania.")
            return

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

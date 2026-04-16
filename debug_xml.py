#!/usr/bin/env python3
import os, time
import xml.etree.ElementTree as ET
from ksef_client import KsefClient, KsefClientOptions, KsefEnvironment
from ksef_client.services import AuthCoordinator
from ksef_client import models as m

KSEF_TOKEN = os.environ["KSEF_TOKEN"]
NIP = "6793324449"

NUMBERS = [
    "5272706082-20260309-45488400003B-EC",  # MA termin w arkuszu
    "5272706082-20260409-616774C0000C-2C",  # NIE MA terminu w arkuszu
]

options = KsefClientOptions(base_url=KsefEnvironment.PROD.value)
with KsefClient(options) as client:
    token_cert_pem = client.security.get_public_key_certificate_pem(
        m.PublicKeyCertificateUsage.KSEFTOKENENCRYPTION,
    )
    auth = AuthCoordinator(client.auth).authenticate_with_ksef_token(
        token=KSEF_TOKEN, public_certificate=token_cert_pem,
        context_identifier_type="nip", context_identifier_value=NIP,
    )
    access_token = auth.access_token
    print("Token OK\n")

    for num in NUMBERS:
        time.sleep(2)
        print(f"{'='*60}")
        print(f"FAKTURA: {num}")
        try:
            result = client.invoices.get_invoice_bytes(num, access_token=access_token)
            xml = result.content
            root = ET.fromstring(xml)
            ns_uri = root.tag.split('}')[0].strip('{') if '}' in root.tag else ''

            def find(path):
                real_path = path.replace("fa:", f"{{{ns_uri}}}" if ns_uri else "")
                el = root.find(real_path)
                return el.text.strip() if el is not None and el.text else ""

            print(f"P_1: {find('.//fa:Fa/fa:P_1')}")
            print(f"P_15: {find('.//fa:Fa/fa:P_15')}")
            print(f"NIP sprzedawcy: {find('.//fa:Podmiot1/fa:DaneIdentyfikacyjne/fa:NIP')}")
            print(f"Termin (Fa/Platnosc): {find('.//fa:Fa/fa:Platnosc/fa:TerminPlatnosci/fa:Termin')}")
            print(f"Termin (Platnosc): {find('.//fa:Platnosc/fa:TerminPlatnosci/fa:Termin')}")

            # Wypisz wszystkie tagi Platnosc i Termin
            print("Tagi płatności:")
            for el in root.iter():
                local = el.tag.split('}')[1] if '}' in el.tag else el.tag
                if local in ('Platnosc', 'TerminPlatnosci', 'Termin', 'DoZaplaty', 'FormaPlatnosci'):
                    # Pokaż też rodzica
                    print(f"  <{local}> = {el.text!r}")

            # Pokaż kontekst - gdzie jest Platnosc w drzewie
            print("Struktura Platnosc:")
            for parent in root.iter():
                p_local = parent.tag.split('}')[1] if '}' in parent.tag else parent.tag
                for child in parent:
                    c_local = child.tag.split('}')[1] if '}' in child.tag else child.tag
                    if c_local in ('Platnosc', 'TerminPlatnosci', 'Termin'):
                        print(f"  {p_local} -> {c_local} = {child.text!r}")

        except Exception as e:
            print(f"BŁĄD: {e}")
        print()

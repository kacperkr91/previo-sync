#!/usr/bin/env python3
"""Tymczasowy skrypt debug — wypisuje XML dwóch faktur."""
import os, time
import xml.etree.ElementTree as ET
from ksef_client import KsefClient, KsefClientOptions, KsefEnvironment
from ksef_client.services import AuthCoordinator
from ksef_client import models as m

KSEF_TOKEN = os.environ["KSEF_TOKEN"]
NIP = "6793324449"

# jedna z terminem, jedna bez
NUMBERS = [
    "6762337735-20260305-5EF63B4002D5-42",  # MA termin
    "6762337735-20260309-5BDA3100002D-3A",  # NIE MA terminu
]

options = KsefClientOptions(base_url=KsefEnvironment.PROD.value)
with KsefClient(options) as client:
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
    print("Token OK\n")

    for num in NUMBERS:
        time.sleep(1)
        print(f"{'='*60}")
        print(f"FAKTURA: {num}")
        try:
            result = client.invoices.get_invoice_bytes(num, access_token=access_token)
            xml = result.content
            # Wypisz cały XML (skrócony)
            print(xml.decode('utf-8')[:3000])
        except Exception as e:
            print(f"BŁĄD: {e}")
        print()

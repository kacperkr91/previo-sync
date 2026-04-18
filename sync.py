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

        note = t("note")
        channel = "Własna"
        note_lower = note.lower()
        if "airbnb" in note_lower:
            channel = "Airbnb"
        elif "booking" in note_lower:
            channel = "Booking.com"
        elif "expedia" in note_lower:
            channel = "Expedia"
        elif "profitroom" in note_lower:
            channel = "Profitroom"

        # Preserve full datetimes with hours for Sheets/dashboard use.
        date_from_full = t("term/from")[:16] if t("term/from") else ""
        date_to_full = t("term/to")[:16] if t("term/to") else ""
        date_from = date_from_full[:10]
        date_to = date_to_full[:10]

        nights = 0
        try:
            d1 = datetime.strptime(date_from, "%Y-%m-%d")
            d2 = datetime.strptime(date_to, "%Y-%m-%d")
            nights = (d2 - d1).days
        except Exception:
            pass

        company_name = t("company/name").strip()

        rows.append(
            [
                t("resId"),  # A: ID rezerwacji
                t("voucher"),  # B: Numer voucher
                t("created")[:10],  # C: Data rezerwacji
                date_from_full,  # D: Data od (z godziną)
                date_to_full,  # E: Data do (z godziną)
                nights,  # F: Liczba nocy
                t("object/name"),  # G: Apartament
                channel,  # H: Kanał
                t("price"),  # I: Cena
                t("currency/code"),  # J: Waluta
                t("status/statusId"),  # K: Status ID
                t("guest/name"),  # L: Gość
                t("guest/countryCode"),  # M: Kraj
                t("contactPerson/phone"),  # N: Telefon
                company_name,  # O: Firma
                datetime.now().strftime("%Y-%m-%d %H:%M"),  # P: Ostatnia aktualizacja
            ]
        )

    return rows


# ── GOOGLE SHEETS ────────────────────────────────────────
def get_sheets_service():
    creds_dict = json.loads(SERVICE_ACCOUNT_JSON)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return build("sheets", "v4", credentials=creds).spreadsheets()


def ensure_sheet_exists(service):
    """Create 'Previo' tab if it doesn't exist."""
    meta = service.get(spreadsheetId=SHEET_ID).execute()
    sheets = [s["properties"]["title"] for s in meta["sheets"]]

    if SHEET_NAME not in sheets:
        service.batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": SHEET_NAME}}}]},
        ).execute()
        print(f"Created sheet: {SHEET_NAME}")


def write_to_sheets(service, rows):
    headers = [
        "ID Rezerwacji",
        "Voucher",
        "Data rezerwacji",
        "Data od",
        "Data do",
        "Noce",
        "Apartament",
        "Kanał",
        "Cena",
        "Waluta",
        "Status",
        "Gość",
        "Kraj",
        "Telefon",
        "Firma",
        "Ostatnia aktualizacja",
    ]

    service.values().clear(
        spreadsheetId=SHEET_ID,
        range=f"{SHEET_NAME}!A:P",
    ).execute()

    service.values().update(
        spreadsheetId=SHEET_ID,
        range=f"{SHEET_NAME}!A1",
        valueInputOption="RAW",
        body={"values": [headers] + rows},
    ).execute()

    print(f"Written {len(rows)} reservations to Google Sheets")


# ── MAIN ─────────────────────────────────────────────────
def main():
    print(f"Fetching reservations {DATE_FROM} - {DATE_TO}...")

    xml_data = fetch_reservations()
    rows = parse_reservations(xml_data)
    print(f"Parsed {len(rows)} reservations")

    service = get_sheets_service()
    ensure_sheet_exists(service)
    write_to_sheets(service, rows)
    print("Done!")


if __name__ == "__main__":
    main()

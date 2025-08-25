from pathlib import Path
import pandas as pd
from pdf_extractors import extract_top_left_and_right, parse_recipient_and_address, parse_county_and_instrument

def stage_a(pdf_path, out_xlsx):
    extracted = extract_top_left_and_right(pdf_path)

    rows = []
    for page in extracted:
        recipient, address = parse_recipient_and_address(page["top_left_text"])
        county, instrument = parse_county_and_instrument(page["top_right_text"])
        rows.append({
            "County": county,
            "Instrument Number": instrument,
            "Recipient": recipient,
            "Address": address,
            "Page": page["page_index"] + 1,
        })

    df = pd.DataFrame(rows, columns=["County","Instrument Number","Recipient","Address","Page"])
    Path(out_xlsx).parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(out_xlsx, index=False)

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 3:
        print("Usage: python -m src.run_stage_a <path_to_pdf> <out_xlsx>")
        sys.exit(1)
    stage_a(sys.argv[1], sys.argv[2])

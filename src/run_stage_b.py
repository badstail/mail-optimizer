from pathlib import Path
import pandas as pd

def normalize_key_cols(df):
    # Make matching more reliable: strip spaces, uppercase
    for col in ["County", "Instrument Number"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()
    return df

def stage_b(jan_xlsx_in, jan_checked_in, out_xlsx):
    # Read both
    left = pd.read_excel(jan_xlsx_in)          # your original JAN.xlsx
    right = pd.read_excel(jan_checked_in)      # produced by Stage A

    left = normalize_key_cols(left)
    right = normalize_key_cols(right)

    # Optional: if your original JAN file uses different column names, map them here:
    # e.g., left.rename(columns={"Instr No":"Instrument Number","CountyName":"County"}, inplace=True)

    # Keep only the fields we want to attach from Stage A
    attach_cols = ["County", "Instrument Number", "Recipient", "Address"]
    right = right[attach_cols].drop_duplicates()

    merged = pd.merge(
        left, right,
        on=["County","Instrument Number"],
        how="left",  # keep all rows from JAN.xlsx; fill matches from JAN_checked
        suffixes=("", "_from_pdf")
    )

    Path(out_xlsx).parent.mkdir(parents=True, exist_ok=True)
    merged.to_excel(out_xlsx, index=False)

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 4:
        print("Usage: python -m src.run_stage_b <JAN.xlsx> <JAN_checked.xlsx> <out_xlsx>")
        sys.exit(1)
    stage_b(sys.argv[1], sys.argv[2], sys.argv[3])

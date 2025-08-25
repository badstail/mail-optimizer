from pathlib import Path
import pandas as pd

def _norm(df):
    for c in ["County", "Instrument Number"]:
        if c in df.columns: df[c] = df[c].astype(str).str.strip().str.upper()
    return df

def stage_b(jan_xlsx_in, jan_checked_in, out_xlsx):
    left = _norm(pd.read_excel(jan_xlsx_in))
    right = _norm(pd.read_excel(jan_checked_in))[["County","Instrument Number","Recipient","Address"]].drop_duplicates()
    merged = pd.merge(left, right, on=["County","Instrument Number"], how="left")
    Path(out_xlsx).parent.mkdir(parents=True, exist_ok=True)
    merged.to_excel(out_xlsx, index=False)

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 4:
        print("Usage: python -m src.run_stage_b <JAN.xlsx> <JAN_checked.xlsx> <out_xlsx>")
        raise SystemExit(1)
    stage_b(sys.argv[1], sys.argv[2], sys.argv[3])

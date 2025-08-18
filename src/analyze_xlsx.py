# src/analyze_xlsx.py
# Purpose:
# - Read your Excel (e.g., test.xlsx) that contains columns:
#   Postage, Page Count or Page Count.1, etc.
# - Compute average postage per page using ONLY rows where Postage is present.
# - Save a small JSON with metrics so other scripts can reuse it.

import json
from pathlib import Path
import pandas as pd
import numpy as np

# ---------- User settings you can adjust ----------
XLSX_PATH = Path("data/xlsx/test.xlsx")  # <-- Put your Excel file here
OUTPUT_JSON = Path("outputs/xlsx_metrics.json")
# If your file has two page columns, we'll prefer Page Count.1 (often “mailed pages”)
PAGE_COL_PREFER = ["Page Count.1", "Page Count"]

def main():
    # 1) Ensure folders exist (won't crash if already there)
    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)

    # 2) Load Excel
    #    errors='ignore' helps avoid weird encoding issues; usually not needed
    df = pd.read_excel(XLSX_PATH)

    # 3) Choose which page column to use
    page_col = None
    for c in PAGE_COL_PREFER:
        if c in df.columns:
            page_col = c
            break
    if page_col is None:
        raise ValueError(
            f"Could not find a page column. Expected one of: {PAGE_COL_PREFER}. "
            f"Columns present: {list(df.columns)}"
        )

    # 4) Convert Postage and Pages to numeric
    #    - errors='coerce' turns invalid values into NaN (missing)
    df["Postage_num"] = pd.to_numeric(df.get("Postage"), errors="coerce")
    df["Pages"] = pd.to_numeric(df[page_col], errors="coerce")

    # 5) Use ONLY rows that have a valid (non-missing) Postage value
    paid = df[df["Postage_num"].notna()].copy()

    # 6) Sum total paid postage and total paid pages
    total_postage = paid["Postage_num"].sum(min_count=1)  # min_count avoids 0 when all NaN
    total_pages = paid["Pages"].sum(min_count=1)

    if not total_pages or np.isnan(total_pages):
        raise ValueError("No valid pages found among paid rows. Check your Excel data.")

    # 7) Compute avg $ per page
    avg_per_page = float(total_postage / total_pages)

    # 8) Save metrics to JSON so other scripts can reuse this
    metrics = {
        "excel_file": str(XLSX_PATH),
        "page_column_used": page_col,
        "total_paid_postage": float(total_postage),
        "total_paid_pages": int(total_pages),
        "avg_postage_per_page": avg_per_page,
    }
    OUTPUT_JSON.write_text(json.dumps(metrics, indent=2))
    print("✅ Wrote", OUTPUT_JSON)
    print(f"Average postage per page: ${avg_per_page:.4f}")

if __name__ == "__main__":
    main()

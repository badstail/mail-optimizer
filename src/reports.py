# src/reports.py
# Purpose:
# - Read ALL monthly summary CSVs created by analyze_pdfs_by_month.py
#   (files named like: outputs/monthly/2025-01_summary.csv)
# - Aggregate by:
#     * Semester: H1 (Jan–Jun) / H2 (Jul–Dec) per year
#     * Year: all months in the year
# - Save results as CSVs under:
#     outputs/semester/<YEAR>_<H1|H2>_summary.csv
#     outputs/yearly/<YEAR>_summary.csv
#
# Notes:
# - This script assumes the monthly CSV has columns:
#     address, count, files, total_pages, sample_raw, estimated_savings
# - The 'files' column may be saved as a string like "['A.pdf','B.pdf']".
#   We'll safely parse it back to a Python list for rollups.

from pathlib import Path
import ast  # safer than eval for parsing list-like strings
import pandas as pd

# ---------- WHERE WE READ/WRITE ----------
MONTHLY_DIR = Path("outputs/monthly")
SEMESTER_DIR = Path("outputs/semester")
YEARLY_DIR = Path("outputs/yearly")

def safe_parse_files_cell(cell):
    """
    Convert a cell that might contain a stringified Python list (e.g. "['a.pdf','b.pdf']")
    back into a real list. If it's already a list, return it. If it's junk/empty, return [].
    """
    if isinstance(cell, list):
        return cell
    if isinstance(cell, str):
        cell = cell.strip()
        # Quick sanity check: looks like a list?
        if cell.startswith("[") and cell.endswith("]"):
            try:
                parsed = ast.literal_eval(cell)  # safer parsing than eval()
                return parsed if isinstance(parsed, list) else []
            except Exception:
                return []
    return []

def load_monthly():
    """
    Load all monthly CSVs into a single DataFrame and attach a 'month' column
    (e.g., '2025-01'). If none found, return an empty DataFrame.
    """
    rows = []
    for csv in sorted(MONTHLY_DIR.glob("*_summary.csv")):
        month = csv.name.split("_")[0]  # '2025-01' from '2025-01_summary.csv'
        df = pd.read_csv(csv)

        # Normalize expected columns just in case user edited the file:
        expected = {"address", "count", "files", "total_pages", "sample_raw", "estimated_savings"}
        missing = expected - set(df.columns)
        if missing:
            raise ValueError(f"{csv} is missing columns: {missing}")

        # Ensure numeric where needed (errors='coerce' turns bad data into NaN -> then fill 0)
        df["count"] = pd.to_numeric(df["count"], errors="coerce").fillna(0).astype(int)
        df["total_pages"] = pd.to_numeric(df["total_pages"], errors="coerce").fillna(0).astype(int)
        df["estimated_savings"] = pd.to_numeric(df["estimated_savings"], errors="coerce").fillna(0.0)

        # Parse files column back into lists
        df["files"] = df["files"].apply(safe_parse_files_cell)

        # Add the month label so we can group later
        df["month"] = month
        rows.append(df)

    if not rows:
        return pd.DataFrame()  # caller will handle the "nothing to do" case

    # Stack all monthly frames together
    return pd.concat(rows, ignore_index=True)

def month_to_year_semester(month_str: str):
    """
    Convert 'YYYY-MM' -> (year, semester), where:
    - H1 = months 1..6
    - H2 = months 7..12
    """
    year, mm = month_str.split("-")
    m = int(mm)
    sem = "H1" if 1 <= m <= 6 else "H2"
    return year, sem

def aggregate_semester(df_all: pd.DataFrame):
    """
    Create semester rollups:
    - Group by (year, semester, address)
    - Sum counts, pages, and savings
    - Combine files lists
    - Keep a sample of the raw address block
    Then write one CSV per (year, semester).
    """
    # Derive year and semester columns from month
    df_all[["year", "semester"]] = df_all["month"].apply(
        lambda s: pd.Series(month_to_year_semester(s))
    )

    # Aggregate. Note: we need to concatenate lists in 'files'
    def combine_files(series_of_lists):
        merged = []
        for lst in series_of_lists:
            merged.extend(lst)
        # Remove duplicates while preserving order:
        seen = set()
        unique = []
        for f in merged:
            if f not in seen:
                seen.add(f)
                unique.append(f)
        return unique

    grp = (
        df_all.groupby(["year", "semester", "address"], dropna=False)
              .agg(
                  count=("count", "sum"),
                  total_pages=("total_pages", "sum"),
                  estimated_savings=("estimated_savings", "sum"),
                  files=("files", combine_files),
                  sample_raw=("sample_raw", "first")  # just keep one example block of text
              )
              .reset_index()
    )

    # Write each (year, semester) slice to disk
    SEMESTER_DIR.mkdir(parents=True, exist_ok=True)
    for (yr, sem), df_part in grp.groupby(["year", "semester"]):
        out = SEMESTER_DIR / f"{yr}_{sem}_summary.csv"
        df_part.sort_values(["estimated_savings", "count"], ascending=[False, False]).to_csv(out, index=False)
        print(f"✅ Wrote {out}")

def aggregate_yearly(df_all: pd.DataFrame):
    """
    Create yearly rollups:
    - Group by (year, address)
    - Sum counts, pages, and savings
    - Combine files lists
    - Keep a sample of the raw address block
    Then write one CSV per year.
    """
    # Extract 'year' from the 'month' label "YYYY-MM"
    df_all["year"] = df_all["month"].str.slice(0, 4)

    def combine_files(series_of_lists):
        merged = []
        for lst in series_of_lists:
            merged.extend(lst)
        # Remove duplicates while preserving order
        seen = set()
        unique = []
        for f in merged:
            if f not in seen:
                seen.add(f)
                unique.append(f)
        return unique

    grp = (
        df_all.groupby(["year", "address"], dropna=False)
              .agg(
                  count=("count", "sum"),
                  total_pages=("total_pages", "sum"),
                  estimated_savings=("estimated_savings", "sum"),
                  files=("files", combine_files),
                  sample_raw=("sample_raw", "first")
              )
              .reset_index()
    )

    # Write one CSV per year
    YEARLY_DIR.mkdir(parents=True, exist_ok=True)
    for yr, df_part in grp.groupby("year"):
        out = YEARLY_DIR / f"{yr}_summary.csv"
        df_part.sort_values(["estimated_savings", "count"], ascending=[False, False]).to_csv(out, index=False)
        print(f"✅ Wrote {out}")

def main():
    # 1) Load all monthly CSVs. If none found, stop with a friendly message.
    df_all = load_monthly()
    if df_all.empty:
        print("⚠️  No monthly CSVs found in outputs/monthly. "
              "Run `python src/analyze_pdfs_by_month.py YYYY-MM` first.")
        return

    # 2) Build semester and yearly reports
    aggregate_semester(df_all)
    aggregate_yearly(df_all)

    # 3) (Optional) Also dump a single combined file for quick pivoting in Excel/Power BI
    combined_csv = YEARLY_DIR / "_all_months_flat.csv"
    YEARLY_DIR.mkdir(parents=True, exist_ok=True)
    df_all.sort_values(["month", "estimated_savings"], ascending=[True, False]).to_csv(combined_csv, index=False)
    print(f"✅ Wrote {combined_csv}")

if __name__ == "__main__":
    main()

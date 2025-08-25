# src/analyze_pdfs_by_month.py
# Purpose:
# - Process PDFs for a single month (folder: data/pdfs/YYYY-MM)
# - Extract likely "MAIL TO" address per PAGE (batch PDFs have many pages)
# - Skip the top recorder header band
# - Crop a large TOP-LEFT region (your updated location) AND TOP-RIGHT region (NEW)
# - Normalize addresses, group duplicates, estimate potential savings using avg $/page from JSON
# - Write outputs/monthly/YYYY-MM_summary.csv
#
# Usage:
#   python src/analyze_pdfs_by_month.py 2025-01

import json
import sys
import re
from pathlib import Path
import pandas as pd
import pdfplumber

# ---------- PATHS / CONFIG ----------
PDF_BASE = Path("data/pdfs")                 # base folder with YYYY-MM subfolders
OUTPUT_DIR = Path("outputs/monthly")         # where monthly CSVs will be written
METRICS_JSON = Path("outputs/xlsx_metrics.json")  # created by analyze_xlsx.py (avg $/page)

FIRST_PAGE_ONLY = False      # scan ALL pages (batch PDFs are huge)

# ====== TOP-LEFT CROP (UPDATED) ======
HEADER_SKIP_RATIO = 0.12     # skip the top 12% (recorder banner/header)
LEFT_BOX_WIDTH_RATIO = 0.62  # take 62% of the page width from the LEFT side
LEFT_BOX_HEIGHT_RATIO = 0.45 # take 45% of the page height starting AFTER header

# ====== TOP-RIGHT CROP (NEW) ======
RIGHT_BOX_WIDTH_RATIO = 0.38  # right 38% of page width
RIGHT_BOX_HEIGHT_RATIO = 0.45 # same height as left

# OCR fallback (helpful for scanned PDFs)
USE_OCR = True
try:
    import pytesseract
    from pdf2image import convert_from_path
except Exception:
    USE_OCR = False  # we'll continue without OCR if libs aren't available

# ---------- HELPERS ----------
def normalize_spaces(s: str) -> str:
    """Collapse multiple spaces/newlines and trim."""
    return re.sub(r"\s+", " ", (s or "")).strip()

def words_to_text(words):
    """Reconstruct visible lines in reading order from word boxes."""
    lines, current_y, line_words = [], None, []
    for w in sorted(words or [], key=lambda w: (round(w.get("top", 0)), w.get("x0", 0))):
        if current_y is None or abs(w.get("top", 0) - current_y) > 3:
            if line_words:
                lines.append(" ".join([lw.get("text", "") for lw in line_words]))
            line_words = [w]; current_y = w.get("top", 0)
        else:
            line_words.append(w)
    if line_words:
        lines.append(" ".join([lw.get("text", "") for lw in line_words]))
    return "\n".join(lines)

def top_left_bbox_excluding_header(page):
    """
    Build a bounding box that starts JUST BELOW the header band,
    covering a large TOP-LEFT rectangle where your mail-to block sits.
    """
    header_px = page.height * HEADER_SKIP_RATIO
    x0 = 0
    x1 = page.width * LEFT_BOX_WIDTH_RATIO
    top = header_px
    bottom = header_px + (page.height * LEFT_BOX_HEIGHT_RATIO)
    x0 = max(0, x0); x1 = min(x1, page.width)
    top = max(0, top); bottom = min(bottom, page.height)
    return (x0, top, x1, bottom)

def top_right_bbox_excluding_header(page):
    """
    Bounding box for the top-right region, avoiding the header.
    """
    header_px = page.height * HEADER_SKIP_RATIO
    x0 = page.width * (1 - RIGHT_BOX_WIDTH_RATIO)
    x1 = page.width
    top = header_px
    bottom = header_px + (page.height * RIGHT_BOX_HEIGHT_RATIO)
    x0 = max(0, x0); x1 = min(x1, page.width)
    top = max(0, top); bottom = min(bottom, page.height)
    return (x0, top, x1, bottom)

def crop_text(page, bbox):
    """
    Extract text ONLY from the given bbox by filtering word boxes.
    """
    words = page.extract_words() or []
    region_words = [
        w for w in words
        if (bbox[0] <= w.get("x0", 0) <= bbox[2]) and (bbox[1] <= w.get("top", 0) <= bbox[3])
    ]
    return words_to_text(region_words).strip()

def ocr_region_from_page(pdf_path: Path, page_index: int, left=True):
    """
    OCR fallback: render the page to an image and OCR the SAME region using the ratios.
    """
    if not USE_OCR:
        return ""
    try:
        imgs = convert_from_path(str(pdf_path), dpi=300,
                                 first_page=page_index + 1, last_page=page_index + 1)
        if not imgs:
            return ""
        img = imgs[0]
        W, H = img.size

        header_px = int(H * HEADER_SKIP_RATIO)
        if left:
            x0 = 0
            x1 = int(W * LEFT_BOX_WIDTH_RATIO)
        else:
            x0 = int(W * (1 - RIGHT_BOX_WIDTH_RATIO))
            x1 = W
        top = header_px
        bottom = int(header_px + (H * LEFT_BOX_HEIGHT_RATIO))
        region = img.crop((x0, top, x1, bottom))

        return pytesseract.image_to_string(region)
    except Exception as e:
        print(f"[DEBUG] OCR failed on page {page_index+1}: {e}")
        return ""

def looks_like_address_block(text_block: str) -> bool:
    """
    Heuristic: return True if the text contains a US-style city/state/ZIP line,
    e.g., 'SAN DIEGO, CA 92101' (comma optional), or a 5-digit ZIP somewhere.
    """
    t = (text_block or "").upper()
    if re.search(r"\b[A-Z]{2}\b[\s,]+(\d{5})(-\d{4})?\b", t):
        return True
    if re.search(r"\b\d{5}(-\d{4})?\b", t):
        return True
    return False

def extract_address_lines(text_block: str, max_lines=6):
    """
    From the cropped region text, keep the first few non-noise lines as the address block.
    """
    lines = [l.strip() for l in (text_block or "").splitlines() if l.strip()]
    drop = ["OFFICIAL RECORDS", "RECORDER", "DOCUMENT", "DOC#", "INSTRUMENT", "PAGES", "DATE", "TIME"]
    lines = [l for l in lines if all(p not in l.upper() for p in drop)]
    return "\n".join(lines[:max_lines]).strip()

def normalize_address(addr: str) -> str:
    """
    Normalize an address for grouping.
    """
    s = (addr or "").upper()
    s = re.sub(r"[^\w\s#&/,-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    repl = {
        r"\bSTREET\b": "ST",
        r"\bST\b": "ST",
        r"\bAVENUE\b": "AVE",
        r"\bROAD\b": "RD",
        r"\bBOULEVARD\b": "BLVD",
        r"\bDRIVE\b": "DR",
        r"\bCOURT\b": "CT",
        r"\bLANE\b": "LN",
        r"\bTERRACE\b": "TER",
    }
    for pat, sub in repl.items():
        s = re.sub(pat, sub, s)
    s = re.sub(r"\b(APARTMENT|APT\.)\b", "APT", s)
    s = re.sub(r"\bSUITE\b", "STE", s)
    return s

def get_avg_per_page() -> float:
    """Read avg $/page from JSON created by src/analyze_xlsx.py."""
    if not METRICS_JSON.exists():
        raise FileNotFoundError(f"{METRICS_JSON} not found. Run `python src/analyze_xlsx.py` first.")
    metrics = json.loads(METRICS_JSON.read_text())
    return float(metrics["avg_postage_per_page"])

# ---------- MAIN PER-MONTH ANALYSIS ----------
def main():
    if len(sys.argv) < 2:
        print("Usage: python src/analyze_pdfs_by_month.py YYYY-MM"); sys.exit(1)
    month = sys.argv[1]
    month_dir = PDF_BASE / month
    if not month_dir.exists():
        print(f"❌ Folder not found: {month_dir}"); sys.exit(1)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_csv = OUTPUT_DIR / f"{month}_summary.csv"

    avg_per_page = get_avg_per_page()
    print(f"[DEBUG] Using avg_per_page=${avg_per_page:.6f}")

    pdfs = sorted(month_dir.glob("*.pdf"))
    print(f"[DEBUG] PDFs found: {len(pdfs)} in {month_dir}")
    if not pdfs:
        pd.DataFrame(columns=[
            "left_address","right_address","count","files","total_pages",
            "sample_left_raw","sample_right_raw","estimated_savings"
        ]).to_csv(out_csv, index=False)
        print(f"⚠️  No PDFs found. Wrote empty summary: {out_csv}")
        return

    page_records = []
    for pdf_path in pdfs:
        try:
            with pdfplumber.open(pdf_path) as pdf:
                num_pages = len(pdf.pages)
                print(f"[DEBUG] Processing {pdf_path.name} ({num_pages} pages)")
                pages_to_scan = range(num_pages) if not FIRST_PAGE_ONLY else range(1)

                for i in pages_to_scan:
                    page = pdf.pages[i]

                    # --- LEFT TOP REGION ---
                    left_bbox = top_left_bbox_excluding_header(page)
                    left_text_block = crop_text(page, left_bbox)
                    if not left_text_block and USE_OCR:
                        left_text_block = ocr_region_from_page(pdf_path, i, left=True)
                    left_addr_raw = extract_address_lines(left_text_block) if left_text_block and looks_like_address_block(left_text_block) else ""
                    left_addr_norm = normalize_address(left_addr_raw) if left_addr_raw else ""

                    # --- RIGHT TOP REGION ---
                    right_bbox = top_right_bbox_excluding_header(page)
                    right_text_block = crop_text(page, right_bbox)
                    if not right_text_block and USE_OCR:
                        right_text_block = ocr_region_from_page(pdf_path, i, left=False)
                    right_addr_raw = extract_address_lines(right_text_block) if right_text_block and looks_like_address_block(right_text_block) else ""
                    right_addr_norm = normalize_address(right_addr_raw) if right_addr_raw else ""

                    # Only add if at least one address block found
                    if left_addr_raw or right_addr_raw:
                        page_records.append({
                            "file": pdf_path.name,
                            "page_index": i + 1,
                            "left_address_raw": left_addr_raw,
                            "left_address_norm": left_addr_norm,
                            "right_address_raw": right_addr_raw,
                            "right_address_norm": right_addr_norm,
                            "pages_for_cost": 1
                        })
        except Exception as e:
            print(f"[ERROR] {pdf_path.name}: {e}")

    if not page_records:
        pd.DataFrame(columns=[
            "left_address","right_address","count","files","total_pages",
            "sample_left_raw","sample_right_raw","estimated_savings"
        ]).to_csv(out_csv, index=False)
        print(f"⚠️  No addresses extracted. Wrote empty summary: {out_csv}")
        return

    df = pd.DataFrame(page_records)

    groups = (
        df.groupby(["left_address_norm", "right_address_norm"], dropna=False)
          .agg(
              count=("file", "count"),
              files=("file", lambda s: sorted(set(s))),
              total_pages=("pages_for_cost", "sum"),
              sample_left_raw=("left_address_raw", "first"),
              sample_right_raw=("right_address_raw", "first")
          )
          .reset_index()
          .rename(columns={
              "left_address_norm": "left_address",
              "right_address_norm": "right_address"
          })
    )

    groups["estimated_savings"] = groups["total_pages"] * avg_per_page

    groups = groups.sort_values(["estimated_savings", "count"], ascending=[False, False])
    groups.to_csv(out_csv, index=False)
    print(f"✅ Wrote {out_csv}")

if __name__ == "__main__":
    main()

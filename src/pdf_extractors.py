from pathlib import Path
import re
import pdfplumber

# ---- Region extractors ----
def _extract_region_text(page, left, bottom, right, top):
    with page.crop((left, bottom, right, top)) as region:
        return region.extract_text() or ""

def extract_top_left_and_right(pdf_path, left_pct=0.35, height_pct=0.28):
    """
    Returns list of dicts per page:
      {page_index, top_left_text, top_right_text}
    """
    pdf_path = Path(pdf_path)
    out = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            W, H = page.width, page.height
            # pdfplumber coords: (0,0)=bottom-left ; bbox = (left, bottom, right, top)

            # Top-left
            tl_left, tl_right = 0, W * left_pct
            tl_bottom, tl_top = H * (1 - height_pct), H
            top_left_text = _extract_region_text(page, tl_left, tl_bottom, tl_right, tl_top)

            # Top-right
            tr_left, tr_right = W * (1 - left_pct), W
            tr_bottom, tr_top = H * (1 - height_pct), H
            top_right_text = _extract_region_text(page, tr_left, tr_bottom, tr_right, tr_top)

            out.append({
                "page_index": i,
                "top_left_text": top_left_text.strip(),
                "top_right_text": top_right_text.strip(),
            })
    return out

# ---- Parsers ----
RECIPIENT_ANCHORS = [
    r"after\s+recording\s+mail\s+to",
    r"when\s+recorded\s+mail\s+to",
    r"recording\s+mail\s+to",
    r"return\s+to",
]

def parse_recipient_and_address(top_left_text, max_lines_after=5):
    """
    Finds anchor like 'After Recording Mail to:' and returns (recipient, address)
    Heuristic: take the next 2-5 non-empty lines as name+address block.
    """
    text = top_left_text.replace("\r", "\n")
    lines = [ln.strip() for ln in text.split("\n")]
    if not lines:
        return "", ""

    anchor_regex = re.compile(rf"({'|'.join(RECIPIENT_ANCHORS)}):?", re.IGNORECASE)
    recipient_lines = []
    start_idx = None

    for idx, ln in enumerate(lines):
        if anchor_regex.search(ln):
            start_idx = idx + 1
            break

    # If we didn't find anchor, fallback to first 3 lines as a weak guess
    if start_idx is None:
        candidate = [ln for ln in lines[:max_lines_after] if ln]
        if not candidate:
            return "", ""
        # name = first line; address = rest
        return candidate[0], "\n".join(candidate[1:])

    candidate = []
    for ln in lines[start_idx: start_idx + max_lines_after]:
        if ln:
            candidate.append(ln)

    if not candidate:
        return "", ""
    recipient = candidate[0]
    address = "\n".join(candidate[1:])
    return recipient, address

def parse_county_and_instrument(top_right_text):
    """
    Extracts County (line containing 'County') and Instrument number.
    Common instrument patterns like: 2024-012345, 1234567, 2024R012345, etc.
    """
    text = top_right_text.replace("\r", "\n")
    lines = [ln.strip() for ln in text.split("\n") if ln.strip()]

    county = ""
    for ln in lines:
        # Prefer line that ends with 'County' or contains 'County' prominently
        if re.search(r"\bcounty\b", ln, re.IGNORECASE):
            county = ln
            break

    instrument = ""
    # Look for a labeled instrument first
    inst_label = re.compile(r"(instrument|inst\.?\s*no\.?|doc(ument)?\s*no\.?)\s*[:#]?\s*([A-Za-z0-9\-\/]+)", re.IGNORECASE)
    for ln in lines:
        m = inst_label.search(ln)
        if m:
            instrument = m.group(4)
            break

    # Fallback: grab a reasonable looking ID (digits with optional letters and hyphens)
    if not instrument:
        generic = re.findall(r"\b[A-Za-z0-9]{4,}[A-Za-z0-9\-\/]*\b", " ".join(lines))
        # Heuristic: prefer ones with digits and length 6-16
        for tok in generic:
            if any(ch.isdigit() for ch in tok) and 6 <= len(tok) <= 18:
                instrument = tok
                break

    return county, instrument

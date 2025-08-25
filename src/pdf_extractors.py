from pathlib import Path
import re
import pdfplumber

def _extract_region_text(page, left, bottom, right, top):
    with page.crop((left, bottom, right, top)) as region:
        return region.extract_text() or ""

def extract_top_left_and_right(pdf_path, left_pct=0.35, height_pct=0.28):
    pdf_path = Path(pdf_path)
    out = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, page in enumerate(pdf.pages):
            W, H = page.width, page.height
            # top-left
            tl = _extract_region_text(page, 0, H*(1-height_pct), W*left_pct, H)
            # top-right
            tr = _extract_region_text(page, W*(1-left_pct), H*(1-height_pct), W, H)
            out.append({"page_index": i, "top_left_text": tl.strip(), "top_right_text": tr.strip()})
    return out

RECIPIENT_ANCHORS = [r"after\s+recording\s+mail\s+to", r"when\s+recorded\s+mail\s+to", r"return\s+to"]

def parse_recipient_and_address(top_left_text, max_lines_after=5):
    lines = [ln.strip() for ln in top_left_text.replace("\r","\n").split("\n")]
    anchor = re.compile(rf"({'|'.join(RECIPIENT_ANCHORS)}):?", re.IGNORECASE)
    start = next((i+1 for i,ln in enumerate(lines) if anchor.search(ln)), None)
    block = [ln for ln in (lines[start:start+max_lines_after] if start is not None else lines[:max_lines_after]) if ln]
    if not block: return "", ""
    return block[0], "\n".join(block[1:])

def parse_county_and_instrument(top_right_text):
    lines = [ln.strip() for ln in top_right_text.replace("\r","\n").split("\n") if ln.strip()]
    county = next((ln for ln in lines if re.search(r"\bcounty\b", ln, re.IGNORECASE)), "")
    m = None
    for ln in lines:
        m = re.search(r"(instrument|inst\.?\s*no\.?|doc(ument)?\s*no\.?)\s*[:#]?\s*([A-Za-z0-9\-\/]+)", ln, re.IGNORECASE)
        if m: break
    instrument = m.group(4) if m else ""
    if not instrument:
        for tok in re.findall(r"\b[A-Za-z0-9]{4,}[A-Za-z0-9\-\/]*\b", " ".join(lines)):
            if any(ch.isdigit() for ch in tok) and 6 <= len(tok) <= 18:
                instrument = tok; break
    return county, instrument

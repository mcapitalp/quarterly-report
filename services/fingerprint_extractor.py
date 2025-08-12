# app/services/fingerprint_extractor.py
import io
import re
import json
import hashlib
import pandas as pd

from utils.helpers import safe, find_row, grab_table, extract_esg
from services.excel_parser import extract_fund_quarter  # returns (fund, "Qx YYYY")

# ---------- small utils ----------
def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def _stable_json(obj) -> str:
    # deterministic JSON (sorted keys, no spaces)
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))

def _parse_year_quarter(label: str):
    # label like "Q3 2025"
    m = re.match(r"Q([1-4])\s+(\d{4})", label or "", flags=re.I)
    if not m:
        return None, None
    return int(m.group(2)), int(m.group(1))  # (year, quarter)

# ---------- structure detection (same style as your code) ----------
def _company_start_cols(df: pd.DataFrame):
    return sorted([
        c for c in range(df.shape[1])
        if df.iloc[:, c].astype(str).str.fullmatch("Name of Investment", case=False).any()
    ])

def _company_name_from_block(df: pd.DataFrame, col0: int):
    r_name = find_row(df, [col0], r"^Name of Investment$")
    if r_name is None:
        return None
    for cc in range(col0 + 1, df.shape[1]):
        nm = safe(df.iloc[r_name, cc])
        if nm:
            s = str(nm).strip()
            if not s or s.lower() in {"nan", "none", "n/a", "-", "name of investment"}:
                continue
            return s
    return None


# ---------- section extractors (mirror your parse_portfolio/KPI logic) ----------
def _extract_fields_dict(df: pd.DataFrame, col0: int):
    """Key/value lines under 'Name of Investment' until a stop header."""
    r_name = find_row(df, [col0], r"^Name of Investment$")
    if r_name is None:
        return {}

    fields = {}
    stop_rgx = re.compile(r"(Year to Date|Actual Cash Flows|Year on Year|ESG Overview)", re.I)
    r = r_name + 1
    while r < df.shape[0]:
        k = safe(df.iloc[r, col0])
        v = safe(df.iloc[r, col0 + 1]) if col0 + 1 < df.shape[1] else None
        if not k or (isinstance(k, str) and k.lower() == "notes:"):
            r += 1
            continue
        if isinstance(k, str) and stop_rgx.search(k):
            break
        # keep only truly filled values to avoid template noise
        if v not in (None, "", "nan"):
            fields[str(k)] = v
        r += 1
    return fields

def _extract_kpis_dict(df: pd.DataFrame, col0: int):
    """Sales / EBITDA / Net income / Net debt as numbers (or None)."""
    metrics = {"sales": None, "ebitda": None, "net_income": None, "net_debt": None}
    pats = {
        "sales": r"^\s*Sales\s*$",
        "ebitda": r"^\s*EBITDA\s*$",
        "net_income": r"^\s*Net income\s*$",
        "net_debt": r"^\s*Net debt"
    }
    for key, pat in pats.items():
        r = find_row(df, [col0], pat)
        if r is not None and col0 + 1 < df.shape[1]:
            raw = safe(df.iloc[r, col0 + 1])
            if raw is not None:
                try:
                    metrics[key] = float(str(raw).replace(",", ""))
                except:
                    metrics[key] = None
    return metrics

def _extract_text_blocks(df: pd.DataFrame, col0: int, cols_in_block: list[int]):
    """Same headers list as your parse_portfolio, gathered as a dict of strings."""
    hdrs = ["Investment Overview", "Significant Events", "YTD Q3 Analysis", "March 2025 Budget", "Exit Plans"]
    text = {}
    for h in hdrs:
        rh = find_row(df, [col0], rf"^{re.escape(h)}$")
        if rh is None:
            continue
        lines = []
        for r in range(rh + 1, df.shape[0]):
            cell = safe(df.iloc[r, col0])
            if cell and any(x.lower() in cell.lower() for x in hdrs + ["ESG Overview"]):
                break
            if cell:
                lines.append(cell)
        if lines:
            text[h] = "\n".join(lines)
    return text

# ---------- main entry ----------
def extract_fingerprints_from_bytes(content: bytes, fname: str):
    """
    Returns a list of dicts with:
      fund, quarter_label, year, quarter, company,
      fields_hash, kpi_hash, text_hash, esg_hash, overall_hash
    Skips:
      - blocks with empty company name
      - blocks where all four sections are empty
    """
    fund, quarter_label = extract_fund_quarter(fname)  # e.g. ("MCIV", "Q3 2025")
    year, quarter = _parse_year_quarter(quarter_label)

    df = pd.read_excel(io.BytesIO(content), sheet_name="Portfolio_Input", header=None, engine="openpyxl")
    starts = _company_start_cols(df)

    items = []
    for i, col0 in enumerate(starts):
        # Determine the column span for this company block (for ESG/text boundaries)
        col1 = starts[i + 1] if i + 1 < len(starts) else df.shape[1]
        cols = list(range(col0, col1))

        # 1) Name guard — skip entire block if no company name
        name = _company_name_from_block(df, col0)
        if not name:
            continue

        # 2) Extract sections (dicts). Each may be empty.
        fields_dict = _extract_fields_dict(df, col0)
        kpis_dict   = _extract_kpis_dict(df, col0)
        text_blocks = _extract_text_blocks(df, col0, cols)
        esg_dict    = extract_esg(df, cols)  # uses your helper

        # 3) Compute hashes per section (None if section is logically empty)
        fields_hash = _sha256(_stable_json(fields_dict)) if fields_dict else None

        # KPI hash only if there is at least one numeric value present
        kpi_hash = None
        if any(v is not None for v in kpis_dict.values()):
            kpi_hash = _sha256(_stable_json(kpis_dict))

        text_hash = _sha256(_stable_json(text_blocks)) if text_blocks else None
        esg_hash  = _sha256(_stable_json(esg_dict)) if esg_dict else None

        # 4) If all four sections are empty, skip (prevents empty investments)
        if not any([fields_hash, kpi_hash, text_hash, esg_hash]):
            continue

        # 5) Overall fingerprint from available section hashes (stable order)
        parts = [h for h in [fields_hash, kpi_hash, text_hash, esg_hash] if h]
        overall_hash = _sha256("|".join(parts))

        items.append({
            "fund": fund,
            "quarter_label": quarter_label,
            "year": year,
            "quarter": quarter,
            "company": name,
            "fields_hash": fields_hash,
            "kpi_hash": kpi_hash,
            "text_hash": text_hash,
            "esg_hash": esg_hash,
            "overall_hash": overall_hash
        })

    return items

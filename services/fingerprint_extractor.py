import io, re, json, hashlib
import pandas as pd
from utils.helpers import safe, find_row
from services.excel_parser import extract_fund_quarter  # returns (fund, "Qx YYYY")

def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def _stable_json(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"))

def _company_start_cols(df: pd.DataFrame):
    # same logic you use elsewhere
    return sorted([
        c for c in range(df.shape[1])
        if df.iloc[:, c].astype(str).str.fullmatch("Name of Investment", case=False).any()
    ])

def _company_name_from_block(df: pd.DataFrame, col0: int):
    # same as your working code, plus stricter empties filter
    r_name = find_row(df, [col0], r"^Name of Investment$")
    if r_name is None:
        return None
    for cc in range(col0 + 1, df.shape[1]):
        nm = safe(df.iloc[r_name, cc])
        if nm:
            nm = str(nm).strip()
            if nm and nm.lower() not in {"", "nan", "none", "n/a", "-"}:
                return nm
    return None

def _extract_fields_hash(df: pd.DataFrame, col0: int):
    # mirror of your parse_portfolio "fields" loop
    r_name = find_row(df, [col0], r"^Name of Investment$")
    if r_name is None:
        return None
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
        fields[str(k)] = v  # keep original casing like your RAG code
        r += 1
    if not fields:
        return None
    return _sha256(_stable_json(fields))

def _extract_kpis_hash(df: pd.DataFrame, col0: int):
    # same metric set & patterns as your KPI extractor
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
    # if all four are None, there is no KPI section → skip
    if all(v is None for v in metrics.values()):
        return None
    return _sha256(_stable_json(metrics))

def _parse_year_quarter(label: str):
    m = re.match(r"Q([1-4])\s+(\d{4})", label or "", flags=re.I)
    if not m:
        return None, None
    return int(m.group(2)), int(m.group(1))  # (year, quarter)

def extract_fingerprints_from_bytes(content: bytes, fname: str):
    fund, quarter_label = extract_fund_quarter(fname)  # e.g., ("MCIV", "Q3 2025")
    year, quarter = _parse_year_quarter(quarter_label)

    df = pd.read_excel(io.BytesIO(content), sheet_name="Portfolio_Input", header=None, engine="openpyxl")
    starts = _company_start_cols(df)

    items = []
    for col0 in starts:
        name = _company_name_from_block(df, col0)
        if not name:
            continue  # skip templates / empty columns

        field_hash = _extract_fields_hash(df, col0)
        kpi_hash   = _extract_kpis_hash(df, col0)

        # if neither section exists, skip entirely (prevents empty investments)
        if not field_hash and not kpi_hash:
            continue

        # overall hash is a stable concat of available section hashes
        parts = [h for h in [field_hash, kpi_hash] if h]
        overall_hash = _sha256("|".join(parts))

        items.append({
            "company_name": name,
            "fund": fund,
            "year": year,
            "quarter": quarter,
            "overall_hash": overall_hash,
            "field_hash": field_hash,
            "kpi_hash": kpi_hash
        })

    return items

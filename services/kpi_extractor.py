import io
import pandas as pd
from utils.helpers import safe, find_row
from services.excel_parser import extract_fund_quarter

def _company_start_cols(df):
    return sorted([
        c for c in range(df.shape[1])
        if df.iloc[:, c].astype(str).str.fullmatch("Name of Investment", case=False).any()
    ])

def _company_name_from_block(df, col0):
    r_name = find_row(df, [col0], r"^Name of Investment$")
    if r_name is None:
        return None
    for cc in range(col0 + 1, df.shape[1]):
        nm = safe(df.iloc[r_name, cc])
        if nm:
            return nm
    return None

def extract_kpi_values(df, col0):
    metrics = {"sales": None, "ebitda": None, "net_income": None, "net_debt": None}
    metric_patterns = {
        "sales": r"^\s*Sales\s*$",
        "ebitda": r"^\s*EBITDA\s*$",
        "net_income": r"^\s*Net income\s*$",
        "net_debt": r"^\s*Net debt"
    }
    for metric, pat in metric_patterns.items():
        r = find_row(df, [col0], pat)
        if r is not None:
            raw_value = safe(df.iloc[r, col0 + 1])
            try:
                metrics[metric] = float(str(raw_value).replace(",", ""))
            except:
                metrics[metric] = None
    return metrics

def extract_all_kpis_from_bytes(content: bytes, fname: str):
    fund, quarter = extract_fund_quarter(fname)
    df = pd.read_excel(
        io.BytesIO(content),
        sheet_name="Portfolio_Input",
        header=None,
        engine="openpyxl",
        nrows=70
    )
    starts = _company_start_cols(df)

    investments = []
    for col0 in starts:
        name = _company_name_from_block(df, col0)
        if not name:
            continue
        kpis = extract_kpi_values(df, col0)
        if all(v is None for v in kpis.values()):
            continue
        investments.append({
            "fund": fund,
            "quarter": quarter,
            "company": name,
            **kpis
        })
    return investments

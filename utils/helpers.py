import pandas as pd
import re
import datetime

def safe(x):
    return None if pd.isna(x) else str(x).strip()

def jdefault(o):
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()
    if isinstance(o, float) and pd.isna(o):
        return None
    return o

def find_row(df, cols, pat):
    rgx = re.compile(pat, re.I)
    for r in range(df.shape[0]):
        for c in cols:
            if rgx.search(str(df.iloc[r, c])):
                return r
    return None

def grab_table(df, row0, cols):
    rows = []
    for r in range(row0, df.shape[0]):
        vals = [safe(df.iloc[r, c]) for c in cols]
        if all(v in (None, "", "nan") for v in vals):
            break
        rows.append(vals)
    return rows if len(rows) > 1 else None

def extract_esg(df, cols):
    start = find_row(df, cols, r"ESG Overview")
    end = find_row(df, cols, r"Action Plan Compliance") or df.shape[0]
    if start is None:
        return {}
    esg = {}
    for r in range(start + 1, end):
        cells = [safe(df.iloc[r, c]) for c in cols if safe(df.iloc[r, c])]
        if len(cells) >= 2:
            esg[cells[0]] = cells[1]
    imp = find_row(df, cols, r"ESG Improvements")
    if imp:
        lines = []
        for r in range(imp + 1, end):
            cell = safe(df.iloc[r, cols[0]])
            if cell:
                lines.append(cell)
        if lines:
            esg["ESG Improvements"] = "\n".join(lines)
    return esg

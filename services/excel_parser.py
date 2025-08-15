import re
import pandas as pd
from typing import List, Dict, Any
from utils.helpers import safe, find_row, grab_table, extract_esg

def extract_fund_quarter(fname: str):
    q = re.search(r"Q([1-4])[\s_-]*(\d{4})", fname, re.I)
    f = re.search(r"MC[\s_-]*[IVXLCD]+", fname, re.I)
    return (
        f.group().replace(" ", "").replace("_", "").replace("-", "").upper() if f else "UNKNOWN",
        f"Q{q.group(1)} {q.group(2)}" if q else "UNKNOWN",
    )

def parse_portfolio(file_stream, fname) -> List[Dict[str, Any]]:
    fund, quarter = extract_fund_quarter(fname)
    df = pd.read_excel(file_stream, sheet_name="Portfolio_Input", header=None, engine="openpyxl")

    starts = [c for c in range(df.shape[1])
              if df.iloc[:, c].astype(str).str.fullmatch("Name of Investment", case=False).any()]
    starts.sort()

    investments = []
    for i, col0 in enumerate(starts):
        col1 = starts[i + 1] if i + 1 < len(starts) else df.shape[1]
        cols = list(range(col0, col1))

        r_name = find_row(df, [col0], r"^Name of Investment$")
        if r_name is None:
            continue
        name = safe(df.iloc[r_name, col0 + 1])
        if not name:
            continue

        # Extract fields until we hit KPI/ESG section
        fields = {}
        stop_rgx = re.compile(r"(Year to Date|Actual Cash Flows|Year on Year|ESG Overview)", re.I)
        r = r_name + 1
        while r < df.shape[0]:
            k, v = safe(df.iloc[r, col0]), safe(df.iloc[r, col0 + 1])
            if not k or k.lower() == "notes:":
                r += 1
                continue
            if stop_rgx.search(k):
                break
            fields[k] = v
            r += 1

        # Removed KPI and cash flow table extraction entirely
        tables = {}

        esg = extract_esg(df, cols)

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

        investments.append({
            "fund": fund,
            "quarter": quarter,
            "name": name,
            "fields": fields,
            "tables": tables,
            "esg": esg,
            "text_blocks": text
        })
    return investments

def table_to_tsv(table):
    return "\n".join(["\t".join("" if v is None else str(v) for v in row) for row in table])

def investments_to_docs(invs):
    docs = []
    for inv in invs:
        tag = f"[Company: {inv['name']}] [Fund: {inv['fund']}] [Quarter: {inv['quarter']}]"

        # Text sections
        for section, text in inv["text_blocks"].items():
            docs.append({
                "document": f"{tag}\n{section}\n\n{text}",
                "metadata": {
                    "fund": inv['fund'],
                    "quarter": inv['quarter'],
                    "investment": inv['name'],
                    "section": section
                }
            })

        # ESG
        if inv.get("esg"):
            lines = [f"{k}: {v}" for k, v in inv["esg"].items() if v]
            docs.append({
                "document": f"{tag}\nESG Overview\n\n" + "\n".join(lines),
                "metadata": {
                    "fund": inv['fund'],
                    "quarter": inv['quarter'],
                    "investment": inv['name'],
                    "section": "ESG"
                }
            })

        # Fields
        if inv.get("fields"):
            lines = [f"{k}: {v}" for k, v in inv["fields"].items()]
            docs.append({
                "document": f"{tag}\nFields\n\n" + "\n".join(lines),
                "metadata": {
                    "fund": inv['fund'],
                    "quarter": inv['quarter'],
                    "investment": inv['name'],
                    "section": "Fields"
                }
            })

        # No KPI tables anymore
        for tname, table in inv["tables"].items():
            docs.append({
                "document": f"{tag}\n{tname}\n\n" + table_to_tsv(table),
                "metadata": {
                    "fund": inv['fund'],
                    "quarter": inv['quarter'],
                    "investment": inv['name'],
                    "section": tname
                }
            })
    return docs

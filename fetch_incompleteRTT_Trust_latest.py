# This code fetches incompleteRTT-Trust-all data from the NHS page and writes it to a Google Sheet.
# It prepends the latest month’s rows under the header (row 1) and writes Month yy (e.g., Oct25) as PURE TEXT (no date parsing, no visible apostrophe).

import warnings
warnings.filterwarnings("ignore", message="urllib3 v2 only supports OpenSSL.*")

import os
import re
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

import gspread
from google.oauth2.service_account import Credentials


# ====== CONFIG ======
PAGE_URL = "https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/rtt-data-2025-26/"

SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1L-22eKojGVYdSq2gPMzX3K8MC9IZVyEtAQ3s7h2jv28/edit?gid=0#gid=0"
SERVICE_ACCOUNT_JSON = "/Users/sonia.rodriguez/Documents/ICB_Performance_Risks/shiny-to-sheets-482011-38125cda0263.json"

META_SHEET_NAME = "meta_all"
OUTPUT_SHEET_NAME = "incompleteRTT-Trust-all"

DOWNLOAD_DIR = os.path.abspath("./downloads")

# File specifics
EXCEL_SHEET_NAME = "Provider"
HEADER_ROW_1_INDEXED = 14
HEADER_ROW_0_INDEXED = HEADER_ROW_1_INDEXED - 1

# Filter
TFC_COL = "Treatment Function Code"
TFC_VALUE = "C_330"

# Keep columns: A–F, then DH–END
KEEP_PREFIX_END = "F"
KEEP_SUFFIX_START = "DH"

LONDON_TZ = ZoneInfo("Europe/London")

# Meta: write values to ROW 11 (no headers)
META_VALUE_ROW = 11

# Month column requirements
MONTH_COL_NAME = "Month yy"
MONTH_COL_INDEX_0BASED = 14      # insert at position 14 => becomes column O (15th)
MONTH_COL_INDEX_1BASED = 15      # O
# =====================


def ensure_download_dir():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)


# ---------- Google Sheets helpers ----------

def connect_gsheets():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_url(SPREADSHEET_URL)


def get_or_create_worksheet(sh, title: str, rows: int = 1000, cols: int = 26):
    try:
        return sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=str(rows), cols=str(cols))


def ensure_ws_size(ws, min_rows: int, min_cols: int):
    if ws.row_count < min_rows or ws.col_count < min_cols:
        ws.resize(rows=max(ws.row_count, min_rows), cols=max(ws.col_count, min_cols))


def write_meta_row_row11_only(
    meta_ws,
    ok: bool,
    file_label: str,
    num_rows: int,
    num_cols: int,
    runtime_seconds: float,
    original_files: str,
):
    status_emoji = "✅" if ok else "⚠️"
    now = datetime.now(LONDON_TZ)

    values = [[
        status_emoji,
        file_label,
        now.strftime("%d/%m/%Y"),
        now.strftime("%H:%M:%S"),
        "London",
        num_rows,
        num_cols,
        round(runtime_seconds, 2),
        original_files,
    ]]

    ensure_ws_size(meta_ws, min_rows=META_VALUE_ROW, min_cols=9)

    meta_ws.update(
        range_name=f"A{META_VALUE_ROW}:I{META_VALUE_ROW}",
        values=values,
        value_input_option="RAW",
    )


def month_exists_in_output_sheet(out_ws, mmmyy: str) -> bool:
    # Column O values (skip header). Compare as stripped strings.
    col_vals = out_ws.col_values(MONTH_COL_INDEX_1BASED)
    target = str(mmmyy).strip()
    for v in col_vals[1:]:
        if str(v).strip() == target:
            return True
    return False


def insert_rows_after_header(out_ws, how_many: int):
    if how_many <= 0:
        return
    ensure_ws_size(out_ws, min_rows=out_ws.row_count + how_many, min_cols=out_ws.col_count)
    out_ws.insert_rows([[""] * out_ws.col_count] * how_many, row=2)


def write_df_into_output_sheet_after_header(out_ws, df: pd.DataFrame):
    """
    Writes df into the sheet starting at A2 (header is assumed to already be in row 1).
    Key fix: Month yy values are re-written with value_input_option="RAW" so Oct25 stays TEXT
    (no date parsing) and does NOT display a leading apostrophe.
    """
    df = df.copy()
    df.columns = df.columns.astype(str)
    df = df.where(pd.notnull(df), "")

    values = df.values.tolist()
    if not values:
        return

    needed_rows = 1 + len(values)
    needed_cols = max(out_ws.col_count, len(df.columns), MONTH_COL_INDEX_1BASED)
    ensure_ws_size(out_ws, min_rows=needed_rows, min_cols=needed_cols)

    # 1) Write everything with USER_ENTERED (keeps numbers as numbers, etc.)
    out_ws.update(values=values, range_name="A2", value_input_option="USER_ENTERED")

    # 2) Overwrite Month yy column ONLY with RAW strings to prevent auto-parsing (Oct25 -> date)
    # Month yy is column O, rows 2..(1+len(values))
    month_vals = [[str(v).strip()] for v in df[MONTH_COL_NAME].tolist()]
    end_row = 1 + len(month_vals)
    out_ws.update(values=month_vals, range_name=f"O2:O{end_row}", value_input_option="RAW")

    # 3) Also set the whole column format to TEXT (belt & braces)
    out_ws.format("O:O", {"numberFormat": {"type": "TEXT"}})


# ---------- Find latest month section + Incomplete Provider link ----------

MONTH_YEAR_RE = re.compile(
    r"^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})$"
)


def parse_month_year(text: str):
    m = MONTH_YEAR_RE.match(text.strip())
    if not m:
        return None
    month_name, year = m.group(1), int(m.group(2))
    dt = datetime.strptime(f"{month_name} {year}", "%B %Y")
    return datetime(dt.year, dt.month, 1)


def find_latest_incomplete_provider_link(page_url: str):
    headers = {"User-Agent": "Mozilla/5.0 (compatible; rtt-scraper/1.0)"}
    r = requests.get(page_url, headers=headers, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    headings = soup.find_all(["h2", "h3"])
    found = []

    for h in headings:
        heading_text = h.get_text(strip=True)
        month_dt = parse_month_year(heading_text)
        if not month_dt:
            continue

        mmmyy = None
        file_url = None
        link_text = None

        for sib in h.find_all_next():
            if sib.name in ("h2", "h3"):
                nxt_text = sib.get_text(strip=True)
                if parse_month_year(nxt_text):
                    break

            if sib.name == "a":
                t = sib.get_text(" ", strip=True)
                m = re.match(r"^Incomplete\s+Provider\s+([A-Za-z]{3}\d{2})\b", t)
                if m:
                    mmmyy = m.group(1).title()
                    href = sib.get("href")
                    file_url = urljoin(page_url, href)
                    link_text = t
                    break

        if mmmyy and file_url:
            found.append((month_dt, heading_text, mmmyy, file_url, link_text))

    if not found:
        raise RuntimeError("Could not find any 'Incomplete Provider <mmmyy>' links inside month sections on the page.")

    found.sort(key=lambda x: x[0], reverse=True)
    _, month_label, mmmyy, file_url, link_text = found[0]
    return month_label, mmmyy, file_url, link_text


# ---------- Download + read ----------

def download_file(url: str, filename: str) -> str:
    ensure_download_dir()
    path = os.path.join(DOWNLOAD_DIR, filename)

    with requests.get(url, stream=True, timeout=180) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if chunk:
                    f.write(chunk)

    return path


def read_provider_sheet(path: str) -> pd.DataFrame:
    return pd.read_excel(
        path,
        sheet_name=EXCEL_SHEET_NAME,
        header=HEADER_ROW_0_INDEXED,
        engine="openpyxl",
    )


# ---------- Column selection (Excel letters) ----------

def excel_col_to_index(col: str) -> int:
    col = col.strip().upper()
    idx = 0
    for c in col:
        idx = idx * 26 + (ord(c) - ord("A") + 1)
    return idx - 1


def keep_A_to_F_and_DH_to_end(df: pd.DataFrame, prefix_end="F", suffix_start="DH") -> pd.DataFrame:
    ncols = df.shape[1]
    end_prefix = excel_col_to_index(prefix_end)
    start_suffix = excel_col_to_index(suffix_start)

    prefix_end_idx = min(end_prefix, ncols - 1)
    prefix_idx = list(range(0, prefix_end_idx + 1))

    suffix_idx = list(range(start_suffix, ncols)) if start_suffix < ncols else []

    keep_idx = prefix_idx + suffix_idx
    out = df.iloc[:, keep_idx].copy()
    out = out.loc[:, ~out.columns.astype(str).str.match(r"^Unnamed")]
    return out


# ---------- Main ----------

def main():
    start = time.time()

    sh = connect_gsheets()
    meta_ws = get_or_create_worksheet(sh, META_SHEET_NAME, rows=1000, cols=10)
    out_ws = get_or_create_worksheet(sh, OUTPUT_SHEET_NAME, rows=5000, cols=250)

    # Make sure column O exists and format it as TEXT up-front (helps, but we also RAW-overwrite later)
    ensure_ws_size(out_ws, min_rows=2, min_cols=MONTH_COL_INDEX_1BASED)
    out_ws.format("O:O", {"numberFormat": {"type": "TEXT"}})

    try:
        month_label, mmmyy, file_url, link_text = find_latest_incomplete_provider_link(PAGE_URL)

        if month_exists_in_output_sheet(out_ws, mmmyy):
            runtime = time.time() - start
            write_meta_row_row11_only(
                meta_ws=meta_ws,
                ok=False,
                file_label=f"incompleteRTT-Trust: {mmmyy} already fetched",
                num_rows=0,
                num_cols=0,
                runtime_seconds=runtime,
                original_files="",
            )
            print(f"Skipped {month_label} already exists in {OUTPUT_SHEET_NAME}")
            return

        filename = f"Incomplete Provider {mmmyy}.xlsx"
        filepath = download_file(file_url, filename)

        df = read_provider_sheet(filepath)
        df = keep_A_to_F_and_DH_to_end(df, prefix_end=KEEP_PREFIX_END, suffix_start=KEEP_SUFFIX_START)

        if TFC_COL not in df.columns:
            raise ValueError(
                f"'{TFC_COL}' not found after keeping A–{KEEP_PREFIX_END} + {KEEP_SUFFIX_START}–END. "
                f"Columns present: {list(df.columns)}"
            )

        df_filtered = df[df[TFC_COL].astype(str).str.strip() == TFC_VALUE].copy()

        # Insert Month yy as column O (15th column)
        df_filtered.insert(MONTH_COL_INDEX_0BASED, MONTH_COL_NAME, str(mmmyy))

        # Insert rows beneath header and write
        insert_rows_after_header(out_ws, len(df_filtered))
        write_df_into_output_sheet_after_header(out_ws, df_filtered)

        runtime = time.time() - start
        write_meta_row_row11_only(
            meta_ws=meta_ws,
            ok=True,
            file_label=f"incompleteRTT-Trust: {mmmyy} has been added",
            num_rows=len(df_filtered),
            num_cols=len(df_filtered.columns),
            runtime_seconds=runtime,
            original_files=filename,
        )

        print(
            f"Loaded {month_label}: {link_text} | Provider | Kept A:{KEEP_PREFIX_END} & {KEEP_SUFFIX_START}:END | "
            f"Filter {TFC_COL}={TFC_VALUE}"
        )

    except Exception as e:
        runtime = time.time() - start
        write_meta_row_row11_only(
            meta_ws=meta_ws,
            ok=False,
            file_label=f"ERROR: {type(e).__name__}: {e}",
            num_rows=0,
            num_cols=0,
            runtime_seconds=runtime,
            original_files="",
        )
        raise


if __name__ == "__main__":
    main()

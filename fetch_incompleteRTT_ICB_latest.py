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


# =========================
# Configuration
# =========================
PAGE_URL = "https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/rtt-data-2025-26/"
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1L-22eKojGVYdSq2gPMzX3K8MC9IZVyEtAQ3s7h2jv28/edit?gid=0#gid=0"
SERVICE_ACCOUNT_JSON = "/Users/sonia.rodriguez/Documents/ICB_Performance_Risks/shiny-to-sheets-482011-38125cda0263.json"

META_SHEET_NAME = "meta_all"
OUTPUT_SHEET_NAME = "incompleteRTT-ICB-all"

MONTH_COL_NAME = "Month yy"
MONTH_COL_ABS_1BASED = 14  # Column N in Google Sheets
MONTH_COL_0BASED = MONTH_COL_ABS_1BASED - 1  # 13

DOWNLOAD_DIR = os.path.abspath("./downloads")
LONDON_TZ = ZoneInfo("Europe/London")

META_ROW = 10  # write meta here


# =========================
# Excel file specifics
# =========================
HEADER_ROW = 13  # 0-indexed header row (row 14 in Excel)
ICB_SHEET_NAME = "ICB"
NATIONAL_SHEET_NAME = "National"

TFC_COL = "Treatment Function Code"
TFC_VALUE = "C_330"

ICB_KEEP_COL = [("A", "E"), ("DG", "DO")]
NAT_KEEP_COL = [("A", "C"), ("DE", "DM")]

NATIONAL_LABEL = "NATIONAL"


# =========================
# Helpers
# =========================
def ensure_download_dir():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)


def connect_gsheets():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON, scopes=scopes)
    return gspread.authorize(creds).open_by_url(SPREADSHEET_URL)


def get_or_create_ws(sh, title: str, rows: int = 1000, cols: int = 50):
    try:
        return sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=str(rows), cols=str(cols))


def ensure_ws_size(ws, min_rows: int, min_cols: int):
    if ws.row_count < min_rows or ws.col_count < min_cols:
        ws.resize(rows=max(ws.row_count, min_rows), cols=max(ws.col_count, min_cols))


def write_meta(meta_ws, ok: bool, message: str, rows: int, cols: int, runtime: float, original_file: str):
    emoji = "✅" if ok else "⚠️"
    now = datetime.now(LONDON_TZ)

    ensure_ws_size(meta_ws, min_rows=META_ROW, min_cols=9)
    meta_ws.update(
        range_name=f"A{META_ROW}:I{META_ROW}",
        values=[[
            emoji,
            message,
            now.strftime("%d/%m/%Y"),
            now.strftime("%H:%M:%S"),
            "London",
            rows,
            cols,
            round(runtime, 2),
            original_file,
        ]],
        value_input_option="RAW",
    )


def month_exists_in_sheet(out_ws, mmmyy: str) -> bool:
    # Column N check
    col_vals = out_ws.col_values(MONTH_COL_ABS_1BASED)
    target = str(mmmyy).strip()
    for v in col_vals[1:]:
        if str(v).strip().lstrip("'") == target:
            return True
    return False


def insert_rows_after_header(out_ws, how_many: int):
    if how_many <= 0:
        return
    ensure_ws_size(out_ws, min_rows=out_ws.row_count + how_many, min_cols=out_ws.col_count)
    out_ws.insert_rows([[""] * out_ws.col_count] * how_many, row=2)


def force_month_col_text(out_ws):
    # Make sure column N is TEXT (prevents parsing)
    ensure_ws_size(out_ws, min_rows=2, min_cols=MONTH_COL_ABS_1BASED)
    out_ws.format("N:N", {"numberFormat": {"type": "TEXT"}})


def write_df_into_output_sheet_after_header(out_ws, df: pd.DataFrame):
    """
    Writes df starting at A2 (as requested).
    After writing, overwrites the Month yy column (N) using RAW strings to prevent
    Oct25 being auto-parsed as a date.
    """
    df = df.copy()
    df.columns = df.columns.astype(str)
    df = df.where(pd.notnull(df), "")

    values = df.values.tolist()
    if not values:
        return

    needed_rows = 1 + len(values)  # header + data
    needed_cols = max(out_ws.col_count, len(df.columns), MONTH_COL_ABS_1BASED)
    ensure_ws_size(out_ws, min_rows=needed_rows, min_cols=needed_cols)

    # 1) Write the whole block at A2
    out_ws.update(values=values, range_name="A2", value_input_option="USER_ENTERED")

    # 2) Ensure Month yy col is TEXT
    force_month_col_text(out_ws)

    # 3) Overwrite Month yy cells with RAW strings (most robust)
    if MONTH_COL_NAME in df.columns:
        month_vals = [[str(v).strip()] for v in df[MONTH_COL_NAME].tolist()]
        end_row = 1 + len(month_vals)
        out_ws.update(values=month_vals, range_name=f"N2:N{end_row}", value_input_option="RAW")


# =========================
# Data cleaning helpers
# =========================
def convert_to_numeric(df: pd.DataFrame, protect_cols=None) -> pd.DataFrame:
    out = df.copy()
    protect_cols = set(protect_cols or [])

    for col in out.columns:
        if col in protect_cols:
            continue
        if out[col].dtype != "object":
            continue

        s = out[col].astype(str).str.strip()
        s = s.replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})

        non_na = s.dropna()
        if non_na.empty:
            continue

        pct_ratio = non_na.str.contains("%", regex=False).mean()
        if pct_ratio > 0.5:
            s_num = s.str.replace("%", "", regex=False).str.replace(r"\s+", "", regex=True)
            comma_decimal_like = non_na.str.match(r"^-?\d+,\d+%?$").mean() > 0.5
            if comma_decimal_like:
                s_num = s_num.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
            out[col] = pd.to_numeric(s_num, errors="coerce") / 100
            continue

        s_clean = s.str.replace(",", "", regex=False)
        converted = pd.to_numeric(s_clean, errors="coerce")
        if converted.notna().mean() > 0.8:
            out[col] = converted

    return out


# =========================
# Web parsing (NHS page) - LATEST ONLY
# =========================
MONTH_YEAR_TEXT = re.compile(
    r"^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})$"
)

def parse_month_year(text: str):
    text = text.strip()
    if not MONTH_YEAR_TEXT.match(text):
        return None
    return datetime.strptime(text, "%B %Y")


def find_latest_commissioner_link(page_url: str):
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(page_url, headers=headers, timeout=60)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    found = []

    for h in soup.find_all(["h2", "h3"]):
        month_dt = parse_month_year(h.get_text(strip=True))
        if not month_dt:
            continue

        for sib in h.find_all_next():
            if sib.name in ("h2", "h3") and parse_month_year(sib.get_text(strip=True)):
                break

            if sib.name == "a":
                text = sib.get_text(" ", strip=True)
                m = re.match(r"^Incomplete\s+Commissioner\s+([A-Za-z]{3}\d{2})\b", text)
                if m:
                    mmmyy = m.group(1).title()
                    file_url = urljoin(page_url, sib.get("href"))
                    found.append((month_dt, mmmyy, text, file_url))
                    break

    if not found:
        raise RuntimeError("No Incomplete Commissioner files found in month sections.")

    found.sort(key=lambda x: x[0], reverse=True)
    return found[0]  # (month_dt, mmmyy, link_text, file_url)


# =========================
# File download/read
# =========================
def download_file(url: str, filename: str) -> str:
    ensure_download_dir()
    path = os.path.join(DOWNLOAD_DIR, filename)

    with requests.get(url, stream=True, timeout=180) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(1024 * 256):
                if chunk:
                    f.write(chunk)
    return path


def read_sheet(path: str, sheet_name: str) -> pd.DataFrame:
    return pd.read_excel(path, sheet_name=sheet_name, header=HEADER_ROW, engine="openpyxl")


def excel_col_to_index(col: str) -> int:
    col = col.strip().upper()
    idx = 0
    for c in col:
        idx = idx * 26 + (ord(c) - ord("A") + 1)
    return idx - 1


def keep_cols(df: pd.DataFrame, ranges) -> pd.DataFrame:
    num_cols = df.shape[1]
    keep_idx = []
    for start, end in ranges:
        s = excel_col_to_index(start)
        e = excel_col_to_index(end)
        if s >= num_cols:
            continue
        e = min(e, num_cols - 1)
        keep_idx.extend(range(s, e + 1))

    out = df.iloc[:, keep_idx].copy()
    out = out.loc[:, ~out.columns.astype(str).str.match(r"^Unnamed")]
    return out


def prepend_national_rows(main_df: pd.DataFrame, national_df: pd.DataFrame, label: str) -> pd.DataFrame:
    if national_df.empty:
        return main_df
    aligned = national_df.reindex(columns=main_df.columns)
    if len(main_df.columns) >= 2:
        aligned[main_df.columns[1]] = label
    return pd.concat([aligned, main_df], ignore_index=True)


# =========================
# MAIN
# =========================
def main():
    start = time.time()

    sh = connect_gsheets()
    meta_ws = get_or_create_ws(sh, META_SHEET_NAME, rows=1000, cols=12)
    out_ws = get_or_create_ws(sh, OUTPUT_SHEET_NAME, rows=5000, cols=250)

    # Make sure Month yy column N is TEXT
    force_month_col_text(out_ws)

    try:
        month_dt, mmmyy, link_text, file_url = find_latest_commissioner_link(PAGE_URL)

        # Check if already present in Column N
        if month_exists_in_sheet(out_ws, mmmyy):
            runtime = time.time() - start
            write_meta(
                meta_ws,
                ok=False,
                message=f"incompleteRTT-ICB: {mmmyy} already exists",
                rows=0,
                cols=0,
                runtime=runtime,
                original_file="",
            )
            print(f"Skipped: {mmmyy} already exists in {OUTPUT_SHEET_NAME}")
            return

        filename = f"Incomplete Commissioner {mmmyy}.xlsx"
        filepath = download_file(file_url, filename)

        protect = {"Region Code", "Region Name", "ICB Code", "ICB Name", TFC_COL, "Treatment Function"}

        # ---- ICB ----
        df_icb = read_sheet(filepath, ICB_SHEET_NAME)
        df_icb = keep_cols(df_icb, ICB_KEEP_COL)
        if TFC_COL not in df_icb.columns:
            raise ValueError(f"'{TFC_COL}' not found in {ICB_SHEET_NAME} after slicing.")
        df_icb = df_icb[df_icb[TFC_COL].astype(str).str.strip() == TFC_VALUE].copy()
        df_icb = convert_to_numeric(df_icb, protect_cols=protect)

        # ---- National ----
        df_nat = read_sheet(filepath, NATIONAL_SHEET_NAME)
        df_nat = keep_cols(df_nat, NAT_KEEP_COL)
        if TFC_COL not in df_nat.columns:
            raise ValueError(f"'{TFC_COL}' not found in {NATIONAL_SHEET_NAME} after slicing.")
        df_nat = df_nat[df_nat[TFC_COL].astype(str).str.strip() == TFC_VALUE].copy()
        df_nat = convert_to_numeric(df_nat, protect_cols=protect)

        # Combine
        df_out = prepend_national_rows(df_icb, df_nat, NATIONAL_LABEL)

        # Ensure Month yy lands in SHEET column N (14th column)
        # If df has fewer than 13 cols, pad with blanks so insert index 13 is valid.
        if df_out.shape[1] < MONTH_COL_0BASED:
            for i in range(df_out.shape[1], MONTH_COL_0BASED):
                df_out[f"_pad_{i}"] = ""

        df_out.insert(MONTH_COL_0BASED, MONTH_COL_NAME, str(mmmyy))

        # Add at top (after header)
        insert_rows_after_header(out_ws, len(df_out))
        write_df_into_output_sheet_after_header(out_ws, df_out)

        runtime = time.time() - start
        write_meta(
            meta_ws,
            ok=True,
            message=f"incompleteRTT-ICB: {mmmyy} fetched and added",
            rows=len(df_out),
            cols=len(df_out.columns),
            runtime=runtime,
            original_file=filename,
        )

        print(f"Added {mmmyy} to top of {OUTPUT_SHEET_NAME} ({len(df_out)} rows)")

    except Exception as e:
        runtime = time.time() - start
        write_meta(
            meta_ws,
            ok=False,
            message=f"ERROR: {type(e).__name__}: {e}",
            rows=0,
            cols=0,
            runtime=runtime,
            original_file="",
        )
        raise


if __name__ == "__main__":
    main()

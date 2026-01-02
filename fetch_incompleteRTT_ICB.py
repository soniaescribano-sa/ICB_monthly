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

META_SHEET_NAME = "meta3"
OUTPUT_SHEET_NAME = "incompleteRTT-ICB"

DOWNLOAD_DIR = os.path.abspath("./downloads")

# Excel specifics
HEADER_ROW_1_INDEXED = 14
HEADER_ROW_0_INDEXED = HEADER_ROW_1_INDEXED - 1

# Sheets in file
ICB_SHEET_NAME = "ICB"
NATIONAL_SHEET_NAME = "National"

# Filter
TFC_COL = "Treatment Function Code"
TFC_VALUE = "C_330"

# Columns to keep
# Main ICB output: A–F then DH–END
KEEP_PREFIX_END = "E"
KEEP_SUFFIX_START = "DG"

# Extra National entry: A–C then DE–DM (inclusive)
NAT_KEEP_RANGES = [("A", "C"), ("DE", "DM")]

# National label requirement: set column B values to this for the prepended rows
NATIONAL_LABEL = "NATIONAL"

MADRID_TZ = ZoneInfo("Europe/Madrid")

META_HEADERS = [
    "date",
    "time",
    "timezone",
    "status",
    "filename",
    "rows",
    "cols",
    "runtime_seconds",
]
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


def clear_and_write_worksheet(ws, df: pd.DataFrame):
    ws.clear()
    df = df.copy()
    df.columns = df.columns.astype(str)
    df = df.where(pd.notnull(df), "")
    values = [df.columns.tolist()] + df.values.tolist()
    ws.update(values, value_input_option="USER_ENTERED")


def append_meta_row(meta_ws, status: str, filename: str, nrows: int, ncols: int, runtime_seconds: float):
    now = datetime.now(MADRID_TZ)

    if not meta_ws.get_all_values():
        meta_ws.append_row(META_HEADERS, value_input_option="RAW")

    meta_ws.append_row(
        [
            now.strftime("%Y-%m-%d"),
            now.strftime("%H:%M:%S"),
            "Europe/Madrid",
            status,
            filename,
            nrows,
            ncols,
            round(runtime_seconds, 2),
        ],
        value_input_option="RAW",
    )


# ---------- Keep % columns numeric (and other numbers numeric) ----------

def coerce_numeric_for_sheets(df: pd.DataFrame, protect_cols=None) -> pd.DataFrame:
    """
    Convert numeric-looking object columns to numeric dtype BEFORE writing to Sheets,
    including percent strings like '52.0%' -> 0.52.
    """
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

        # If most values look like percent strings, convert to decimals
        pct_ratio = non_na.str.contains("%", regex=False).mean()
        if pct_ratio > 0.5:
            s_num = s.str.replace("%", "", regex=False).str.replace(r"\s+", "", regex=True)

            # Handle decimal comma if present
            comma_decimal_like = non_na.str.match(r"^-?\d+,\d+%?$").mean() > 0.5
            if comma_decimal_like:
                s_num = s_num.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)

            out[col] = pd.to_numeric(s_num, errors="coerce") / 100
            continue

        # General numeric conversion (with thousands commas)
        s_clean = s.str.replace(",", "", regex=False)
        converted = pd.to_numeric(s_clean, errors="coerce")

        # Only replace if mostly successful (avoid mangling genuine text cols)
        if converted.notna().mean() > 0.8:
            out[col] = converted

    return out


# ---------- NHS page parsing ----------

MONTH_YEAR_RE = re.compile(
    r"^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})$"
)

def parse_month_year(text: str):
    text = text.strip()
    if not MONTH_YEAR_RE.match(text):
        return None
    return datetime.strptime(text, "%B %Y")


def find_latest_incomplete_commissioner_link(page_url: str):
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
    month_dt, mmmyy, link_text, file_url = found[0]
    return month_dt.strftime("%B %Y"), mmmyy, link_text, file_url


# ---------- Download + read ----------

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
    return pd.read_excel(
        path,
        sheet_name=sheet_name,
        header=HEADER_ROW_0_INDEXED,
        engine="openpyxl",
    )


# ---------- Column slicing (Excel letters) ----------

def excel_col_to_index(col: str) -> int:
    col = col.strip().upper()
    idx = 0
    for c in col:
        idx = idx * 26 + (ord(c) - ord("A") + 1)
    return idx - 1


def keep_A_to_F_and_DH_to_end(df: pd.DataFrame) -> pd.DataFrame:
    ncols = df.shape[1]
    end_prefix = excel_col_to_index(KEEP_PREFIX_END)
    start_suffix = excel_col_to_index(KEEP_SUFFIX_START)

    prefix_idx = list(range(0, min(end_prefix + 1, ncols)))
    suffix_idx = list(range(start_suffix, ncols)) if start_suffix < ncols else []

    out = df.iloc[:, prefix_idx + suffix_idx].copy()
    out = out.loc[:, ~out.columns.astype(str).str.match(r"^Unnamed")]
    return out


def keep_ranges(df: pd.DataFrame, ranges) -> pd.DataFrame:
    ncols = df.shape[1]
    keep_idx = []
    for start, end in ranges:
        s = excel_col_to_index(start)
        e = excel_col_to_index(end)
        if s >= ncols:
            continue
        e = min(e, ncols - 1)
        keep_idx.extend(range(s, e + 1))

    if not keep_idx:
        raise ValueError(f"No columns selected for ranges={ranges}. Sheet has {ncols} cols.")

    # dedupe preserve order
    seen = set()
    keep_idx = [i for i in keep_idx if not (i in seen or seen.add(i))]

    out = df.iloc[:, keep_idx].copy()
    out = out.loc[:, ~out.columns.astype(str).str.match(r"^Unnamed")]
    return out


# ---------- Combine: prepend National rows + set column B to NATIONAL ----------

def prepend_national_rows(main_df: pd.DataFrame, national_df: pd.DataFrame, label: str) -> pd.DataFrame:
    """
    Prepend national_df rows to main_df, aligning on column names.
    Also sets *column B of the final output* to `label` for the prepended rows.
    """
    if national_df.empty:
        return main_df

    # Align to main output columns
    aligned = national_df.reindex(columns=main_df.columns)

    # Set column B (2nd column) to NATIONAL for all prepended rows
    if len(main_df.columns) >= 2:
        col_b_name = main_df.columns[1]
        aligned[col_b_name] = label

    return pd.concat([aligned, main_df], ignore_index=True)


# ---------- Main ----------

def main():
    start = time.time()

    sh = connect_gsheets()
    meta_ws = get_or_create_worksheet(sh, META_SHEET_NAME, rows=1000, cols=10)
    out_ws = get_or_create_worksheet(sh, OUTPUT_SHEET_NAME, rows=5000, cols=250)

    try:
        month_label, mmmyy, link_text, file_url = find_latest_incomplete_commissioner_link(PAGE_URL)
        filename = f"Incomplete Commissioner {mmmyy}.xlsx"
        filepath = download_file(file_url, filename)

        # ---- MAIN DATA: ICB sheet ----
        df_icb = read_sheet(filepath, ICB_SHEET_NAME)
        df_icb = keep_A_to_F_and_DH_to_end(df_icb)

        if TFC_COL not in df_icb.columns:
            raise ValueError(f"'{TFC_COL}' not found in {ICB_SHEET_NAME} after slicing.")

        df_icb = df_icb[df_icb[TFC_COL].astype(str).str.strip() == TFC_VALUE].copy()

        # Keep % columns as numeric for ICB (same behavior as before)
        protect = {"Region Code", "Region Name", "ICB Code", "ICB Name", TFC_COL, "Treatment Function"}
        df_icb = coerce_numeric_for_sheets(df_icb, protect_cols=protect)

        # ---- EXTRA TOP ENTRY: National sheet ----
        df_nat = read_sheet(filepath, NATIONAL_SHEET_NAME)
        df_nat = keep_ranges(df_nat, NAT_KEEP_RANGES)

        if TFC_COL not in df_nat.columns:
            raise ValueError(f"'{TFC_COL}' not found in {NATIONAL_SHEET_NAME} after slicing A:C + DE:DM.")

        df_nat = df_nat[df_nat[TFC_COL].astype(str).str.strip() == TFC_VALUE].copy()

        # Keep % columns numeric for National too (matching ICB handling)
        df_nat = coerce_numeric_for_sheets(df_nat, protect_cols=protect)

        # ---- Prepend National row(s) and set column B to NATIONAL ----
        df_out = prepend_national_rows(df_icb, df_nat, NATIONAL_LABEL)

        clear_and_write_worksheet(out_ws, df_out)

        runtime = time.time() - start
        append_meta_row(
            meta_ws,
            status=(
                f"OK - {month_label} - {link_text} - "
                f"ICB(C_330) + National(C_330 top, colB='{NATIONAL_LABEL}') "
                f"[ICB kept A:F & DH:END; Nat kept A:C & DE:DM]"
            ),
            filename=filename,
            nrows=len(df_out),
            ncols=len(df_out.columns),
            runtime_seconds=runtime,
        )

        print(
            f"Loaded {month_label} | Incomplete Commissioner | "
            f"ICB rows={len(df_icb)} + National rows={len(df_nat)} (prepended, colB={NATIONAL_LABEL})"
        )

    except Exception as e:
        runtime = time.time() - start
        append_meta_row(
            meta_ws,
            status=f"ERROR: {type(e).__name__}: {e}",
            filename="",
            nrows=0,
            ncols=0,
            runtime_seconds=runtime,
        )
        raise


if __name__ == "__main__":
    main()

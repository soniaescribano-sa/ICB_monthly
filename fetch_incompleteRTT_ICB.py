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


# Configuration: page to scrape, target google sheet and credentials
PAGE_URL = "https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/rtt-data-2025-26/"
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1L-22eKojGVYdSq2gPMzX3K8MC9IZVyEtAQ3s7h2jv28/edit?gid=0#gid=0"
SERVICE_ACCOUNT_JSON = "service_account.json"

# Sheet names: one for metadata one for output data (keeping naming convention from DK original excel)
META_SHEET_NAME = "meta_all"
OUTPUT_SHEET_NAME = "incompleteRTT-ICB"
DOWNLOAD_DIR = os.path.abspath("./downloads")
LONDON_TIME = ZoneInfo("Europe/London")

# ====== Section relevant to the file we are reading from ======
# Information from file to scrape: row where data starts and relevant sheet names
HEADER_ROW = 14 - 1
ICB_SHEET_NAME = "ICB"
NATIONAL_SHEET_NAME = "National"

# Define filter column and filer value
TFC_COL = "Treatment Function Code"
TFC_VALUE = "C_330"

# Columns to keep in differet pages: A-E and DG to end in ICB and A-C and DE-DM in national sheet
ICB_KEEP_COL = [("A", "E"), ("DG", "DO")]
NAT_KEEP_COL = [("A", "C"), ("DE", "DM")]



# ====== Section relevant to the file we are writing to ======
# Label for national data (add to column B)
NATIONAL_LABEL = "NATIONAL"

# Meta headers for key info in meta file
META_HEADERS = [
    "Status",
    "File",
    "Date",
    "Time",
    "Time Zone",
    "Log info",
    "Original File",
    "# Rows",
    "# Cols",
    "Run time (s)",
]


# Check that downloads folder exist, and create it if not
def ensure_download_dir():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)


# ====== Section relevant to Google Sheets and formatting ======

# Connect to Google Sheets by its url (need to load credentials from json file)
def connect_gsheets():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_url(SPREADSHEET_URL)

# Check if worksheet (ws) exists and if not create it (1000 rows and 30cols is enough?)
def get_or_create_ws(sh, title: str, rows: int = 1000, cols: int = 30):
    try:
        return sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return sh.add_ws(title=title, rows=str(rows), cols=str(cols))

# Clear sheet to avoid mixing data and write to it
def clear_and_write_ws(ws, df: pd.DataFrame):
    ws.clear()
    df = df.copy()
    df.columns = df.columns.astype(str)
    df = df.where(pd.notnull(df), "")
    values = [df.columns.tolist()] + df.values.tolist() #1st headers then actual data
    ws.update(values, value_input_option="USER_ENTERED")

# Conver column number to letter (df has numbers, spreadsheet letters)
def col_num_to_letter(n: int) -> str:
    """1 -> A, 2 -> B, 27 -> AA..."""
    s = ""
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s

# Formatting for metasheet: headers in bold + centered, values cenetered except long ones (log info + original file) and add emoji for status check
def format_meta_sheet(meta_ws, ok: bool):
    num_cols = len(META_HEADERS)
    last_col = col_num_to_letter(num_cols)

    header_range = f"A1:{last_col}1" # Write header to row 1
    values_range = f"A2:{last_col}2" # Write values to row 2

    # Headers centered + bold
    meta_ws.format(
        header_range,
        {"horizontalAlignment": "CENTER", "textFormat": {"bold": True}},
    )

    # Values centered by default, left for long ones
    meta_ws.format(
        values_range,
        {"horizontalAlignment": "CENTER"},
    )
    log_info_idx = META_HEADERS.index("Log info") + 1
    orig_file_idx = META_HEADERS.index("Original File") + 1
    meta_ws.format(f"{col_num_to_letter(log_info_idx)}2", {"horizontalAlignment": "LEFT"})
    meta_ws.format(f"{col_num_to_letter(orig_file_idx)}2", {"horizontalAlignment": "LEFT"})

    # Add a cell colour for check: cell A2
    fetching_cell = "A2"
    bg = {"red": 0.85, "green": 0.95, "blue": 0.85} if ok else {"red": 0.98, "green": 0.85, "blue": 0.85}
    meta_ws.format(
        fetching_cell,
        {
            "horizontalAlignment": "CENTER",
            "backgroundColor": bg,
        },
    )

# Write metadata to meta_ws
def append_meta_row(meta_ws, status: str, filename: str, num_rows: int, num_cols: int, runtime_seconds: float):
    # Select emoji based on status prefix (OK or somwthing else)
    ok = status.startswith("OK")
    check_emoji = "✅" if ok else "❌"

    # Ensure the sheet is large enough to write into row 5 and has enough columns for all metadata to be there
    rows_meta = 5
    cols_meta = max(meta_ws.col_count, len(META_HEADERS))
    if meta_ws.row_count < rows_meta or meta_ws.col_count < cols_meta:
        meta_ws.resize(rows=max(meta_ws.row_count, rows_meta), cols=cols_meta)
    last_col_letter = col_num_to_letter(len(META_HEADERS))

    # Row 1: headers
    meta_ws.update(
        f"A1:{last_col_letter}1",
        [META_HEADERS],
        value_input_option="RAW",
    )

    # Row 2: values
    now = datetime.now(LONDON_TIME)
    meta_ws.update(
        f"A2:{last_col_letter}5",
        [[
            check_emoji,
            OUTPUT_SHEET_NAME,
            now.strftime("%d/%m/%Y"),
            now.strftime("%H:%M:%S"),
            "London",
            status,
            filename,
            num_rows,
            num_cols,
            round(runtime_seconds, 2),
        ]],
        value_input_option="RAW",
    )

    # Apply formatting
    format_meta_sheet(meta_ws, ok=ok)

# Convert numbers and % to an actual numeric type, not just text (otherwise excel operations don't work)
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

        # Only replace if mostly successful (avoid genuine text cols)
        if converted.notna().mean() > 0.8:
            out[col] = converted

    return out


# ====== Section for parsing data from NHS webpage ======

# Check possible dates combination, this webpage has format October 2025
MONTH_YEAR_TEXT = re.compile(r"^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})$")

def parse_month_year(text: str):
    text = text.strip()
    if not MONTH_YEAR_TEXT.match(text):
        return None
    return datetime.strptime(text, "%B %Y")

# Download the webpage and parse it to find link
def find_commissioner_link(page_url: str):
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


# Download excel file
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

# Read excel file downloaded it
def read_sheet(path: str, sheet_name: str) -> pd.DataFrame:
    return pd.read_excel(
        path,
        sheet_name=sheet_name,
        header=HEADER_ROW,
        engine="openpyxl",
    )


# Convert letter to index + specify which columns to keep

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

    if not keep_idx:
        raise ValueError(f"No columns selected for ranges={ranges}. Sheet has {num_cols} cols.")

    # dedupe preserve order
    seen = set()
    keep_idx = [i for i in keep_idx if not (i in seen or seen.add(i))]

    out = df.iloc[:, keep_idx].copy()
    out = out.loc[:, ~out.columns.astype(str).str.match(r"^Unnamed")]
    return out


# Move national rows at the top (to keep consistency with DKs format)
def prepend_national_rows(main_df: pd.DataFrame, national_df: pd.DataFrame, label: str) -> pd.DataFrame:
    if national_df.empty:
        return main_df

    # Align to main output columns
    aligned = national_df.reindex(columns=main_df.columns)

    # Set column B (2nd column) to NATIONAL for all prepended rows
    if len(main_df.columns) >= 2:
        col_b_name = main_df.columns[1]
        aligned[col_b_name] = label

    return pd.concat([aligned, main_df], ignore_index=True)


# ====== MAIN section of the code ======

def main():
    start = time.time()

    sh = connect_gsheets()
    meta_ws = get_or_create_ws(sh, META_SHEET_NAME, rows=1000, cols=20)
    out_ws = get_or_create_ws(sh, OUTPUT_SHEET_NAME, rows=5000, cols=250)

    try:
        month_label, mmmyy, link_text, file_url = find_commissioner_link(PAGE_URL)
        filename = f"Incomplete Commissioner {mmmyy}.xlsx"
        filepath = download_file(file_url, filename)

        # Process ICB datasheet
        df_icb = read_sheet(filepath, ICB_SHEET_NAME)
        df_icb = keep_cols(df_icb, ICB_KEEP_COL)

        # Filter data
        if TFC_COL not in df_icb.columns:
            raise ValueError(f"'{TFC_COL}' not found in {ICB_SHEET_NAME} after slicing.")
        df_icb = df_icb[df_icb[TFC_COL].astype(str).str.strip() == TFC_VALUE].copy()

        # Keep % columns as numeric
        protect = {"Region Code", "Region Name", "ICB Code", "ICB Name", TFC_COL, "Treatment Function"}
        df_icb = convert_to_numeric(df_icb, protect_cols=protect)

        # National sheet
        df_nat = read_sheet(filepath, NATIONAL_SHEET_NAME)
        df_nat = keep_cols(df_nat, NAT_KEEP_COL)

        if TFC_COL not in df_nat.columns:
            raise ValueError(f"'{TFC_COL}' not found in {NATIONAL_SHEET_NAME} after slicing A:C + DE:DM.")
        df_nat = df_nat[df_nat[TFC_COL].astype(str).str.strip() == TFC_VALUE].copy()

        # Keep % columns numeric for National too and prepend national rows
        df_nat = convert_to_numeric(df_nat, protect_cols=protect)
        df_out = prepend_national_rows(df_icb, df_nat, NATIONAL_LABEL)

        clear_and_write_ws(out_ws, df_out)

        runtime = time.time() - start

        append_meta_row(
            meta_ws,
            status=(
                f"OK - {month_label} - {link_text} - "
                f"ICB(C_330) + National(C_330 top, colB='{NATIONAL_LABEL}') "
                f"[ICB kept A:F & DH:END; Nat kept A:C & DE:DM]"
            ),
            filename=filename,
            num_rows=len(df_out),
            num_cols=len(df_out.columns),
            runtime_seconds=runtime,
        )

        print(
            f"Loaded {month_label} | Incomplete Commissioner | "
            f"ICB rows={len(df_icb)} + National rows={len(df_nat)}"
        )

    except Exception as e:
        runtime = time.time() - start
        append_meta_row(
            meta_ws,
            status=f"ERROR: {type(e).__name__}: {e}",
            filename="",
            num_rows=0,
            num_cols=0,
            runtime_seconds=runtime,
        )
        raise


if __name__ == "__main__":
    main()

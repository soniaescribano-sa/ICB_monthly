import warnings
warnings.filterwarnings("ignore", message="urllib3 v2 only supports OpenSSL.*")

import os
import re
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

import gspread
from google.oauth2.service_account import Credentials


# ====== CONFIG ======
PAGE_URL = "https://www.england.nhs.uk/statistics/statistical-work-areas/cancer-waiting-times/monthly-data-and-summaries/2025-26-monthly-cancer-waiting-times-statistics/"
SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1L-22eKojGVYdSq2gPMzX3K8MC9IZVyEtAQ3s7h2jv28/edit?gid=0#gid=0"
SERVICE_ACCOUNT_JSON = "service_account.json"

META_SHEET_NAME = "meta_all"
META_WRITE_ROW = 14  # write meta VALUES here (no headers)

OUTPUT_SHEET_NAME = "CWT-ICB-all"
DOWNLOAD_DIR = os.path.abspath("./downloads")

# Filtering: keep rows where this column contains "skin" (case-insensitive)
FILTER_COLUMN = "Cancer Type"
FILTER_PATTERN = r"skin"

# Workbook structure (headers on row 15, 1-indexed)
HEADER_ROW_NUMBER = 15
SKIPROWS_BEFORE_HEADER = HEADER_ROW_NUMBER - 1
DEFAULT_EXCEL_SHEET = "System Level Performance"

LONDON_TZ = ZoneInfo("Europe/London")

# Month column requirements (Google Sheets)
MONTH_COL_NAME = "Month yy"
MONTH_COL_LETTER = "X"          # must be column X
MONTH_COL_INDEX_1BASED = 24     # X = 24th column
# =====================


# ---------- Helpers ----------
MONTH_YEAR_HEADING_RE = re.compile(
    r"^(January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d{2})$",
    re.IGNORECASE,
)

MONTH_TO_NUM = {
    "January": 1, "February": 2, "March": 3, "April": 4,
    "May": 5, "June": 6, "July": 7, "August": 8,
    "September": 9, "October": 10, "November": 11, "December": 12,
}


def ensure_download_dir():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)


def connect_gsheets():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON, scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_url(SPREADSHEET_URL)


def get_or_create_worksheet(sh, title: str, rows: int = 200, cols: int = 30):
    try:
        return sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=str(rows), cols=str(cols))


def ensure_ws_size(ws, min_rows: int, min_cols: int):
    if ws.row_count < min_rows or ws.col_count < min_cols:
        ws.resize(rows=max(ws.row_count, min_rows), cols=max(ws.col_count, min_cols))


def month_year_to_mmmyy(month_year: str) -> str:
    """
    Input: 'October 2025'
    Output: 'Oct25'
    """
    month, year = month_year.split()[:2]
    mon = month[:3].capitalize()
    yy = str(year)[-2:]
    return f"{mon}{yy}"


def pick_latest_month_year(month_years: list[tuple[str, int]]) -> tuple[str, int]:
    """
    Return (month_name, year_int) for the latest.
    """
    return max(month_years, key=lambda x: (x[1], MONTH_TO_NUM.get(x[0], 0)))


def read_downloaded_file(filepath: str) -> pd.DataFrame:
    """
    Reads downloaded file with headers on row 15 (1-indexed).
    For pandas: skiprows=14 makes row 15 the header row.
    """
    lower = filepath.lower()

    if lower.endswith(".csv"):
        return pd.read_csv(filepath, skiprows=SKIPROWS_BEFORE_HEADER)
    if lower.endswith(".tsv") or lower.endswith(".txt"):
        return pd.read_csv(filepath, sep="\t", skiprows=SKIPROWS_BEFORE_HEADER)
    if lower.endswith(".xlsx") or lower.endswith(".xls"):
        return pd.read_excel(filepath, sheet_name=DEFAULT_EXCEL_SHEET, skiprows=SKIPROWS_BEFORE_HEADER)

    if lower.endswith(".zip"):
        import zipfile
        with zipfile.ZipFile(filepath, "r") as z:
            names = z.namelist()
            candidate = next((n for n in names if n.lower().endswith(".csv")), None) or next(
                (n for n in names if n.lower().endswith((".xlsx", ".xls"))), None
            )
            if not candidate:
                raise ValueError(f"ZIP contains files, but no csv/xlsx found: {names}")
            extracted_path = z.extract(candidate, DOWNLOAD_DIR)
            return read_downloaded_file(extracted_path)

    raise ValueError(f"Unsupported file type: {filepath}")


def drop_unnamed_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[:, ~df.columns.astype(str).str.match(r"^Unnamed")]


def accept_cookies_if_present(page):
    candidates = [
        re.compile(r"accept all", re.I),
        re.compile(r"accept", re.I),
        re.compile(r"agree", re.I),
        re.compile(r"allow all", re.I),
        re.compile(r"ok", re.I),
    ]

    for pat in candidates:
        try:
            btn = page.get_by_role("button", name=pat)
            if btn.count() > 0 and btn.first.is_visible():
                btn.first.click(timeout=1500)
                page.wait_for_timeout(800)
                return
        except Exception:
            pass

    for pat in candidates:
        try:
            lnk = page.get_by_role("link", name=pat)
            if lnk.count() > 0 and lnk.first.is_visible():
                lnk.first.click(timeout=1500)
                page.wait_for_timeout(800)
                return
        except Exception:
            pass


def find_all_month_years_from_headings(page) -> list[tuple[str, int]]:
    selectors = [
        "main h2, main h3, main h4",
        "article h2, article h3, article h4",
        "h2, h3, h4",
    ]

    found = []
    seen = set()

    for sel in selectors:
        texts = page.locator(sel).all_text_contents()
        for t in texts:
            t = (t or "").strip()
            m = MONTH_YEAR_HEADING_RE.match(t)
            if m:
                month = m.group(1).capitalize()
                year = int(m.group(2))
                key = (month, year)
                if key not in seen:
                    seen.add(key)
                    found.append(key)

        if found:
            break

    if not found:
        raise RuntimeError("Could not find any Month YYYY heading (e.g., 'October 2025') on the page.")

    found.sort(key=lambda x: (x[1], MONTH_TO_NUM.get(x[0], 0)))
    return found


def click_and_download_workbook(page, month: str, year: int) -> tuple[str, str]:
    """
    Downloads the '<Month> <Year> Monthly Combined Workbook Provisional' file from the page.
    Returns (downloaded_filepath, link_text_clicked)
    """
    link_text = f"{month} {year} Monthly Combined Workbook Provisional"

    link_locator = page.get_by_role(
        "link",
        name=re.compile(rf"^\s*{re.escape(link_text)}\s*$", re.IGNORECASE),
    )

    if link_locator.count() == 0:
        link_locator = page.locator(f'a:has-text("{link_text}")')

    if link_locator.count() == 0:
        link_re = re.compile(
            rf"{month}\s+{year}.*Monthly\s+Combined\s+Workbook.*Provisional",
            re.IGNORECASE,
        )
        link_locator = page.get_by_role("link", name=link_re)

    if link_locator.count() == 0:
        raise RuntimeError(f"Could not find the workbook link for '{month} {year}'.")

    ensure_download_dir()
    try:
        with page.expect_download(timeout=180_000) as download_info:
            link_locator.first.click()
        download = download_info.value
    except PlaywrightTimeoutError:
        raise RuntimeError(f"Timed out waiting for the download after clicking '{link_text}'.")

    suggested = download.suggested_filename or f"{month}_{year}_Monthly_Combined_Workbook_Provisional.xlsx"
    safe_name = re.sub(r"[^\w\-.() ]+", "_", suggested)
    final_path = os.path.join(DOWNLOAD_DIR, safe_name)
    download.save_as(final_path)

    return final_path, link_text


def fetch_latest_workbook_with_playwright() -> tuple[str, str, str]:
    """
    Downloads ONLY the latest month workbook found on the page.
    Returns: (filepath, link_text_clicked, month_year_string like "October 2025")
    """
    ensure_download_dir()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        page.goto(PAGE_URL, wait_until="domcontentloaded")
        page.wait_for_timeout(2500)

        accept_cookies_if_present(page)

        month_years = find_all_month_years_from_headings(page)
        latest_month, latest_year = pick_latest_month_year(month_years)

        filepath, link_text = click_and_download_workbook(page, latest_month, latest_year)

        context.close()
        browser.close()

    return filepath, link_text, f"{latest_month} {latest_year}"


def filter_skin_rows(df: pd.DataFrame) -> pd.DataFrame:
    if FILTER_COLUMN not in df.columns:
        raise ValueError(
            f"Missing required column '{FILTER_COLUMN}'. Columns found: {list(df.columns)}"
        )
    mask = df[FILTER_COLUMN].astype(str).str.contains(FILTER_PATTERN, case=False, na=False, regex=True)
    return df.loc[mask].copy()


def sort_cwt_data(
    df: pd.DataFrame,
    org_col: str = "Org Code",
    cancer_col: str = "Cancer Type",
    referral_col: str = "Referral Route/Stage",
    total_label: str = "Total",
) -> pd.DataFrame:
    required = {org_col, cancer_col, referral_col}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    out = df.copy()

    out["_org_norm"] = out[org_col].astype(str).str.strip().str.casefold()
    out["_cancer_norm"] = out[cancer_col].astype(str).str.strip().str.casefold()
    out["_referral_norm"] = out[referral_col].astype(str).str.strip().str.casefold()

    out["_is_total"] = out["_org_norm"].eq(total_label.casefold())

    out = out.sort_values(
        by=["_is_total", "_org_norm", "_cancer_norm", "_referral_norm"],
        ascending=[False, True, True, True],
        kind="stable",
        na_position="last",
    )

    return out.drop(columns=["_is_total", "_org_norm", "_cancer_norm", "_referral_norm"])


def force_month_column_text(ws):
    # IMPORTANT: format BEFORE writing values, and ensure column exists
    ensure_ws_size(ws, min_rows=2, min_cols=MONTH_COL_INDEX_1BASED)
    ws.format(f"{MONTH_COL_LETTER}:{MONTH_COL_LETTER}", {"numberFormat": {"type": "TEXT"}})


def month_exists_in_sheet_column_x(ws, mmmyy: str) -> bool:
    """
    Checks if mmmyy exists anywhere in column X (excluding header row).
    Robust to accidental leading apostrophes/spaces.
    """
    ensure_ws_size(ws, min_rows=1, min_cols=MONTH_COL_INDEX_1BASED)
    col_vals = ws.col_values(MONTH_COL_INDEX_1BASED)  # X
    for v in col_vals[1:]:
        if str(v).strip().lstrip("'") == mmmyy:
            return True
    return False


def ensure_header_row(ws, df_columns: list[str]):
    """
    Ensure we have headers in row 1.
    We write df headers into A1.., and ensure X1 is 'Month yy'.
    (No shifting: data will always start at A2.)
    """
    # Need enough columns for df + X
    needed_cols = max(len(df_columns), MONTH_COL_INDEX_1BASED)
    ensure_ws_size(ws, min_rows=2, min_cols=needed_cols)

    existing = ws.get("A1:A1")
    has_header = bool(existing and existing[0] and any(str(x).strip() for x in existing[0]))
    if has_header:
        return

    # Build header row up to needed_cols
    header = [""] * needed_cols
    for i, name in enumerate(df_columns):
        header[i] = str(name)

    header[MONTH_COL_INDEX_1BASED - 1] = MONTH_COL_NAME  # X1

    ws.update(values=[header], range_name="A1", value_input_option="RAW")


def insert_rows_after_header(ws, how_many: int):
    if how_many <= 0:
        return
    ensure_ws_size(ws, min_rows=2, min_cols=max(ws.col_count, MONTH_COL_INDEX_1BASED))
    ws.insert_rows([[""] * ws.col_count] * how_many, row=2)


def write_df_to_top_starting_A2(ws, df: pd.DataFrame, mmmyy: str):
    """
    Inserts rows after header and writes df values starting A2 (no header).
    Writes Month yy to X as TEXT using RAW (critical).
    """
    df = df.copy()
    df.columns = df.columns.astype(str)
    df = df.where(pd.notnull(df), "")

    # Guard: if df already has >= 24 columns, writing Month into X would overwrite real data
    if df.shape[1] >= MONTH_COL_INDEX_1BASED:
        raise ValueError(
            f"Dataframe has {df.shape[1]} columns (>=24). "
            f"Cannot safely force Month yy into column X without overwriting existing data."
        )

    # Ensure month col is TEXT BEFORE writing
    force_month_column_text(ws)

    # Ensure header exists (row 1)
    ensure_header_row(ws, df.columns.tolist())

    # Insert rows for new data
    insert_rows_after_header(ws, len(df))

    # Ensure grid is large enough
    needed_rows = 1 + len(df)
    needed_cols = max(ws.col_count, len(df.columns), MONTH_COL_INDEX_1BASED)
    ensure_ws_size(ws, min_rows=needed_rows, min_cols=needed_cols)

    # Write data values to A2
    ws.update(values=df.values.tolist(), range_name="A2", value_input_option="USER_ENTERED")

    # Write month values to X2:X... as RAW strings (prevents coercion)
    end_row = 1 + len(df)
    ws.update(
        values=[[mmmyy]] * len(df),
        range_name=f"{MONTH_COL_LETTER}2:{MONTH_COL_LETTER}{end_row}",
        value_input_option="RAW",
    )


def write_meta_row_14(meta_ws, status_emoji: str, update_msg: str, nrows: int, ncols: int, runtime_seconds: float):
    """
    Writes meta to row 14 in this order (8 cols):
    Status | Update | Date | Time | Time Zone | # Rows | # Cols | Run time (s)
    """
    ensure_ws_size(meta_ws, min_rows=META_WRITE_ROW, min_cols=8)
    now = datetime.now(LONDON_TZ)

    values = [[
        status_emoji,
        update_msg,
        now.strftime("%d/%m/%Y"),
        now.strftime("%H:%M:%S"),
        "London",
        nrows,
        ncols,
        round(runtime_seconds, 2),
    ]]

    meta_ws.update(
        range_name=f"A{META_WRITE_ROW}:H{META_WRITE_ROW}",
        values=values,
        value_input_option="RAW",
    )


def main():
    start_time = time.time()

    sh = connect_gsheets()
    meta_ws = get_or_create_worksheet(sh, META_SHEET_NAME, rows=200, cols=12)
    out_ws = get_or_create_worksheet(sh, OUTPUT_SHEET_NAME, rows=5000, cols=80)

    filepath = ""
    try:
        # 1) Fetch ONLY latest workbook
        filepath, link_text, latest_month_year = fetch_latest_workbook_with_playwright()
        mmmyy = month_year_to_mmmyy(latest_month_year)  # e.g., Oct25

        # 2) Ensure Month column X is TEXT (critical)
        force_month_column_text(out_ws)

        # 3) Check if latest month already exists in column X
        if month_exists_in_sheet_column_x(out_ws, mmmyy):
            runtime = time.time() - start_time
            write_meta_row_14(
                meta_ws,
                status_emoji="⚠️",
                update_msg=f"CWT-ICB {mmmyy} already fetched",
                nrows=0,
                ncols=0,
                runtime_seconds=runtime,
            )
            print(f"Skipped: {mmmyy} already exists in {OUTPUT_SHEET_NAME}")
            return

        # 4) Read + filter + sort latest workbook
        df = read_downloaded_file(filepath)
        df = drop_unnamed_columns(df)
        df_filtered = filter_skin_rows(df)

        org_col = "Org Code" if "Org Code" in df_filtered.columns else ("Code" if "Code" in df_filtered.columns else None)
        if not org_col:
            raise ValueError("Could not find an Org column ('Org Code' or 'Code') in the workbook.")

        df_filtered = sort_cwt_data(
            df_filtered,
            org_col=org_col,
            cancer_col="Cancer Type",
            referral_col="Referral Route/Stage",
        )

        # 5) Add to TOP of sheet, writing data starting at A2, and Month yy to X as RAW text
        write_df_to_top_starting_A2(out_ws, df_filtered, mmmyy)

        runtime = time.time() - start_time
        write_meta_row_14(
            meta_ws,
            status_emoji="✅",
            update_msg=f"Added latest CWT-ICB month: {mmmyy}",
            nrows=len(df_filtered),
            ncols=len(df_filtered.columns),
            runtime_seconds=runtime,
        )

        print(f"Added {mmmyy} from {latest_month_year} -> {OUTPUT_SHEET_NAME}")
        print(f"Downloaded: {filepath}")
        print(f"Link clicked: {link_text}")
        print(f"Rows: input={len(df)}, filtered={len(df_filtered)}")

    except Exception as e:
        runtime = time.time() - start_time
        try:
            write_meta_row_14(
                meta_ws,
                status_emoji="❌",
                update_msg=f"ERROR: {type(e).__name__}: {e}",
                nrows=0,
                ncols=0,
                runtime_seconds=runtime,
            )
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()

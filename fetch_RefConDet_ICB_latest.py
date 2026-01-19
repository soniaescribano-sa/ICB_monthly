import warnings
warnings.filterwarnings("ignore", message="urllib3 v2 only supports OpenSSL.*")

import os
import re
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Optional, List

import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

import gspread
from google.oauth2.service_account import Credentials


# ====== CONFIG ======
SHINY_URL = "https://nhsd-ndrs.shinyapps.io/cwt_referral_conversion_detection/"

SPREADSHEET_URL = "https://docs.google.com/spreadsheets/d/1L-22eKojGVYdSq2gPMzX3K8MC9IZVyEtAQ3s7h2jv28/edit?gid=0#gid=0"
FALLBACK_SPREADSHEET_TITLE = "RefConDet ICB all (auto-created)"

SERVICE_ACCOUNT_JSON = "/Users/sonia.rodriguez/Documents/ICB_Performance_Risks/shiny-to-sheets-482011-38125cda0263.json"

META_SHEET_NAME = "meta_all"
FILTER_SHEET_NAME = "RefConDet-ICB-all"

DOWNLOAD_DIR = os.path.abspath("./downloads")

TAB_TEXT = "(2) Geographical variation"
DOWNLOAD_BUTTON_TEXT = "Download data all geographies and suspected referral types or cancer sites"

LONDON_TZ = ZoneInfo("Europe/London")

META_HEADERS = [
    "Status",
    "File(s) Status",
    "Date",
    "Time",
    "Time Zone",
    "# Rows",
    "# Cols",
    "Run time (s)"
]
# =====================


def ensure_download_dir():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)


def connect_gsheets_client() -> gspread.Client:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON, scopes=scopes)
    return gspread.authorize(creds)


def open_or_create_spreadsheet(gc: gspread.Client):
    try:
        sh = gc.open_by_url(SPREADSHEET_URL)
        print(f"Opened existing spreadsheet: {sh.title}")
        print(f"Spreadsheet URL: {sh.url}")
        return sh
    except Exception as e:
        print(f"Could not open spreadsheet URL. Reason: {type(e).__name__}: {e}")
        print("Creating a new spreadsheet instead...")
        sh = gc.create(FALLBACK_SPREADSHEET_TITLE)
        print(f"Created new spreadsheet: {sh.title}")
        print(f"Spreadsheet URL: {sh.url}")
        return sh


def get_or_create_worksheet(sh, title: str, rows: int = 1000, cols: int = 26):
    try:
        return sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=str(rows), cols=str(cols))


def ensure_ws_size(ws, min_rows: int, min_cols: int):
    rows = ws.row_count
    cols = ws.col_count
    if rows < min_rows or cols < min_cols:
        ws.resize(rows=max(rows, min_rows), cols=max(cols, min_cols))


def write_meta_row(
    meta_ws,
    row_number: int,
    ok_emoji: str,
    file_status: str = "",
    nrows: int = 0,
    ncols: int = 0,
    runtime_seconds: float = 0.0,
):
    """
    Writes metadata into meta sheet for the given row_number, columns A:I.
    Ensures headers exist in row 3.
    """
    ensure_ws_size(meta_ws, min_rows=max(3, row_number), min_cols=9)

    existing_headers = meta_ws.get("A3:I3")
    if not existing_headers or not existing_headers[0] or all(v == "" for v in existing_headers[0]):
        meta_ws.update(values=[META_HEADERS], range_name="A3:I3", value_input_option="RAW")

    now = datetime.now(LONDON_TZ)

    row_values = [
        ok_emoji,                       # Status
        file_status,                    # File(s) Status
        now.strftime("%d/%m/%Y"),       # Date
        now.strftime("%H:%M:%S"),       # Time
        "Europe/London",                # Time Zone
        nrows,                          # # Rows
        ncols,                          # # Cols
        round(runtime_seconds, 2),      # Run time (s)
    ]

    meta_ws.update(
        values=[row_values],
        range_name=f"A{row_number}:I{row_number}",
        value_input_option="USER_ENTERED",
    )


def read_downloaded_file(filepath: str) -> pd.DataFrame:
    lower = filepath.lower()
    if lower.endswith(".csv"):
        return pd.read_csv(filepath)
    if lower.endswith(".xlsx") or lower.endswith(".xls"):
        return pd.read_excel(filepath)
    if lower.endswith(".tsv") or lower.endswith(".txt"):
        return pd.read_csv(filepath, sep="\t")
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


def fetch_file_with_playwright() -> str:
    ensure_download_dir()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        page.goto(SHINY_URL, wait_until="domcontentloaded")
        page.wait_for_timeout(5000)

        page.get_by_role("link", name=TAB_TEXT).click()
        page.wait_for_timeout(2000)

        try:
            with page.expect_download(timeout=180_000) as download_info:
                dl_link = page.get_by_role("link", name=re.compile(DOWNLOAD_BUTTON_TEXT, re.I))
                if dl_link.count() > 0:
                    dl_link.first.click()
                else:
                    dl_button = page.get_by_role("button", name=re.compile(DOWNLOAD_BUTTON_TEXT, re.I))
                    dl_button.first.click()
            download = download_info.value
        except PlaywrightTimeoutError:
            raise RuntimeError("Timed out waiting for the download.")

        suggested = download.suggested_filename
        safe_name = re.sub(r"[^\w\-.() ]+", "_", suggested)
        final_path = os.path.join(DOWNLOAD_DIR, safe_name)
        download.save_as(final_path)

        context.close()
        browser.close()

        return final_path


def filter_input_data(df: pd.DataFrame) -> pd.DataFrame:
    required = {"GEOG_LEVEL", "CANCER_SITE", "FINAN_YEAR", "AREACODE"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns for filtering: {sorted(missing)}")

    out = df[
        (
            (df["GEOG_LEVEL"].astype(str).str.strip() == "Integrated Care Board")
            | (df["GEOG_LEVEL"].astype(str).str.strip() == "National")
        )
        & (df["CANCER_SITE"].astype(str).str.strip() == "Skin")
    ].copy()

    # Sort alphabetically by first col, but push National to the end
    geog = out["GEOG_LEVEL"].astype(str).str.strip()
    is_national = geog.eq("National")

    non_national = out.loc[~is_national].sort_values(by=out.columns[0], kind="stable")
    national = out.loc[is_national]

    out = pd.concat([non_national, national], ignore_index=True)
    return out


def coerce_numeric_for_sheets(df: pd.DataFrame, protect_cols=None) -> pd.DataFrame:
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

            comma_decimal_like = non_na.str.match(r"^-?\d+,\d+$").mean() > 0.5
            if comma_decimal_like:
                s_num = s_num.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)

            out[col] = pd.to_numeric(s_num, errors="coerce") / 100
            continue

        comma_decimal_like = non_na.str.match(r"^-?\d+,\d+$").mean() > 0.5
        if comma_decimal_like:
            s_clean = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
            converted = pd.to_numeric(s_clean, errors="coerce")
        else:
            s_clean = s.str.replace(",", "", regex=False)
            converted = pd.to_numeric(s_clean, errors="coerce")

        success_ratio = converted.notna().mean()
        if success_ratio > 0.8:
            out[col] = converted

    return out


def parse_finan_year_to_start_year(series: pd.Series) -> pd.Series:
    """
    Convert strings like '2023/24' -> 2023 (int). Non-matching -> NA.
    """
    s = series.astype(str).str.strip()
    start = s.str.extract(r"^(\d{4})/(\d{2})$")[0]
    return pd.to_numeric(start, errors="coerce")


def get_latest_finan_year_on_sheet(ws) -> Optional[int]:
    """
    Reads existing sheet values, finds FINAN_YEAR column and returns latest start year as int.
    Returns None if sheet empty/no FINAN_YEAR found.
    """
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return None

    header = values[0]
    try:
        idx = header.index("FINAN_YEAR")
    except ValueError:
        return None

    col_vals = [row[idx] for row in values[1:] if len(row) > idx and row[idx]]
    if not col_vals:
        return None

    years = parse_finan_year_to_start_year(pd.Series(col_vals)).dropna()
    if years.empty:
        return None
    return int(years.max())


def insert_rows_below_header(ws, rows_values: List[List], header_cols: int):
    """
    Inserts rows directly below the header (row 1).
    rows_values should NOT include headers.
    """
    if not rows_values:
        return

    padded = [
        r + [""] * (header_cols - len(r)) if len(r) < header_cols else r[:header_cols]
        for r in rows_values
    ]
    ws.insert_rows(padded, row=2, value_input_option="USER_ENTERED")


def main():
    start_time = time.time()

    gc = connect_gsheets_client()
    sh = open_or_create_spreadsheet(gc)

    meta_ws = get_or_create_worksheet(sh, META_SHEET_NAME, rows=20, cols=12)
    filter_ws = get_or_create_worksheet(sh, FILTER_SHEET_NAME, rows=2000, cols=80)

    filepath = ""
    try:
        filepath = fetch_file_with_playwright()
        df = read_downloaded_file(filepath)

        df_filtered = filter_input_data(df)
        df_filtered = coerce_numeric_for_sheets(
            df_filtered,
            protect_cols={"AREACODE", "GEOG_LEVEL", "CANCER_SITE", "FINAN_YEAR"},
        )

        # --- Check existing sheet latest FINAN_YEAR ---
        latest_start_year = get_latest_finan_year_on_sheet(filter_ws)

        # --- Determine "new" data (FINAN_YEAR > latest on sheet) ---
        df_filtered["_FINAN_START_YEAR"] = parse_finan_year_to_start_year(df_filtered["FINAN_YEAR"])
        if latest_start_year is None:
            df_new = df_filtered.copy()
        else:
            df_new = df_filtered.loc[df_filtered["_FINAN_START_YEAR"] > latest_start_year].copy()

        df_filtered = df_filtered.drop(columns=["_FINAN_START_YEAR"], errors="ignore")
        df_new = df_new.drop(columns=["_FINAN_START_YEAR"], errors="ignore")

        runtime = time.time() - start_time

        existing = filter_ws.get_all_values()
        sheet_empty = (
            (not existing)
            or (len(existing) == 0)
            or (len(existing) == 1 and all(v == "" for v in existing[0]))
        )

        if sheet_empty:
            # Write full dataset (headers + data)
            df_out = df_filtered.where(pd.notnull(df_filtered), "")
            values = [df_out.columns.astype(str).tolist()] + df_out.values.tolist()
            ensure_ws_size(filter_ws, min_rows=len(values), min_cols=len(values[0]) if values else 1)
            filter_ws.clear()
            filter_ws.update(values=values, range_name="A1", value_input_option="USER_ENTERED")

            write_meta_row(
                meta_ws,
                row_number=12,
                ok_emoji="✅",
                file_status="Sheet was empty — wrote full dataset",
                nrows=len(df_out),
                ncols=len(df_out.columns),
                runtime_seconds=runtime,
            )

            print("Done ✅ (initial write)")
            return

        if df_new.empty:
            # No new data
            write_meta_row(
                meta_ws,
                row_number=12,
                ok_emoji="⚠️",
                file_status=f"{FILTER_SHEET_NAME} {latest_start_year}/{latest_start_year+1} already fetched",
                nrows=0,
                ncols=len(df_filtered.columns),
                runtime_seconds=runtime,
            )
            print("No new data ⚠️")
            return

        # Insert new rows under header
        header = existing[0]
        header_cols = len(header)

        # Keep column order consistent with existing headers if possible
        if set(header).issubset(set(df_new.columns)):
            df_new_ordered = df_new[header].copy()
        else:
            df_new_ordered = df_new.copy()

        df_new_ordered = df_new_ordered.where(pd.notnull(df_new_ordered), "")
        insert_rows_below_header(filter_ws, df_new_ordered.values.tolist(), header_cols=header_cols)

        write_meta_row(
            meta_ws,
            row_number=12,
            ok_emoji="✅",
            file_status=f"{FILTER_SHEET_NAME} added  {latest_start_year}/{latest_start_year+1} data",
            nrows=len(df_new_ordered),
            ncols=len(df_new_ordered.columns),
            runtime_seconds=runtime,
        )

        print(f"Inserted {len(df_new_ordered)} new rows ✅")

    except Exception as e:
        runtime = time.time() - start_time
        try:
            write_meta_row(
                meta_ws,
                row_number=12,
                ok_emoji="❌",
                file_status=f"{type(e).__name__}: {e}",
               # original_file=os.path.basename(filepath) if filepath else "",
                nrows=0,
                ncols=0,
                runtime_seconds=runtime,
            )
        except Exception as meta_err:
            print(f"Also failed writing meta sheet: {type(meta_err).__name__}: {meta_err}")
        raise


if __name__ == "__main__":
    main()

from __future__ import annotations
import json
import time
import datetime
import threading
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build as google_build

from app.utils.cache import get_parquet_cache, ParquetCache
from app.utils.logger import get_logger
from app.config import get_settings

logger = get_logger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
]

NUMERIC = "numeric"
TEXT    = "text"


class SheetsService:

    def __init__(self):
        self._settings     = get_settings()
        self._cache        = get_parquet_cache(self._settings.cache_dir)
        self._gspread:     gspread.Client | None = None
        self._drive        = None          # Drive API client
        self._fetch_lock   = threading.Lock()  # prevent concurrent Sheets fetches

        # Boot: load Parquet cache into memory so first query is fast
        self._cache.load_from_disk()

    # ──────────────────────────────────────────────────────────────────────────
    # Public API  (called by query engine + orchestrator)
    # ──────────────────────────────────────────────────────────────────────────

    def get_all_dataframes(self, force_refresh: bool = False) -> dict[str, pd.DataFrame]:
        """
        Return all tabs as { tab_name: DataFrame }.

        On every call:
          1. Ask Drive API for current modifiedTime of the spreadsheet.
          2. Compare with the modifiedTime we recorded at our last fetch.
          3. If unchanged → serve from memory (or reload parquet if memory cold).
          4. If changed (or force_refresh) → fetch from Sheets API + save parquet.

        Returns fresh copies so callers cannot corrupt the cache.
        """
        t0 = time.monotonic()

        # ── 1. Get current modifiedTime from Drive API ────────────────────────
        try:
            current_modified = self._get_sheet_modified_time()
        except Exception as exc:
            logger.warning(
                "drive_check_failed_serving_from_cache",
                error=str(exc),
            )
            # Drive API failed → fall back to whatever we have
            return self._serve_from_cache_or_raise()

        # ── 2. Compare with stored modifiedTime ───────────────────────────────
        stored_modified = self._cache.get_stored_modified_time()

        if not force_refresh and current_modified == stored_modified:
            # Sheet has not changed
            drive_ms = round((time.monotonic() - t0) * 1000, 1)
            if self._cache.has_memory_data():
                logger.debug(
                    "cache_valid_serving_memory",
                    modified=current_modified,
                    drive_check_ms=drive_ms,
                )
                return self._cache.get_dataframes()
            else:
                # Memory was cleared (e.g. after restart) but Parquet is valid
                logger.info(
                    "memory_cold_reloading_parquet",
                    modified=current_modified,
                    drive_check_ms=drive_ms,
                )
                if self._cache.load_from_disk():
                    return self._cache.get_dataframes()
                # Parquet gone too → fall through to full fetch

        # ── 3. Sheet has changed (or no cache exists) → fetch from Sheets API ─
        change_reason = (
            "force_refresh" if force_refresh
            else "first_load" if stored_modified is None
            else f"sheet_modified  {stored_modified} → {current_modified}"
        )
        logger.info("sheet_data_stale_fetching", reason=change_reason)

        # Prevent multiple concurrent fetches (e.g. burst of simultaneous queries)
        with self._fetch_lock:
            # Re-check after acquiring lock — another thread may have just fetched
            if not force_refresh:
                latest_stored = self._cache.get_stored_modified_time()
                if latest_stored == current_modified and self._cache.has_memory_data():
                    logger.debug("cache_populated_by_concurrent_fetch")
                    return self._cache.get_dataframes()

            all_dfs = self._fetch_all_tabs()
            schema  = {tab: self._infer_column_types(df) for tab, df in all_dfs.items()}
            self._cache.set_dataframes(all_dfs, schema)
            self._cache.save_metadata(current_modified)

        total_ms = round((time.monotonic() - t0) * 1000, 1)
        logger.info(
            "sheets_fetch_complete",
            tabs=list(all_dfs.keys()),
            total_ms=total_ms,
        )
        return {k: v.copy() for k, v in all_dfs.items()}

    def sync_dataframe(self,force_refresh: bool = False) -> pd.DataFrame:
        all_dfs = self.get_all_dataframes(force_refresh=force_refresh)
        

    def get_schema(self, force_refresh: bool = False) -> dict[str, dict[str, str]]:
        """
        Return { tab_name: { col_name: "numeric"|"text" } }.
        Triggers the same Drive-check / cache logic as get_all_dataframes.
        """
        self.get_all_dataframes(force_refresh=force_refresh)   # ensure cache is current
        return self._cache.get_schema()

    def get_tab_names(self) -> list[str]:
        return list(self.get_all_dataframes().keys())

    def last_refreshed_str(self) -> str:
        cached_at = self._cache.get_cached_at()
        if cached_at == "never":
            return "never"
        # cached_at is already an ISO string — just trim the seconds for display
        return cached_at[:19].replace("T", " ") + " UTC"

    # ──────────────────────────────────────────────────────────────────────────
    # Drive API  –  modifiedTime check
    # ──────────────────────────────────────────────────────────────────────────

    def _get_drive_client(self):
        if self._drive is None:
            creds_dict = json.loads(self._settings.google_credentials_json)
            creds = Credentials.from_service_account_info(
                creds_dict,
                scopes=SCOPES,
            )
            self._drive = google_build("drive", "v3", credentials=creds, cache_discovery=False)
            logger.info("drive_client_initialized")
        return self._drive

    def _get_sheet_modified_time(self) -> str:
        """
        Call Drive API files.get to retrieve the spreadsheet's modifiedTime.
        Returns an ISO 8601 string, e.g. "2024-11-15T08:23:10.000Z".
        This is a metadata-only request — no cell data is transferred.
        """
        drive   = self._get_drive_client()
        result  = (
            drive.files()
            .get(fileId=self._settings.google_sheet_id, fields="modifiedTime")
            .execute()
        )
        return result["modifiedTime"]

    # ──────────────────────────────────────────────────────────────────────────
    # Sheets API  –  full data fetch
    # ──────────────────────────────────────────────────────────────────────────

    def _get_gspread_client(self) -> gspread.Client:
        if self._gspread is None:
            creds = Credentials.from_service_account_file(
                self._settings.google_service_account_file,
                scopes=SCOPES,
            )
            self._gspread = gspread.authorize(creds)
            logger.info("gspread_client_initialized")
        return self._gspread

    def _fetch_all_tabs(self) -> dict[str, pd.DataFrame]:
        t0     = time.monotonic()
        client = self._get_gspread_client()
        sheet  = client.open_by_key(self._settings.google_sheet_id)
        result: dict[str, pd.DataFrame] = {}

        for worksheet in sheet.worksheets():
            tab = worksheet.title
            try:
                records = worksheet.get_all_records(
                    expected_headers=[],
                    value_render_option="UNFORMATTED_VALUE",
                    numericise_ignore=["all"],
                )
                if not records:
                    logger.warning("tab_empty_skipped", tab=tab)
                    continue

                df = pd.DataFrame(records)
                # Drop padding columns/rows Google Sheets sometimes adds
                # df = df.loc[:, df.astype(str).ne("").any(axis=0)]
                df = df[df.astype(str).ne("").any(axis=1)].reset_index(drop=True)

                if df.empty:
                    logger.warning("tab_empty_after_clean", tab=tab)
                    continue

                df = self._coerce_types(df)
                result[tab] = df
                logger.info("tab_fetched", tab=tab, rows=len(df), cols=list(df.columns))

            except Exception as exc:
                logger.error("tab_fetch_error", tab=tab, error=str(exc))

        elapsed = round((time.monotonic() - t0) * 1000, 1)
        logger.info(
            "all_tabs_fetched_from_sheets_api",
            tabs=list(result.keys()),
            ms=elapsed,
        )
        return result

    # ──────────────────────────────────────────────────────────────────────────
    # Type inference
    # ──────────────────────────────────────────────────────────────────────────

    def _coerce_types(self, df: pd.DataFrame) -> pd.DataFrame:
        threshold = self._settings.numeric_detection_threshold
        df = df.copy()
        for col in df.columns:
            cleaned = (
                df[col].astype(str).str.strip()
                .str.replace(",", "", regex=False)
                .str.replace("%", "", regex=False)
                .str.replace("₹", "", regex=False)
                .str.replace("$", "", regex=False)
                .str.replace("£", "", regex=False)
            )
            non_empty = cleaned[cleaned.str.strip().ne("") & cleaned.str.lower().ne("nan")]
            if non_empty.empty:
                df[col] = cleaned
                continue
            parsed       = pd.to_numeric(non_empty, errors="coerce")
            success_rate = parsed.notna().sum() / len(non_empty)
            if success_rate >= threshold:
                df[col] = pd.to_numeric(
                    cleaned.replace({"": None, "nan": None}),
                    errors="coerce"
                )
            else:
                df[col] = cleaned
        return df

    def _infer_column_types(self, df: pd.DataFrame) -> dict[str, dict]:
        """
        Return rich per-column metadata for LLM + query engine.

        Handles:
        - Empty columns
        - Numeric columns (with range + scale hints)
        - Text columns (with sample values)
        """

        result = {}

        for col in df.columns:
            col_series = df[col]

            # ─────────────────────────────────────────────
            # 1. Detect completely empty column
            # ─────────────────────────────────────────────
            is_empty = (
                col_series.isna().all() or
                col_series.astype(str).str.strip().eq("").all()
            )

            if is_empty:
                result[col] = {
                    "type": "empty",
                    "note": "column exists but has no data yet"
                }
                continue

            # ─────────────────────────────────────────────
            # 2. Numeric column
            # ─────────────────────────────────────────────
            if pd.api.types.is_numeric_dtype(col_series):
                col_clean = col_series.dropna()

                if col_clean.empty:
                    result[col] = {
                        "type": "numeric",
                        "min": None,
                        "max": None,
                        "scale_hint": ""
                    }
                    continue

                mn = float(col_clean.min())
                mx = float(col_clean.max())

                # Detect integer-only column (likely ID/category)
                all_integers = (col_clean % 1 == 0).all()

                # Detect scale
                if 0.0 <= mn and mx <= 1.0 and not all_integers:
                    scale_hint = "decimal ratio 0-1 (e.g. 0.75 = 75%)"
                elif 0.0 <= mn and mx <= 100.0:
                    scale_hint = "percentage 0-100"
                else:
                    scale_hint = f"range {mn} to {mx}"

                result[col] = {
                    "type": "numeric",
                    "min": round(mn, 4),
                    "max": round(mx, 4),
                    "scale_hint": scale_hint,
                }

            # ─────────────────────────────────────────────
            # 3. Text column
            # ─────────────────────────────────────────────
            else:
                cleaned = (
                    col_series
                    .dropna()
                    .astype(str)
                    .str.strip()
                )

                non_empty = cleaned[cleaned != ""]

                samples = non_empty.unique().tolist()[:10]

                result[col] = {
                    "type": "text",
                    "samples": samples
                }

        return result

    # ──────────────────────────────────────────────────────────────────────────
    # Internal helper
    # ──────────────────────────────────────────────────────────────────────────

    def _serve_from_cache_or_raise(self) -> dict[str, pd.DataFrame]:
        """Called when Drive API is unreachable. Serve stale data with a warning."""
        if self._cache.has_memory_data():
            return self._cache.get_dataframes()
        if self._cache.load_from_disk():
            return self._cache.get_dataframes()
        raise RuntimeError(
            "Drive API unreachable and no local cache found. "
            "Please ensure the server can reach Google APIs."
        )


# ─────────────────────────────────────────────────────────────────────────────
_sheets_service: SheetsService | None = None


def get_sheets_service() -> SheetsService:
    global _sheets_service
    if _sheets_service is None:
        _sheets_service = SheetsService()
    return _sheets_service
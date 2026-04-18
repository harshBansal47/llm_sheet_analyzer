from __future__ import annotations
import json
import time
import datetime
import threading
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build as google_build
from googleapiclient.errors import HttpError

from app.utils.cache import get_parquet_cache, ParquetCache
from app.utils.logger import get_logger
from app.config import get_settings

logger = get_logger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive",
]

NUMERIC = "numeric"
TEXT    = "text"


class SheetsServiceError(Exception):
    """Base exception for SheetsService errors."""


class CredentialsError(SheetsServiceError):
    """Raised when credentials are invalid or missing."""


class SheetFetchError(SheetsServiceError):
    """Raised when fetching sheet data fails."""


class CacheError(SheetsServiceError):
    """Raised when cache operations fail."""


class SheetsService:

    def __init__(self):
        try:
            self._settings = get_settings()
        except Exception as exc:
            raise CredentialsError(f"Failed to load settings: {exc}") from exc

        try:
            self._cache = get_parquet_cache(self._settings.cache_dir)
        except Exception as exc:
            raise CacheError(f"Failed to initialize cache at '{self._settings.cache_dir}': {exc}") from exc

        self._gspread: gspread.Client | None = None
        self._drive = None
        self._fetch_lock = threading.Lock()

        try:
            self._cache.load_from_disk()
        except Exception as exc:
            logger.warning("cache_load_from_disk_failed_on_init", error=str(exc))

    # ──────────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────────

    def get_all_dataframes(self, force_refresh: bool = False) -> dict[str, pd.DataFrame]:
        t0 = time.monotonic()

        # ── 1. Get current modifiedTime from Drive API ────────────────────────
        try:
            current_modified = self._get_sheet_modified_time()
        except CredentialsError:
            raise
        except Exception as exc:
            logger.warning("drive_check_failed_serving_from_cache", error=str(exc))
            return self._serve_from_cache_or_raise()

        # ── 2. Compare with stored modifiedTime ───────────────────────────────
        try:
            stored_modified = self._cache.get_stored_modified_time()
        except Exception as exc:
            logger.warning("get_stored_modified_time_failed", error=str(exc))
            stored_modified = None

        if not force_refresh and current_modified == stored_modified:
            drive_ms = round((time.monotonic() - t0) * 1000, 1)
            try:
                has_memory = self._cache.has_memory_data()
            except Exception as exc:
                logger.warning("has_memory_data_check_failed", error=str(exc))
                has_memory = False

            if has_memory:
                try:
                    logger.debug(
                        "cache_valid_serving_memory",
                        modified=current_modified,
                        drive_check_ms=drive_ms,
                    )
                    return self._cache.get_dataframes()
                except Exception as exc:
                    logger.warning("get_dataframes_from_memory_failed", error=str(exc))
            else:
                logger.info(
                    "memory_cold_reloading_parquet",
                    modified=current_modified,
                    drive_check_ms=drive_ms,
                )
                try:
                    if self._cache.load_from_disk():
                        return self._cache.get_dataframes()
                except Exception as exc:
                    logger.warning("load_from_disk_failed", error=str(exc))
                # Parquet gone too → fall through to full fetch

        # ── 3. Sheet changed or no cache → fetch from Sheets API ─────────────
        change_reason = (
            "force_refresh" if force_refresh
            else "first_load" if stored_modified is None
            else f"sheet_modified  {stored_modified} → {current_modified}"
        )
        logger.info("sheet_data_stale_fetching", reason=change_reason)

        with self._fetch_lock:
            if not force_refresh:
                try:
                    latest_stored = self._cache.get_stored_modified_time()
                    if latest_stored == current_modified and self._cache.has_memory_data():
                        logger.debug("cache_populated_by_concurrent_fetch")
                        return self._cache.get_dataframes()
                except Exception as exc:
                    logger.warning("concurrent_fetch_cache_check_failed", error=str(exc))

            try:
                all_dfs = self._fetch_all_tabs()
            except SheetFetchError:
                raise
            except Exception as exc:
                raise SheetFetchError(f"Unexpected error fetching tabs: {exc}") from exc

            if not all_dfs:
                raise SheetFetchError("No tabs were fetched — spreadsheet may be empty or inaccessible.")

            try:
                schema = {tab: self._infer_column_types(df) for tab, df in all_dfs.items()}
            except Exception as exc:
                logger.warning("schema_inference_failed", error=str(exc))
                schema = {}

            try:
                self._cache.set_dataframes(all_dfs, schema)
            except Exception as exc:
                logger.warning("cache_set_dataframes_failed", error=str(exc))

            try:
                self._cache.save_metadata(current_modified)
            except Exception as exc:
                logger.warning("cache_save_metadata_failed", error=str(exc))

        total_ms = round((time.monotonic() - t0) * 1000, 1)
        logger.info(
            "sheets_fetch_complete",
            tabs=list(all_dfs.keys()),
            total_ms=total_ms,
        )
        return {k: v.copy() for k, v in all_dfs.items()}

    def sync_dataframe(self, force_refresh: bool = False) -> pd.DataFrame | None:
        try:
            all_dfs = self.get_all_dataframes(force_refresh=force_refresh)
            return all_dfs
        except SheetsServiceError:
            raise
        except Exception as exc:
            raise SheetFetchError(f"sync_dataframe failed: {exc}") from exc

    def get_schema(self, force_refresh: bool = False) -> dict[str, dict[str, str]]:
        try:
            self.get_all_dataframes(force_refresh=force_refresh)
        except Exception as exc:
            logger.warning("get_schema_refresh_failed_returning_stale", error=str(exc))

        try:
            return self._cache.get_schema()
        except Exception as exc:
            raise CacheError(f"Failed to retrieve schema from cache: {exc}") from exc

    def get_tab_names(self) -> list[str]:
        try:
            return list(self.get_all_dataframes().keys())
        except Exception as exc:
            raise SheetFetchError(f"Failed to get tab names: {exc}") from exc

    def last_refreshed_str(self) -> str:
        try:
            cached_at = self._cache.get_cached_at()
            if cached_at == "never":
                return "never"
            return cached_at[:19].replace("T", " ") + " UTC"
        except Exception as exc:
            logger.warning("last_refreshed_str_failed", error=str(exc))
            return "unknown"

    # ──────────────────────────────────────────────────────────────────────────
    # Drive API  –  modifiedTime check
    # ──────────────────────────────────────────────────────────────────────────

    def _get_drive_client(self):
        if self._drive is None:
            try:
                creds_json = self._settings.google_credentials_json
                if not creds_json:
                    raise CredentialsError("google_credentials_json is empty or not set.")
                creds_dict = json.loads(creds_json)
            except json.JSONDecodeError as exc:
                raise CredentialsError(f"google_credentials_json is not valid JSON: {exc}") from exc
            except CredentialsError:
                raise
            except Exception as exc:
                raise CredentialsError(f"Failed to read credentials: {exc}") from exc

            try:
                creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
            except Exception as exc:
                raise CredentialsError(f"Failed to build Drive credentials: {exc}") from exc

            try:
                self._drive = google_build("drive", "v3", credentials=creds, cache_discovery=False)
                logger.info("drive_client_initialized")
            except Exception as exc:
                raise SheetsServiceError(f"Failed to build Drive API client: {exc}") from exc

        return self._drive

    def _get_sheet_modified_time(self) -> str:
        try:
            drive = self._get_drive_client()
        except CredentialsError:
            raise
        except Exception as exc:
            raise SheetsServiceError(f"Failed to get Drive client: {exc}") from exc

        try:
            result = drive.files().get(fileId="1H6cEKF-6kabeCmt7buU0xdeNG1Pw-XJaBKYweRuJoPQ", fields="modifiedTime", supportsAllDrives=True).execute()
        except HttpError as exc:
            if exc.resp.status == 404:
                raise SheetFetchError(
                    f"Spreadsheet '{self._settings.google_sheet_id}' not found. "
                    "Check GOOGLE_SHEET_ID and service account permissions."
                ) from exc
            elif exc.resp.status in (401, 403):
                raise CredentialsError(
                    f"Permission denied accessing spreadsheet: {exc}"
                ) from exc
            raise SheetsServiceError(f"Drive API HTTP error: {exc}") from exc
        except Exception as exc:
            raise SheetsServiceError(f"Drive API request failed: {exc}") from exc

        try:
            return result["modifiedTime"]
        except KeyError as exc:
            raise SheetsServiceError("Drive API response missing 'modifiedTime' field.") from exc

    # ──────────────────────────────────────────────────────────────────────────
    # Sheets API  –  full data fetch
    # ──────────────────────────────────────────────────────────────────────────

    def _get_gspread_client(self) -> gspread.Client:
        if self._gspread is None:
            try:
                creds_json = self._settings.google_credentials_json
                if not creds_json:
                    raise CredentialsError("google_credentials_json is empty or not set.")
                creds_dict = json.loads(creds_json)
                creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
            except CredentialsError:
                raise
            except json.JSONDecodeError as exc:
                raise CredentialsError(f"google_credentials_json is not valid JSON: {exc}") from exc
            except Exception as exc:
                raise CredentialsError(f"Failed to build gspread credentials: {exc}") from exc

            try:
                self._gspread = gspread.authorize(creds)
                logger.info("gspread_client_initialized")
            except Exception as exc:
                raise SheetsServiceError(f"Failed to authorize gspread client: {exc}") from exc

        return self._gspread

    def _fetch_all_tabs(self) -> dict[str, pd.DataFrame]:
        t0 = time.monotonic()

        try:
            client = self._get_gspread_client()
        except (CredentialsError, SheetsServiceError):
            raise
        except Exception as exc:
            raise SheetFetchError(f"Failed to get gspread client: {exc}") from exc

        try:
            sheet = client.open_by_key(self._settings.google_sheet_id)
        except gspread.exceptions.SpreadsheetNotFound as exc:
            raise SheetFetchError(
                f"Spreadsheet '{self._settings.google_sheet_id}' not found via gspread."
            ) from exc
        except gspread.exceptions.APIError as exc:
            raise SheetFetchError(f"gspread API error opening spreadsheet: {exc}") from exc
        except Exception as exc:
            raise SheetFetchError(f"Failed to open spreadsheet: {exc}") from exc

        try:
            worksheets = sheet.worksheets()
        except gspread.exceptions.APIError as exc:
            raise SheetFetchError(f"Failed to list worksheets: {exc}") from exc
        except Exception as exc:
            raise SheetFetchError(f"Unexpected error listing worksheets: {exc}") from exc

        if not worksheets:
            raise SheetFetchError("Spreadsheet has no worksheets.")

        result: dict[str, pd.DataFrame] = {}

        for worksheet in worksheets:
            tab = worksheet.title
            try:
                records = worksheet.get_all_records(
                    expected_headers=[],
                    value_render_option="UNFORMATTED_VALUE",
                    numericise_ignore=["all"],
                )
            except gspread.exceptions.APIError as exc:
                logger.error("tab_fetch_api_error", tab=tab, error=str(exc))
                continue
            except Exception as exc:
                logger.error("tab_fetch_error", tab=tab, error=str(exc))
                continue

            if not records:
                logger.warning("tab_empty_skipped", tab=tab)
                continue

            try:
                df = pd.DataFrame(records)
                df = df[df.astype(str).ne("").any(axis=1)].reset_index(drop=True)

                if df.empty:
                    logger.warning("tab_empty_after_clean", tab=tab)
                    continue

                df = self._coerce_types(df)
                result[tab] = df
                logger.info("tab_fetched", tab=tab, rows=len(df), cols=list(df.columns))

            except Exception as exc:
                logger.error("tab_processing_error", tab=tab, error=str(exc))
                continue

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
        try:
            threshold = self._settings.numeric_detection_threshold
        except Exception as exc:
            logger.warning("numeric_detection_threshold_missing_using_default", error=str(exc))
            threshold = 0.8

        try:
            df = df.copy()
        except Exception as exc:
            raise ValueError(f"Failed to copy DataFrame: {exc}") from exc

        for col in df.columns:
            try:
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

                parsed = pd.to_numeric(non_empty, errors="coerce")
                success_rate = parsed.notna().sum() / len(non_empty)

                if success_rate >= threshold:
                    df[col] = pd.to_numeric(
                        cleaned.replace({"": None, "nan": None}),
                        errors="coerce"
                    )
                else:
                    df[col] = cleaned
            except Exception as exc:
                logger.warning("coerce_types_column_failed", col=col, error=str(exc))
                # Leave column as-is

        return df

    def _infer_column_types(self, df: pd.DataFrame) -> dict[str, dict]:
        result = {}

        for col in df.columns:
            try:
                col_series = df[col]

                # ── 1. Detect completely empty column ─────────────────────────
                try:
                    is_empty = (
                        col_series.isna().all() or
                        col_series.astype(str).str.strip().eq("").all()
                    )
                except Exception as exc:
                    logger.warning("empty_check_failed", col=col, error=str(exc))
                    is_empty = False

                if is_empty:
                    result[col] = {"type": "empty", "note": "column exists but has no data yet"}
                    continue

                # ── 2. Numeric column ─────────────────────────────────────────
                if pd.api.types.is_numeric_dtype(col_series):
                    try:
                        col_clean = col_series.dropna()

                        if col_clean.empty:
                            result[col] = {"type": "numeric", "min": None, "max": None, "scale_hint": ""}
                            continue

                        mn = float(col_clean.min())
                        mx = float(col_clean.max())
                        all_integers = (col_clean % 1 == 0).all()

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
                    except Exception as exc:
                        logger.warning("numeric_inference_failed", col=col, error=str(exc))
                        result[col] = {"type": "numeric", "min": None, "max": None, "scale_hint": ""}

                # ── 3. Text column ────────────────────────────────────────────
                else:
                    try:
                        cleaned = col_series.dropna().astype(str).str.strip()
                        non_empty = cleaned[cleaned != ""]
                        samples = non_empty.unique().tolist()[:10]
                        result[col] = {"type": "text", "samples": samples}
                    except Exception as exc:
                        logger.warning("text_inference_failed", col=col, error=str(exc))
                        result[col] = {"type": "text", "samples": []}

            except Exception as exc:
                logger.error("infer_column_types_unexpected_error", col=col, error=str(exc))
                result[col] = {"type": "unknown", "error": str(exc)}

        return result

    # ──────────────────────────────────────────────────────────────────────────
    # Internal helper
    # ──────────────────────────────────────────────────────────────────────────

    def _serve_from_cache_or_raise(self) -> dict[str, pd.DataFrame]:
        try:
            if self._cache.has_memory_data():
                return self._cache.get_dataframes()
        except Exception as exc:
            logger.warning("serve_from_memory_failed", error=str(exc))

        try:
            if self._cache.load_from_disk():
                return self._cache.get_dataframes()
        except Exception as exc:
            logger.warning("serve_from_disk_failed", error=str(exc))

        raise SheetsServiceError(
            "Drive API unreachable and no local cache found. "
            "Please ensure the server can reach Google APIs."
        )


# ─────────────────────────────────────────────────────────────────────────────
_sheets_service: SheetsService | None = None


def get_sheets_service() -> SheetsService:
    global _sheets_service
    if _sheets_service is None:
        try:
            _sheets_service = SheetsService()
        except SheetsServiceError:
            raise
        except Exception as exc:
            raise SheetsServiceError(f"Failed to initialize SheetsService: {exc}") from exc
    return _sheets_service
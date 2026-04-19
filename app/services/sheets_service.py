from __future__ import annotations
import asyncio
import json
import time
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build as google_build
from googleapiclient.errors import HttpError

from app.services.llm_client import get_llm_client
from app.services.query_parser import _build_header_row_detection_prompt
from app.utils.cache import get_parquet_cache, ParquetCache
from app.utils.df_utils import _dedup_columns
from app.utils.logger import get_logger
from app.config import get_settings
from app.utils.validators import _to_number

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

        # asyncio.Lock instead of threading.Lock — get_all_dataframes is now async
        self._fetch_lock = asyncio.Lock()

        try:
            self._cache.load_from_disk()
        except Exception as exc:
            logger.warning("cache_load_from_disk_failed_on_init", error=str(exc))

    # ──────────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────────

    async def get_all_dataframes(self, force_refresh: bool = False) -> dict[str, pd.DataFrame]:
        t0 = time.monotonic()
        try:
            current_modified = await asyncio.to_thread(self._get_sheet_modified_time)
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

        # asyncio.Lock used with async with — prevents concurrent fetches
        async with self._fetch_lock:
            if not force_refresh:
                try:
                    latest_stored = self._cache.get_stored_modified_time()
                    if latest_stored == current_modified and self._cache.has_memory_data():
                        logger.debug("cache_populated_by_concurrent_fetch")
                        return self._cache.get_dataframes()
                except Exception as exc:
                    logger.warning("concurrent_fetch_cache_check_failed", error=str(exc))

            # _fetch_all_tabs is sync (blocking gspread calls) — run in thread pool
            # Returns dict[str, list[list[str]]] — raw rows, no DataFrame yet
            try:
                all_raw = await asyncio.to_thread(self._fetch_all_tabs)
            except SheetFetchError:
                raise
            except Exception as exc:
                raise SheetFetchError(f"Unexpected error fetching tabs: {exc}") from exc

            if not all_raw:
                raise SheetFetchError("No tabs were fetched — spreadsheet may be empty or inaccessible.")

            
           
            all_dfs = await self._normalize_all_dfs(all_raw)

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

    # ──────────────────────────────────────────────────────────────────────────
    # Normalization
    # ──────────────────────────────────────────────────────────────────────────

    async def _normalize_all_dfs(
        self, all_raw: dict[str, list[list[str]]]
    ) -> dict[str, pd.DataFrame]:
        """
        For each tab's raw rows (list[list[str]]):
          1. Detect the header row index directly on raw rows — no DataFrame needed.
          2. Build the DataFrame once, correctly, using the detected header.
          3. Coerce column types.
        """
        cleaned: dict[str, pd.DataFrame] = {}

        for tab, raw in all_raw.items():
            try:
                preview      = raw[:6]
                header_row   = await self._detect_header_row_from_raw(preview)

                # Build DataFrame in one shot — header promoted, data sliced cleanly
                headers = [c.strip() for c in raw[header_row]]
                data    = raw[header_row + 1:]

                df = pd.DataFrame(data, columns=headers)
                df.columns = _dedup_columns(list(df.columns))
                # Drop columns whose name is empty/whitespace
                df = df.loc[:, df.columns.str.strip() != ""]

                df = self._coerce_types(df)
                cleaned[tab] = df

                logger.info(
                    "tab_normalized",
                    tab=tab,
                    header_row=header_row,
                    rows=len(df),
                    cols=list(df.columns),
                )

            except Exception as exc:
                logger.warning("df_normalization_failed", tab=tab, error=str(exc))
                # Fallback: treat row 0 as header, rest as data
                try:
                    fallback_df = pd.DataFrame(raw[1:], columns=raw[0])
                    cleaned[tab] = fallback_df
                except Exception as fallback_exc:
                    logger.error(
                        "df_normalization_fallback_failed",
                        tab=tab,
                        error=str(fallback_exc),
                    )

        return cleaned

    async def _detect_header_row_from_raw(self, preview: list[list[str]]) -> int:
        """
        Use the configured LLM to detect which row (0-based) is the header row.
        Receives raw rows (list[list[str]]) directly — no DataFrame conversion needed.
        Falls back to row 0 if the AI call fails or returns an invalid index.
        """
        LOOK_AT_ROWS = len(preview)  # preview is already sliced to min(6, total)

        # Strip whitespace on each cell — mirrors what the old .astype(str).str.strip() did
        rows_preview = [
            [cell.strip() for cell in row]
            for row in preview
        ]

        system_prompt = _build_header_row_detection_prompt(rows_preview)
        user_prompt   = (
            "Identify which row is the header row containing column names.\n\n"
            "Return ONLY valid JSON with keys: header_row_index, confidence, reason."
        )

        try:
            client   = get_llm_client()
            raw_json = await client.complete(system_prompt, user_prompt)

            # Strip markdown fences — some providers add them despite instructions
            raw_json = raw_json.strip()
            if raw_json.startswith("```"):
                lines    = raw_json.splitlines()
                raw_json = "\n".join(
                    line for line in lines
                    if not line.strip().startswith("```")
                ).strip()

            response = json.loads(raw_json)
            index    = int(response["header_row_index"])

            if not (0 <= index < LOOK_AT_ROWS):
                raise ValueError(f"header_row_index {index} out of range 0–{LOOK_AT_ROWS - 1}")

            logger.info(
                "ai_header_detection_success",
                detected_row=index,
                confidence=response.get("confidence"),
                reason=response.get("reason"),
            )
            return index

        except (json.JSONDecodeError, KeyError, ValueError, TypeError) as exc:
            logger.warning("header_row_detection_bad_response", error=str(exc))
            return 0

        except Exception as exc:
            logger.warning("header_row_detection_failed", error=str(exc))
            return 0

    # ──────────────────────────────────────────────────────────────────────────
    # Public helpers
    # ──────────────────────────────────────────────────────────────────────────

    async def sync_dataframe(self, force_refresh: bool = False) -> dict[str, pd.DataFrame] | None:
        """Async wrapper — kept for call-site compatibility."""
        try:
            return await self.get_all_dataframes(force_refresh=force_refresh)
        except Exception as exc:
            raise SheetFetchError(f"sync_dataframe failed: {exc}") from exc

    async def get_schema(self, force_refresh: bool = False) -> dict[str, dict[str, str]]:
        try:
            await self.get_all_dataframes(force_refresh=force_refresh)
        except Exception as exc:
            logger.warning("get_schema_refresh_failed_returning_stale", error=str(exc))
        try:
            return self._cache.get_schema()
        except Exception as exc:
            raise CacheError(f"Failed to retrieve schema from cache: {exc}") from exc

    async def get_tab_names(self) -> list[str]:
        try:
            dfs = await self.get_all_dataframes()
            return list(dfs.keys())
        except Exception as exc:
            raise SheetFetchError(f"Failed to get tab names: {exc}") from exc

    def last_refreshed_str(self) -> str:
        """Sync — reads from local cache only, no I/O."""
        try:
            cached_at = self._cache.get_cached_at()
            if cached_at == "never":
                return "never"
            return cached_at[:19].replace("T", " ") + " UTC"
        except Exception as exc:
            logger.warning("last_refreshed_str_failed", error=str(exc))
            return "unknown"

    # ──────────────────────────────────────────────────────────────────────────
    # Drive API  –  modifiedTime check  (sync — wrapped in to_thread above)
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
        """Sync — called via asyncio.to_thread() from get_all_dataframes."""
        try:
            drive = self._get_drive_client()
        except CredentialsError:
            raise
        except Exception as exc:
            raise SheetsServiceError(f"Failed to get Drive client: {exc}") from exc

        try:
            result = (
                drive.files()
                .get(
                    fileId=self._settings.google_sheet_id,
                    fields="modifiedTime",
                    supportsAllDrives=True,
                )
                .execute()
            )
        except HttpError as exc:
            if exc.resp.status == 404:
                raise SheetFetchError(
                    f"Spreadsheet '{self._settings.google_sheet_id}' not found. "
                    "Check GOOGLE_SHEET_ID and service account permissions."
                ) from exc
            elif exc.resp.status in (401, 403):
                raise CredentialsError(f"Permission denied accessing spreadsheet: {exc}") from exc
            raise SheetsServiceError(f"Drive API HTTP error: {exc}") from exc
        except Exception as exc:
            raise SheetsServiceError(f"Drive API request failed: {exc}") from exc

        try:
            return result["modifiedTime"]
        except KeyError as exc:
            raise SheetsServiceError("Drive API response missing 'modifiedTime' field.") from exc

    # ──────────────────────────────────────────────────────────────────────────
    # Sheets API  –  full data fetch  (sync — wrapped in to_thread above)
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

    def _fetch_all_tabs(self) -> dict[str, list[list[str]]]:
        """
        Sync — called via asyncio.to_thread() from get_all_dataframes.
        Returns raw rows as dict[str, list[list[str]]] — NO DataFrame is built here.
        Header detection and DataFrame construction happen in _normalize_all_dfs(),
        so no row is pre-consumed or misidentified as a header at this stage.
        """
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

        result: dict[str, list[list[str]]] = {}

        for worksheet in worksheets:
            tab = worksheet.title
            try:
                raw = worksheet.get_all_values()
            except gspread.exceptions.APIError as exc:
                logger.error("tab_fetch_api_error", tab=tab, error=str(exc))
                continue
            except Exception as exc:
                logger.error("tab_fetch_error", tab=tab, error=str(exc))
                continue

            if not raw:
                logger.warning("tab_empty_skipped", tab=tab)
                continue

            try:
                # Drop fully-empty rows (all cells are blank/whitespace)
                raw = [row for row in raw if any(cell.strip() for cell in row)]

                if not raw:
                    logger.warning("tab_empty_after_clean", tab=tab)
                    continue

                result[tab] = raw
                logger.info(
                    "tab_fetched_raw",
                    tab=tab,
                    rows=len(raw),
                    cols=len(raw[0]) if raw else 0,
                )

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
    # Type coercion & schema inference  (sync — pure pandas, no I/O)
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
                        errors="coerce",
                    )
                else:
                    df[col] = cleaned
            except Exception as exc:
                logger.warning("coerce_types_column_failed", col=col, error=str(exc))

        return df


    def _infer_column_types(self, df: pd.DataFrame) -> dict[str, dict]:
        result = {}

        for col in df.columns:
            try:
                col_series = df[col]

                # ── 0. Duplicate column names → df[col] returns DataFrame ─────────
                if isinstance(col_series, pd.DataFrame):
                    col_series = col_series.iloc[:, 0]

                col_lower  = col.lower().strip()
                col_tokens = set(col_lower.replace("-", " ").replace("_", " ").replace("\n", " ").split())

                # ── 1. Empty ──────────────────────────────────────────────────────
                is_empty = (
                    col_series.isna().all() or
                    col_series.astype(str).str.strip().eq("").all()
                )
                if is_empty:
                    result[col] = {"type": "empty"}
                    continue

                cleaned_str = col_series.dropna().astype(str).str.strip()
                non_empty   = cleaned_str[cleaned_str != ""]

                # ── 2. Name-based overrides — before dtype check ──────────────────
                # Phone: name matches AND values look like digit strings
                if col_tokens & {"mob", "phone", "mobile", "contact", "cell"}:
                    digit_ratio = (
                        non_empty.str.replace(r"[\s\-\+\(\)]", "", regex=True)
                        .str.isnumeric().mean()
                    )
                    if digit_ratio >= 0.5:
                        result[col] = {"type": "phone", "note": "treat as string, do not aggregate"}
                        continue

                # Email
                if col_tokens & {"email", "mail"}:
                    result[col] = {"type": "email", "note": "use equality or contains filter"}
                    continue

                # ── 3. Numeric dtype ──────────────────────────────────────────────
                if pd.api.types.is_numeric_dtype(col_series):
                    col_clean    = col_series.dropna()
                    mn           = float(col_clean.min())
                    mx           = float(col_clean.max())
                    all_integers = bool((col_clean % 1 == 0).all())
                    n_unique     = col_clean.nunique()
                    mn_out       = int(mn) if mn == int(mn) else round(mn, 4)
                    mx_out       = int(mx) if mx == int(mx) else round(mx, 4)

                    # Small-range integers → categorical (e.g. Phase: 1,2)
                    if all_integers and (mx - mn) <= 20 and n_unique <= 10:
                        unique_vals = sorted(col_clean.astype(int).unique().tolist())
                        result[col] = {"type": "categorical", "values": unique_vals, "note": "use exact match"}
                        continue

                    # Identifier
                    if col_lower in ("sn", "sr", "s.no", "id", "no") or col_lower.endswith("_id"):
                        result[col] = {"type": "identifier", "min": mn_out, "max": mx_out}
                        continue

                    # Percentage
                    if "%" in col or col_tokens & {"percent", "pct", "percentage"}:
                        result[col] = {"type": "percentage", "min": mn_out, "max": mx_out}
                        continue

                    # Currency — only when max is large enough to be a monetary value
                    if mx >= 1000 and any(k in col_lower for k in (
                        "price", "amount", "amt", "value", "rate",
                        "balance", "bal", "demanded", "paid", "sale",
                    )):
                        result[col] = {
                            "type": "currency",
                            "min": mn_out,
                            "max": mx_out,
                            "note": "monetary value — do not treat as plain integer",
                        }
                        continue

                    result[col] = {
                        "type": "numeric",
                        "min": mn_out,
                        "max": mx_out,
                        "all_integers": all_integers,
                    }
                    continue

                # ── 4. String columns ─────────────────────────────────────────────

                # Date
                sample_parsed = pd.to_datetime(
                    non_empty.head(10), format="mixed", dayfirst=True, errors="coerce"
                )
                if sample_parsed.notna().sum() >= min(5, len(non_empty.head(10))):
                    result[col] = {
                        "type": "date",
                        "samples": non_empty.unique().tolist()[:5],
                        "note": "use date comparison operators when filtering",
                    }
                    continue

                unique_vals  = non_empty.str.upper().unique().tolist()
                n_unique     = len(unique_vals)
                cardinality  = n_unique / max(len(non_empty), 1)

                # Grade (A–E)
                if set(unique_vals) <= {"A", "B", "C", "D", "E"} and n_unique >= 2:
                    result[col] = {
                        "type": "grade",
                        "values": sorted(unique_vals),
                        "note": "ordinal — A is best, E is worst",
                    }
                    continue

                # Boolean
                if set(v.lower() for v in unique_vals) <= {"yes", "no", "y", "n", "true", "false"}:
                    result[col] = {"type": "boolean", "values": sorted(unique_vals)}
                    continue

                # Categorical
                if n_unique <= 20 and cardinality <= 0.6:
                    result[col] = {
                        "type": "categorical",
                        "values": sorted(unique_vals[:20]),
                        "note": "use exact match or isin() when filtering",
                    }
                    continue

                # Identifier-like text
                if col_tokens & {"apt", "unit", "flat", "no.", "code", "ref"}:
                    result[col] = {
                        "type": "identifier",
                        "samples": non_empty.unique().tolist()[:10],
                        "note": "unique code — use exact match",
                    }
                    continue

                # Free text
                result[col] = {
                    "type": "free_text",
                    "samples": non_empty.unique().tolist()[:5],
                    "note": "use contains/partial match when filtering",
                }

            except Exception as exc:
                logger.error("infer_column_types_error", col=col, error=str(exc))
                result[col] = {"type": "unknown", "error": str(exc)}

        return result

    # ──────────────────────────────────────────────────────────────────────────
    # Internal helpers
    # ──────────────────────────────────────────────────────────────────────────

    def _serve_from_cache_or_raise(self) -> dict[str, pd.DataFrame]:
        """Sync — reads only from local memory/disk cache, no I/O."""
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
"""
services/sheets_service.py  –  Multi-tab Google Sheets integration.

Key changes from v1:
  • Loads ALL worksheet tabs, not just one named tab.
  • Returns a dict[tab_name → DataFrame] — one entry per sheet.
  • Auto-detects numeric columns via heuristic (no static column map).
  • Exposes get_schema() → dict[tab_name → {col_name: "numeric"|"text"}]
    which the query parser injects into its dynamic system prompt.
  • Tab list and column headers update automatically on next cache refresh —
    no code changes needed when the spreadsheet structure changes.

Cache keys:
  "all_tabs"  → dict[str, pd.DataFrame]
  "schema"    → dict[str, dict[str, str]]  (derived, invalidated with data)
"""
from __future__ import annotations
import time
import datetime
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from app.utils.cache import get_sheet_cache
from app.utils.logger import get_logger
from app.config import get_settings

logger = get_logger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

CACHE_KEY_DATA   = "all_tabs"
CACHE_KEY_SCHEMA = "schema"

NUMERIC = "numeric"
TEXT    = "text"


class SheetsService:
    def __init__(self):
        self._settings = get_settings()
        self._cache    = get_sheet_cache(ttl=self._settings.sheet_cache_ttl)
        self._client: gspread.Client | None = None

    # ──────────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────────

    def get_all_dataframes(self, force_refresh: bool = False) -> dict[str, pd.DataFrame]:
        """
        Return all tabs as { tab_name: DataFrame }.
        Each call returns fresh copies so callers cannot mutate the cache.
        """
        if not force_refresh:
            cached = self._cache.get(CACHE_KEY_DATA)
            if cached is not None:
                logger.debug("sheet_cache_hit")
                return {k: v.copy() for k, v in cached.items()}

        logger.info("sheet_cache_miss_fetching_all_tabs")
        all_dfs = self._fetch_all_tabs()
        self._cache.set(CACHE_KEY_DATA, all_dfs)
        self._cache.invalidate(CACHE_KEY_SCHEMA)   # rebuild on next call
        return {k: v.copy() for k, v in all_dfs.items()}

    def get_dataframe(self, tab_name: str, force_refresh: bool = False) -> pd.DataFrame:
        """Convenience: return a single tab's DataFrame. Raises KeyError if not found."""
        all_dfs = self.get_all_dataframes(force_refresh=force_refresh)
        if tab_name not in all_dfs:
            available = ", ".join(f'"{t}"' for t in all_dfs.keys())
            raise KeyError(f"Tab '{tab_name}' not found. Available: {available}")
        return all_dfs[tab_name]

    def get_schema(self, force_refresh: bool = False) -> dict[str, dict[str, str]]:
        """
        Return the live schema:
          { tab_name: { column_name: "numeric" | "text" } }

        Injected into the NLP parser's system prompt at parse time so the LLM
        always knows the current sheet structure with zero hardcoding.
        """
        if not force_refresh:
            cached = self._cache.get(CACHE_KEY_SCHEMA)
            if cached is not None:
                return cached

        all_dfs = self.get_all_dataframes(force_refresh=force_refresh)
        schema: dict[str, dict[str, str]] = {
            tab: self._infer_column_types(df)
            for tab, df in all_dfs.items()
        }
        self._cache.set(CACHE_KEY_SCHEMA, schema)
        logger.info(
            "schema_built",
            tabs=list(schema.keys()),
            total_cols=sum(len(v) for v in schema.values()),
        )
        return schema

    def get_tab_names(self) -> list[str]:
        return list(self.get_all_dataframes().keys())

    def last_refreshed_str(self) -> str:
        ts = self._cache.last_refreshed(CACHE_KEY_DATA)
        if ts is None:
            return "never"
        return datetime.datetime.fromtimestamp(ts).strftime("%H:%M:%S")

    # ──────────────────────────────────────────────────────────────────────────
    # Internal – fetching
    # ──────────────────────────────────────────────────────────────────────────

    def _get_client(self) -> gspread.Client:
        if self._client is None:
            creds = Credentials.from_service_account_file(
                self._settings.google_service_account_file,
                scopes=SCOPES,
            )
            self._client = gspread.authorize(creds)
            logger.info("google_sheets_client_initialized")
        return self._client

    def _fetch_all_tabs(self) -> dict[str, pd.DataFrame]:
        """Fetch every worksheet. Skips empty or broken sheets gracefully."""
        t0     = time.monotonic()
        client = self._get_client()
        sheet  = client.open_by_key(self._settings.google_sheet_id)
        all_dfs: dict[str, pd.DataFrame] = {}

        for worksheet in sheet.worksheets():
            tab_name = worksheet.title
            try:
                records = worksheet.get_all_records(
                    expected_headers=[],
                    value_render_option="UNFORMATTED_VALUE",
                    numericise_ignore=["all"],
                )
                if not records:
                    logger.warning("tab_empty_skipped", tab=tab_name)
                    continue

                df = pd.DataFrame(records)
                # Drop entirely-empty columns and rows (Google Sheets padding)
                df = df.loc[:, df.astype(str).ne("").any(axis=0)]
                df = df[df.astype(str).ne("").any(axis=1)].reset_index(drop=True)

                if df.empty:
                    logger.warning("tab_empty_after_clean", tab=tab_name)
                    continue

                df = self._coerce_types(df)
                all_dfs[tab_name] = df
                logger.info("tab_loaded", tab=tab_name, rows=len(df), cols=list(df.columns))

            except Exception as exc:
                logger.error("tab_fetch_error", tab=tab_name, error=str(exc))
                continue   # skip broken tab; don't abort entire fetch

        elapsed = round((time.monotonic() - t0) * 1000, 1)
        logger.info("all_tabs_fetched", count=len(all_dfs), tabs=list(all_dfs.keys()), ms=elapsed)
        return all_dfs

    # ──────────────────────────────────────────────────────────────────────────
    # Internal – type inference
    # ──────────────────────────────────────────────────────────────────────────

    def _coerce_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Auto-detect and coerce column types.
        A column is numeric if >= numeric_detection_threshold of its non-empty
        cells parse successfully as a number after stripping currency/% symbols.
        """
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

            parsed      = pd.to_numeric(non_empty, errors="coerce")
            success_rate = parsed.notna().sum() / len(non_empty)

            if success_rate >= threshold:
                df[col] = pd.to_numeric(
                    cleaned.replace({"": float("nan"), "nan": float("nan")}),
                    errors="coerce",
                )
            else:
                df[col] = cleaned

        return df

    def _infer_column_types(self, df: pd.DataFrame) -> dict[str, str]:
        return {
            col: NUMERIC if pd.api.types.is_numeric_dtype(df[col]) else TEXT
            for col in df.columns
        }


# ─────────────────────────────────────────────────────────────────────────────
_sheets_service: SheetsService | None = None


def get_sheets_service() -> SheetsService:
    global _sheets_service
    if _sheets_service is None:
        _sheets_service = SheetsService()
    return _sheets_service
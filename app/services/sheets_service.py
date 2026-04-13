"""
services/sheets_service.py  –  Google Sheets integration.

Responsibilities:
  • Authenticate via Service Account
  • Fetch the worksheet as a pandas DataFrame
  • Cache with TTL so repeated queries within `SHEET_CACHE_TTL` seconds
    don't hit the Sheets API (stays fast + within quota)
  • Expose canonical column names mapped from config

The DataFrame returned is ALWAYS a fresh copy so callers can't
mutate the cache.
"""
from __future__ import annotations
import time
import datetime
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from utils.cache import get_sheet_cache
from utils.logger import get_logger
from config import get_settings

logger = get_logger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

CACHE_KEY = "sheet_dataframe"


class SheetsService:
    def __init__(self):
        self._settings = get_settings()
        self._cache = get_sheet_cache(ttl=self._settings.sheet_cache_ttl)
        self._client: gspread.Client | None = None

    # ──────────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────────

    def get_dataframe(self, force_refresh: bool = False) -> pd.DataFrame:
        """
        Return the sheet data as a pandas DataFrame.
        Uses cache unless force_refresh=True or TTL expired.
        """
        if not force_refresh:
            cached = self._cache.get(CACHE_KEY)
            if cached is not None:
                logger.debug("sheet_cache_hit")
                return cached.copy()

        logger.info("sheet_cache_miss_fetching_from_api")
        df = self._fetch_from_api()
        self._cache.set(CACHE_KEY, df)
        return df.copy()

    def get_canonical_columns(self) -> dict[str, str]:
        """Return {canonical_name: actual_sheet_column} from config."""
        return self._settings.column_map

    def last_refreshed_str(self) -> str:
        ts = self._cache.last_refreshed(CACHE_KEY)
        if ts is None:
            return "never"
        dt = datetime.datetime.fromtimestamp(ts)
        return dt.strftime("%H:%M:%S")

    def get_available_canonical_fields(self) -> list[str]:
        """Returns list of canonical field names present in the sheet."""
        col_map = self.get_canonical_columns()
        df = self.get_dataframe()
        return [canon for canon, actual in col_map.items() if actual in df.columns]

    # ──────────────────────────────────────────────────────────────────────────
    # Internal
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

    def _fetch_from_api(self) -> pd.DataFrame:
        t0 = time.monotonic()
        client = self._get_client()
        sheet = client.open_by_key(self._settings.google_sheet_id)
        worksheet = sheet.worksheet(self._settings.google_sheet_tab)
        records = worksheet.get_all_records(
            expected_headers=[],   # accept any headers
            value_render_option="UNFORMATTED_VALUE",
            numericise_ignore=["all"],  # keep raw strings; we'll coerce ourselves
        )
        df = pd.DataFrame(records)
        df = self._coerce_types(df)
        elapsed = (time.monotonic() - t0) * 1000
        logger.info("sheet_fetched", rows=len(df), cols=len(df.columns), ms=round(elapsed, 1))
        return df

    def _coerce_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and type-coerce the DataFrame.
        Numeric columns get NaN for blanks; strings get stripped.
        """
        cfg = self._settings
        numeric_cols = [cfg.col_total_cost, cfg.col_amount_received, cfg.col_payment_percent]

        for col in df.columns:
            if col in numeric_cols:
                df[col] = pd.to_numeric(
                    df[col].astype(str).str.replace(",", "").str.replace("%", "").str.strip(),
                    errors="coerce",
                )
            else:
                df[col] = df[col].astype(str).str.strip()

        return df


# Module-level singleton
_sheets_service: SheetsService | None = None


def get_sheets_service() -> SheetsService:
    global _sheets_service
    if _sheets_service is None:
        _sheets_service = SheetsService()
    return _sheets_service
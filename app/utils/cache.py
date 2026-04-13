"""
utils/cache.py  –  Two-layer persistent cache for Google Sheets data.

Layer 1 – In-memory dict
  Serves requests in ~0ms.  Holds DataFrames and schema in process memory.
  Invalidated only when Drive API reports the sheet has been modified.

Layer 2 – Parquet files on disk  (via pyarrow)
  Written every time fresh data is fetched from the Sheets API.
  Read on startup (or after a restart) so the first query never hits Sheets
  API unless the sheet was actually edited while the server was down.

  Layout on disk:
    {cache_dir}/
      metadata.json          ← { sheet_modified_time, cached_at, tabs: [...] }
      tab_<safe_name>.parquet ← one file per worksheet tab

No TTL.  Freshness is determined exclusively by comparing Drive API
modifiedTime against the last value stored in metadata.json.
"""
from __future__ import annotations
import json
import re
import threading
import datetime
from pathlib import Path
from typing import Any
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from app.utils.logger import get_logger

logger = get_logger(__name__)

METADATA_FILE = "metadata.json"


def _safe_filename(tab_name: str) -> str:
    """Convert a tab name to a filesystem-safe filename stem."""
    safe = re.sub(r"[^\w\-]", "_", tab_name)
    return f"tab_{safe}"


class ParquetCache:
    """
    Thread-safe two-layer cache:
      • in-memory dict  (zero-latency reads between fetches)
      • Parquet files   (survive restarts, avoid cold-fetch on startup)

    The caller (SheetsService) decides WHEN to invalidate by comparing
    Drive API modifiedTime — this class only handles HOW data is stored
    and retrieved.
    """

    def __init__(self, cache_dir: str = "cache"):
        self._dir   = Path(cache_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._lock  = threading.Lock()

        # In-memory state
        self._dataframes: dict[str, pd.DataFrame] = {}   # tab_name → DataFrame
        self._schema:     dict[str, dict[str, str]] = {} # tab_name → {col: type}
        self._metadata:   dict[str, Any] = {}            # sheet_modified_time, cached_at, tabs

    # ──────────────────────────────────────────────────────────────────────────
    # Metadata (modifiedTime tracking)
    # ──────────────────────────────────────────────────────────────────────────

    def get_stored_modified_time(self) -> str | None:
        """
        Return the sheet modifiedTime we recorded at our last Sheets API fetch.
        None if no cache exists yet.
        """
        with self._lock:
            # 1. Check in-memory first
            if self._metadata:
                return self._metadata.get("sheet_modified_time")
            # 2. Fall back to metadata.json on disk
            meta_path = self._dir / METADATA_FILE
            if meta_path.exists():
                try:
                    data = json.loads(meta_path.read_text())
                    self._metadata = data
                    return data.get("sheet_modified_time")
                except Exception as exc:
                    logger.warning("metadata_read_error", error=str(exc))
            return None

    def save_metadata(self, sheet_modified_time: str) -> None:
        """Persist modifiedTime and fetch timestamp to metadata.json."""
        with self._lock:
            self._metadata = {
                "sheet_modified_time": sheet_modified_time,
                "cached_at": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
                "tabs": list(self._dataframes.keys()),
            }
            try:
                (self._dir / METADATA_FILE).write_text(
                    json.dumps(self._metadata, indent=2)
                )
            except Exception as exc:
                logger.error("metadata_write_error", error=str(exc))

    def get_cached_at(self) -> str:
        with self._lock:
            return self._metadata.get("cached_at", "never")

    # ──────────────────────────────────────────────────────────────────────────
    # DataFrame access
    # ──────────────────────────────────────────────────────────────────────────

    def has_memory_data(self) -> bool:
        with self._lock:
            return bool(self._dataframes)

    def get_dataframes(self) -> dict[str, pd.DataFrame]:
        """Return fresh copies from in-memory store. Empty dict if not loaded."""
        with self._lock:
            return {k: v.copy() for k, v in self._dataframes.items()}

    def get_schema(self) -> dict[str, dict[str, str]]:
        with self._lock:
            return dict(self._schema)

    def set_dataframes(
        self,
        dataframes: dict[str, pd.DataFrame],
        schema: dict[str, dict[str, str]],
    ) -> None:
        """
        Store DataFrames in memory AND flush each one to a Parquet file.
        Called only after a successful Sheets API fetch.
        """
        with self._lock:
            self._dataframes = {k: v.copy() for k, v in dataframes.items()}
            self._schema     = dict(schema)

        # Write parquet outside the lock (IO-bound, doesn't need the lock)
        self._write_parquet(dataframes)

    def load_from_disk(self) -> bool:
        """
        Attempt to populate in-memory store from Parquet files on disk.
        Returns True if at least one tab was loaded successfully.

        Called on startup before checking Drive API, so a restart doesn't
        force an immediate Sheets API fetch if the cache is still valid.
        """
        parquet_files = list(self._dir.glob("tab_*.parquet"))
        if not parquet_files:
            logger.info("parquet_cache_empty_no_files")
            return False

        loaded: dict[str, pd.DataFrame] = {}
        for pq_file in parquet_files:
            try:
                df = pq.read_table(pq_file).to_pandas()
                # Recover tab name from filename: tab_<safe>.parquet
                # We store the real name in parquet metadata
                tab_name = pq_file.stem[4:]  # strip "tab_" prefix
                # Try to read the real tab name from parquet metadata
                pq_meta = pq.read_metadata(pq_file)
                if pq_meta.metadata and b"tab_name" in pq_meta.metadata:
                    tab_name = pq_meta.metadata[b"tab_name"].decode()
                loaded[tab_name] = df
                logger.info("parquet_tab_loaded", tab=tab_name, rows=len(df))
            except Exception as exc:
                logger.error("parquet_read_error", file=str(pq_file), error=str(exc))

        if not loaded:
            return False

        with self._lock:
            self._dataframes = loaded
            # Rebuild schema from loaded DataFrames
            self._schema = {
                tab: {
                    col: "numeric" if pd.api.types.is_numeric_dtype(df[col]) else "text"
                    for col in df.columns
                }
                for tab, df in loaded.items()
            }

        logger.info("parquet_cache_loaded_into_memory", tabs=list(loaded.keys()))
        return True

    # ──────────────────────────────────────────────────────────────────────────
    # Internal – Parquet I/O
    # ──────────────────────────────────────────────────────────────────────────

    def _write_parquet(self, dataframes: dict[str, pd.DataFrame]) -> None:
        """Write each DataFrame to its own Parquet file with tab_name metadata."""
        for tab_name, df in dataframes.items():
            pq_path = self._dir / f"{_safe_filename(tab_name)}.parquet"
            try:
                table = pa.Table.from_pandas(df, preserve_index=False)
                # Embed the real tab name in parquet file metadata
                existing_meta = table.schema.metadata or {}
                merged_meta   = {**existing_meta, b"tab_name": tab_name.encode()}
                table = table.replace_schema_metadata(merged_meta)
                pq.write_table(table, pq_path, compression="snappy")
                logger.info(
                    "parquet_written",
                    tab=tab_name,
                    path=str(pq_path),
                    rows=len(df),
                    size_kb=round(pq_path.stat().st_size / 1024, 1),
                )
            except Exception as exc:
                logger.error("parquet_write_error", tab=tab_name, error=str(exc))

    def clear_disk_cache(self) -> None:
        """Delete all cached files — useful for debugging / forced reset."""
        with self._lock:
            self._dataframes.clear()
            self._schema.clear()
            self._metadata.clear()
        for f in self._dir.glob("tab_*.parquet"):
            f.unlink(missing_ok=True)
        meta = self._dir / METADATA_FILE
        if meta.exists():
            meta.unlink()
        logger.info("cache_cleared")


# ─── Module-level singleton ───────────────────────────────────────────────────
_parquet_cache: ParquetCache | None = None


def get_parquet_cache(cache_dir: str = "cache") -> ParquetCache:
    global _parquet_cache
    if _parquet_cache is None:
        _parquet_cache = ParquetCache(cache_dir=cache_dir)
    return _parquet_cache
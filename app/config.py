"""
config.py  –  Central configuration loaded from environment / .env file.
All secrets and tunables live here.

Column mapping has been removed: the system now auto-discovers all tab names
and column headers directly from Google Sheets at runtime.  No static metadata
needed — adding a new tab or renaming a column in the sheet is automatically
picked up on the next cache refresh.
"""
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path

base_dir = Path(__file__).resolve().parent

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Google Sheets ─────────────────────────────────────────────────────────
    google_service_account_file: Path = base_dir / "credentials.json"
    google_sheet_id: str 

    # ── OpenAI (NL parsing ONLY) ──────────────────────────────────────────────
    openai_api_key: str
    openai_model: str = "gpt-4o-mini"

    # ── Telegram ──────────────────────────────────────────────────────────────
    telegram_bot_token: str = ""
    telegram_webhook_url: str = ""

    # ── WhatsApp ──────────────────────────────────────────────────────────────
    whatsapp_phone_number_id: str = ""
    whatsapp_access_token: str = ""
    whatsapp_verify_token: str = "verify_token"
    whatsapp_webhook_url: str = ""

    # ── App ───────────────────────────────────────────────────────────────────
    app_env: str = "production"
    secret_key: str = "change_me"
    log_level: str = "INFO"
    sheet_cache_ttl: int = 30           # seconds between sheet refreshes
    cache_dir:Path = base_dir
    allowed_telegram_user_ids: str = "" # comma-separated IDs, empty = all

    # ── Numeric detection threshold ───────────────────────────────────────────
    # A column is auto-detected as numeric if this fraction of its non-empty
    # values parse successfully as numbers.  Default 0.6 = 60%.
    numeric_detection_threshold: float = 0.6
    
    @property
    def allowed_user_ids(self) -> list[int]:
        if not self.allowed_telegram_user_ids.strip():
            return []
        return [int(uid.strip()) for uid in self.allowed_telegram_user_ids.split(",") if uid.strip()]

    @property
    def is_dev(self) -> bool:
        return self.app_env == "development"


@lru_cache
def get_settings() -> Settings:
    return Settings()
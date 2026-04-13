"""
config.py  –  Central configuration loaded from environment / .env file.
All secrets and tunables live here. No magic strings scattered in code.
"""
from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Google Sheets ─────────────────────────────────────────────────────────
    google_service_account_file: str = "credentials/service_account.json"
    google_sheet_id: str
    google_sheet_tab: str = "Sheet1"

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
    allowed_telegram_user_ids: str = "" # comma-separated IDs, empty = all

    # ── Column Mapping ────────────────────────────────────────────────────────
    col_customer_name: str = "Customer Name"
    col_unit: str = "Unit"
    col_phase: str = "Phase"
    col_total_cost: str = "Total Cost"
    col_amount_received: str = "Amount Received"
    col_payment_percent: str = "Payment %"
    col_status: str = "Status"
    col_remarks: str = "Remarks"

    @property
    def column_map(self) -> dict:
        """Canonical name → actual sheet column header mapping."""
        return {
            "customer_name":    self.col_customer_name,
            "unit":             self.col_unit,
            "phase":            self.col_phase,
            "total_cost":       self.col_total_cost,
            "amount_received":  self.col_amount_received,
            "payment_percent":  self.col_payment_percent,
            "status":           self.col_status,
            "remarks":          self.col_remarks,
        }

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
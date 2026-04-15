"""
services/query_orchestrator.py  –  End-to-end query pipeline

Wires together:  SheetsService → NLP Parser → Query Engine → Formatter

v2 change: orchestrator fetches the live schema from SheetsService and passes
it to the parser.  The parser uses it to build a dynamic system prompt so the
LLM always knows the current tabs and columns with zero hardcoding.
"""
from __future__ import annotations
import asyncio
import time
from app.models.models import IncomingMessage
from app.services.query_parser import get_query_parser
from app.services.query_engine import get_query_engine
from app.services.sheets_service import get_sheets_service
from app.services.response_formatter import get_formatter
from app.utils.logger import get_logger
from app.config import get_settings

logger = get_logger(__name__)


class QueryOrchestrator:

    async def handle(self, message: IncomingMessage) -> str:
        t0       = time.monotonic()
        question = message.text.strip()

        if not question:
            return "Please send a question about your data."

        # Special commands
        if question.lower() in ("/start", "hi", "hello", "help"):
            return await self._help_message()

        if question.lower() in ("/refresh", "refresh data"):
            return await self._refresh_sheet()

        if question.lower() in ("/schema", "/tabs"):
            return self._schema_summary()

        logger.info(
            "incoming_query",
            platform=message.platform,
            user=message.user_id,
            question=question[:100],
        )

        # ── 1. Fetch live schema (cached, fast) ───────────────────────────────
        loop = asyncio.get_event_loop()
        try:
            svc    = get_sheets_service()
            schema = await loop.run_in_executor(None, svc.get_schema)
        except Exception as exc:
            logger.error("schema_fetch_failed", error=str(exc))
            return "❌ Could not load sheet data. Please try again in a moment."

        # ── 2. NLP parse with live schema injected ────────────────────────────
        try:
            parser          = get_query_parser()
            structured_query = await parser.parse(question, schema)
        except Exception as exc:
            logger.error("parse_failed", error=str(exc))
            return (
                "❌ Sorry, I couldn't understand that question.\n\n"
                "Try rephrasing. Type /help for examples.\n"
                "Type /schema to see available sheets and columns."
            )

        # ── 3. Deterministic execution ────────────────────────────────────────
        try:
            engine = get_query_engine()
            result = await loop.run_in_executor(None, engine.execute, structured_query)
        except Exception as exc:
            logger.error("engine_failed", error=str(exc))
            return "❌ Error processing your query. Please try again."

        # ── 4. Format response ────────────────────────────────────────────────
        formatter = get_formatter()
        response  = (
            formatter.format_telegram(result)
            if message.platform == "telegram"
            else formatter.format_whatsapp(result)
        )

        elapsed = round((time.monotonic() - t0) * 1000, 1)
        logger.info("query_complete", ms=elapsed, success=result.success)

        if get_settings().is_dev:
            response += f"\n\n`[{elapsed}ms total | engine: {result.execution_time_ms}ms]`"

        return response

    # ── Helpers ───────────────────────────────────────────────────────────────

    async def _refresh_sheet(self) -> str:
        loop = asyncio.get_event_loop()
        try:
            svc = get_sheets_service()
            dfs = await loop.run_in_executor(
                None, lambda: svc.get_all_dataframes(force_refresh=True)
            )
            summary = "\n".join(
                f"  • *{tab}*: {len(df)} rows, {len(df.columns)} columns"
                for tab, df in dfs.items()
            )
            return f"✅ Sheet data refreshed.\n\n{summary}"
        except Exception as exc:
            return f"❌ Failed to refresh: {exc}"

    def _schema_summary(self) -> str:
        try:
            svc    = get_sheets_service()
            schema = svc.get_schema()
            lines  = ["📋 *Available Sheets & Columns*\n"]
            for tab, cols in schema.items():
                lines.append(f"*{tab}*")
                for col, ctype in cols.items():
                    icon = "🔢" if ctype == "numeric" else "🔤"
                    lines.append(f"  {icon} {col}")
                lines.append("")
            return "\n".join(lines)
        except Exception as exc:
            return f"❌ Could not load schema: {exc}"

    async def _help_message(self) -> str:
        try:
            svc  = get_sheets_service()
            tabs = svc.get_tab_names()
            tab_list = ", ".join(f"*{t}*" for t in tabs) if tabs else "_(not loaded yet)_"
        except Exception:
            tab_list = "_(not loaded yet)_"

        return (
            "👋 *Data Query Bot*\n\n"
            f"Available sheets: {tab_list}\n\n"
            "Ask natural language questions. Examples:\n\n"
            "• _What percent has been received for Customer A?_\n"
            "• _How many customers paid more than 70%?_\n"
            "• _Total cost and received amount for unit A-302_\n"
            "• _Show all records in Phase 2 with payment below 50%_\n"
            "• _Customers with court cases and payment below 30%_\n\n"
            "Commands:\n"
            "`/refresh` — reload sheet data\n"
            "`/schema`  — show all tabs and column names\n"
            "`/help`    — this message"
        )


# Singleton
_orchestrator: QueryOrchestrator | None = None


def get_orchestrator() -> QueryOrchestrator:
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = QueryOrchestrator()
    return _orchestrator
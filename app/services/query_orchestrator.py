"""
services/query_orchestrator.py  –  End-to-end query pipeline  (v3)

Pipeline:
  User message
    → [QueryRewriter]   Stage 0 — LLM cleans / normalises any input
    → [QueryParser]     Stage 1 — LLM converts clean query → StructuredQuery JSON
    → [QueryEngine]     Stage 2 — Deterministic pandas execution
    → [ResponseFormatter] Stage 3 — Human-readable output

v3 changes:
  • QueryRewriter inserted as Stage 0.
  • _schema_summary() is now async (fixes coroutine crash on /schema).
  • Both LLM calls share the same schema fetch (one await, used twice).
"""
from __future__ import annotations
import asyncio
import time
from app.models.models import IncomingMessage
from app.services.query_rewriter import get_rewriter
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

        # Special commands — bypass rewriter entirely
        if question.lower() in ("/start", "hi", "hello", "help"):
            return await self._help_message()

        if question.lower() in ("/refresh", "refresh data"):
            return await self._refresh_sheet()

        if question.lower() in ("/schema", "/tabs"):
            return await self._schema_summary()

        logger.info(
            "incoming_query",
            platform=message.platform,
            user=message.user_id,
            question=question[:100],
        )

        # ── 1. Fetch live schema ONCE (used by both rewriter + parser) ─────────
        try:
            svc    = get_sheets_service()
            schema = await svc.get_schema()
        except Exception as exc:
            logger.error("schema_fetch_failed", error=str(exc))
            return "❌ Could not load sheet data. Please try again in a moment."

        # ── 2. Stage 0: Rewrite — clean any user input before parsing ─────────
        rewriter = get_rewriter()
        clean_question, needs_clarification = await rewriter.rewrite_with_fallback(
            question, schema
        )

        # If the rewriter decided it needs clarification, return that directly
        if needs_clarification:
            logger.info("pipeline_short_circuited_by_rewriter", reason=clean_question[:80])
            return f"🤔 {clean_question}"

        if clean_question != question:
            logger.info(
                "query_rewritten",
                original=question[:80],
                rewritten=clean_question[:80],
            )

        # ── 3. Stage 1: Parse — rewritten query → StructuredQuery ─────────────
        try:
            parser           = get_query_parser()
            structured_query = await parser.parse(clean_question, schema)
        except Exception as exc:
            logger.error("parse_failed", error=str(exc))
            return (
                "❌ Sorry, I couldn't understand that question.\n\n"
                "Try rephrasing. Type /help for examples.\n"
                "Type /schema to see available sheets and columns."
            )

        # ── 4. Stage 2: Execute — deterministic pandas ────────────────────────
        try:
            engine = get_query_engine()
            result = await engine.execute(structured_query)
        except Exception as exc:
            logger.error("engine_failed", error=str(exc))
            return "❌ Error processing your query. Please try again."

        # ── 5. Stage 3: Format ─────────────────────────────────────────────────
        formatter = get_formatter()
        response  = (
            formatter.format_telegram(result)
            if message.platform == "telegram"
            else formatter.format_whatsapp(result)
        )

        elapsed = round((time.monotonic() - t0) * 1000, 1)
        logger.info("query_complete", ms=elapsed, success=result.success)

        if get_settings().is_dev:
            # Show both LLM timings in dev mode
            response += (
                f"\n\n`[{elapsed}ms total | engine: {result.execution_time_ms}ms]`"
                f"\n`[rewritten: \"{clean_question[:60]}\"]`"
            )

        return response

    # ── Helpers ───────────────────────────────────────────────────────────────

    async def _refresh_sheet(self) -> str:
        try:
            svc = get_sheets_service()
            dfs = await svc.get_all_dataframes(force_refresh=True)
            summary = "\n".join(
                f"  • *{tab}*: {len(df)} rows, {len(df.columns)} columns"
                for tab, df in dfs.items()
            )
            return f"✅ Sheet data refreshed.\n\n{summary}"
        except Exception as exc:
            return f"❌ Failed to refresh: {exc}"

    async def _schema_summary(self) -> str:
        """Fixed: now async so get_schema() can be properly awaited."""
        try:
            svc    = get_sheets_service()
            schema = await svc.get_schema()          # ← was missing await
            lines  = ["📋 *Available Sheets & Columns*\n"]
            for tab, cols in schema.items():
                lines.append(f"*{tab}*")
                for col, meta in cols.items():
                    ctype = meta.get("type", "text") if isinstance(meta, dict) else str(meta)
                    if ctype == "empty":
                        continue   # don't confuse users with empty columns
                    icon = (
                        "🔢" if ctype in ("numeric", "currency", "percentage", "identifier")
                        else "📅" if ctype == "date"
                        else "✅" if ctype == "boolean"
                        else "🔤"
                    )
                    lines.append(f"  {icon} {col}  _({ctype})_")
                lines.append("")
            return "\n".join(lines)
        except Exception as exc:
            return f"❌ Could not load schema: {exc}"

    async def _help_message(self) -> str:
        try:
            svc  = get_sheets_service()
            tabs = await svc.get_tab_names()
            tab_list = ", ".join(f"*{t}*" for t in tabs) if tabs else "_(not loaded yet)_"
        except Exception:
            tab_list = "_(not loaded yet)_"

        return (
            "👋 *Data Query Bot*\n\n"
            f"Available sheets: {tab_list}\n\n"
            "You can ask in plain language — even with typos or mixed Hindi/English:\n\n"
            "• _L-044 ka total aur received kya hai?_\n"
            "• _cogers ke kitne units hain?_\n"
            "• _how many customers paid more than 70%?_\n"
            "• _total rcvd from nclt-2_\n"
            "• _show all records in Phase 2 with payment below 50%_\n\n"
            "Commands:\n"
            "`/refresh` — reload sheet data\n"
            "`/schema`  — show all tabs and column names\n"
            "`/help`    — this message"
        )


# ─────────────────────────────────────────────────────────────────────────────
_orchestrator: QueryOrchestrator | None = None


def get_orchestrator() -> QueryOrchestrator:
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = QueryOrchestrator()
    return _orchestrator
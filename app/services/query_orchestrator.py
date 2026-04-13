"""
services/query_orchestrator.py  –  End-to-end query pipeline

Ties together:  NLP Parser → Query Engine → Response Formatter
This is what the bot handlers call.
"""
from __future__ import annotations
import asyncio
import time
from app.models.models import IncomingMessage, QueryResult
from app.services.query_parser import get_query_parser
from app.services.query_engine import get_query_engine
from app.services.response_formatter import get_formatter
from app.utils.logger import get_logger

logger = get_logger(__name__)


class QueryOrchestrator:
    """
    Single entry point for processing a user message.
    Returns a formatted string ready to send back to the user.
    """

    async def handle(self, message: IncomingMessage) -> str:
        t0 = time.monotonic()
        question = message.text.strip()

        if not question:
            return "Please send a question about your data."

        # Special commands
        if question.lower() in ("/start", "hi", "hello", "help"):
            return self._help_message()

        if question.lower() in ("/refresh", "refresh data"):
            return await self._refresh_sheet()

        logger.info(
            "incoming_query",
            platform=message.platform,
            user=message.user_id,
            question=question[:100],
        )

        # 1. NLP: Natural Language → StructuredQuery
        try:
            parser = get_query_parser()
            structured_query = await parser.parse(question)
        except Exception as exc:
            logger.error("parse_failed", error=str(exc))
            return (
                "❌ Sorry, I couldn't understand that question.\n\n"
                "Please try rephrasing. Example:\n"
                "_What is the payment percent for customer Ahmed Ali?_"
            )

        # 2. Engine: StructuredQuery → QueryResult  (sync, run in thread)
        loop = asyncio.get_event_loop()
        try:
            engine = get_query_engine()
            result: QueryResult = await loop.run_in_executor(
                None, engine.execute, structured_query
            )
        except Exception as exc:
            logger.error("engine_failed", error=str(exc))
            return "❌ Error processing your query. Please try again."

        # 3. Format result
        formatter = get_formatter()
        if message.platform == "telegram":
            response_text = formatter.format_telegram(result)
        else:
            response_text = formatter.format_whatsapp(result)

        elapsed = round((time.monotonic() - t0) * 1000, 1)
        logger.info("query_complete", ms=elapsed, success=result.success)

        # Append debug timing in dev mode
        from app.config import get_settings
        if get_settings().is_dev:
            response_text += f"\n\n`[{elapsed}ms total | engine: {result.execution_time_ms}ms]`"

        return response_text

    async def _refresh_sheet(self) -> str:
        from app.services.sheets_service import get_sheets_service
        loop = asyncio.get_event_loop()
        try:
            svc = get_sheets_service()
            await loop.run_in_executor(None, lambda: svc.get_dataframe(force_refresh=True))
            return "✅ Sheet data refreshed successfully."
        except Exception as exc:
            return f"❌ Failed to refresh: {exc}"

    def _help_message(self) -> str:
        return (
            "👋 *Data Query Bot*\n\n"
            "Ask me anything about the payment data. Examples:\n\n"
            "• _What percent has been received for Ahmed Ali?_\n"
            "• _How many customers paid more than 70%?_\n"
            "• _Total cost and received amount for unit A-302_\n"
            "• _Units in Phase 1 with payment less than 50%_\n"
            "• _Customers with court cases and payment below 30%_\n\n"
            "Type `/refresh` to force-reload the sheet data."
        )


# Singleton
_orchestrator: QueryOrchestrator | None = None


def get_orchestrator() -> QueryOrchestrator:
    global _orchestrator
    if _orchestrator is None:
        _orchestrator = QueryOrchestrator()
    return _orchestrator
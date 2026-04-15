from __future__ import annotations
import asyncio

from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from telegram.constants import ParseMode

from app.models.models import IncomingMessage
from app.services.query_orchestrator import get_orchestrator
from app.utils.logger import get_logger
from app.config import get_settings

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Message Handler
# ─────────────────────────────────────────────────────────────────────────────

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle all incoming text messages safely."""
    try:
        settings = get_settings()

        if not update.message or not update.message.text:
            logger.warning("empty_message_received")
            return

        user = update.effective_user
        user_id = str(user.id)

        # # ── Access control ────────────────────────────────────────────────
        # try:
        #     allowed = settings.allowed_user_ids
        #     if allowed and user.id not in allowed:
        #         await update.message.reply_text(
        #             "⛔ You are not authorised to use this bot.\nContact your administrator."
        #         )
        #         return
        # except Exception as exc:
        #     logger.error("access_control_error", error=str(exc))

        msg = IncomingMessage(
            platform="telegram",
            user_id=user_id,
            username=user.username or user.full_name or user_id,
            text=update.message.text,
            message_id=str(update.message.message_id),
        )

        # ── Typing indicator (non-critical) ───────────────────────────────
        try:
            await context.bot.send_chat_action(
                chat_id=update.effective_chat.id,
                action="typing",
            )
        except Exception as exc:
            logger.warning("typing_indicator_failed", error=str(exc))

        # ── Core processing ───────────────────────────────────────────────
        try:
            orchestrator = get_orchestrator()
            response = await orchestrator.handle(msg)
        except Exception as exc:
            logger.error(
                "orchestrator_failed",
                error=str(exc),
                user_id=user_id,
                text=update.message.text[:100],
            )
            response = "⚠️ Something went wrong while processing your request. Please try again."

        # ── Send response ────────────────────────────────────────────────
        try:
            await update.message.reply_text(
                text=response,
                parse_mode=ParseMode.MARKDOWN,
            )
        except Exception as exc:
            logger.error("telegram_send_failed", error=str(exc))

    except Exception as exc:
        logger.exception("handle_message_fatal_error", error=str(exc))


# ─────────────────────────────────────────────────────────────────────────────
# Start / Help Handler
# ─────────────────────────────────────────────────────────────────────────────

async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        msg = IncomingMessage(
            platform="telegram",
            user_id=str(update.effective_user.id),
            text="/start",
        )

        try:
            response = await get_orchestrator().handle(msg)
        except Exception as exc:
            logger.error("start_handler_failed", error=str(exc))
            response = "⚠️ Failed to initialize. Please try again later."

        await update.message.reply_text(
            response,
            parse_mode=ParseMode.MARKDOWN
        )

    except Exception as exc:
        logger.exception("handle_start_fatal_error", error=str(exc))


# ─────────────────────────────────────────────────────────────────────────────
# Global Error Handler (VERY IMPORTANT)
# ─────────────────────────────────────────────────────────────────────────────

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Catch ALL unhandled exceptions from telegram."""
    logger.exception(
        "telegram_global_error",
        error=str(context.error),
        update=str(update)[:500],
    )

    # Try notifying user (best effort)
    try:
        if isinstance(update, Update) and update.message:
            await update.message.reply_text(
                "⚠️ Unexpected error occurred. Please try again."
            )
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# Build Application
# ─────────────────────────────────────────────────────────────────────────────

def build_application() -> Application:
    settings = get_settings()

    try:
        app = (
            Application.builder()
            .token(settings.telegram_bot_token)
            .build()
        )

        app.add_handler(CommandHandler("start", handle_start))
        app.add_handler(CommandHandler("help", handle_start))
        app.add_handler(
            MessageHandler(filters.TEXT , handle_message)
        )

        # 🔥 Register global error handler
        app.add_error_handler(error_handler)

        return app

    except Exception as exc:
        logger.exception("application_build_failed", error=str(exc))
        raise


# ─────────────────────────────────────────────────────────────────────────────
# Webhook Setup
# ─────────────────────────────────────────────────────────────────────────────

async def setup_webhook(app: Application) -> None:
    settings = get_settings()

    try:
        if not settings.telegram_webhook_url:
            logger.warning("telegram_webhook_url_not_set_skipping_webhook_setup")
            return

        webhook_url = f"{settings.telegram_webhook_url}/webhook/telegram"

        await app.bot.set_webhook(
            url=webhook_url,
            drop_pending_updates=True
        )

        logger.info("telegram_webhook_set", url=webhook_url)

    except Exception as exc:
        logger.exception("webhook_setup_failed", error=str(exc))


# ─────────────────────────────────────────────────────────────────────────────
# Singleton
# ─────────────────────────────────────────────────────────────────────────────

_telegram_app: Application | None = None


def get_telegram_app() -> Application:
    global _telegram_app

    try:
        if _telegram_app is None:
            _telegram_app = build_application()
        return _telegram_app

    except Exception as exc:
        logger.exception("get_telegram_app_failed", error=str(exc))
        raise
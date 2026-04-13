from __future__ import annotations
import asyncio
from telegram import Update, Bot
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from telegram.constants import ParseMode
from app.models.models import IncomingMessage
from app.services.query_orchestrator import get_orchestrator
from app.utils.logger import get_logger
from app.config import get_settings

logger = get_logger(__name__)


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle all incoming text messages."""
    settings = get_settings()

    if not update.message or not update.message.text:
        return

    user = update.effective_user
    user_id = str(user.id)

    # Access control
    allowed = settings.allowed_user_ids
    if allowed and user.id not in allowed:
        await update.message.reply_text(
            "⛔ You are not authorised to use this bot.\nContact your administrator."
        )
        return

    msg = IncomingMessage(
        platform="telegram",
        user_id=user_id,
        username=user.username or user.full_name or user_id,
        text=update.message.text,
        message_id=str(update.message.message_id),
    )

    # Show typing indicator while processing
    await context.bot.send_chat_action(
        chat_id=update.effective_chat.id,
        action="typing",
    )

    orchestrator = get_orchestrator()
    response = await orchestrator.handle(msg)

    await update.message.reply_text(
        text=response,
        parse_mode=ParseMode.MARKDOWN,
    )


async def handle_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    msg = IncomingMessage(
        platform="telegram",
        user_id=str(update.effective_user.id),
        text="/start",
    )
    response = await get_orchestrator().handle(msg)
    await update.message.reply_text(response, parse_mode=ParseMode.MARKDOWN)


def build_application() -> Application:
    settings = get_settings()
    app = (
        Application.builder()
        .token(settings.telegram_bot_token)
        .build()
    )
    app.add_handler(CommandHandler("start", handle_start))
    app.add_handler(CommandHandler("help",  handle_start))
    app.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)
    )
    return app


# ─── Called once on FastAPI startup ──────────────────────────────────────────

async def setup_webhook(app: Application) -> None:
    settings = get_settings()
    if not settings.telegram_webhook_url:
        logger.warning("telegram_webhook_url_not_set_skipping_webhook_setup")
        return

    webhook_url = f"{settings.telegram_webhook_url}/webhook/telegram"
    await app.bot.set_webhook(url=webhook_url, drop_pending_updates=True)
    logger.info("telegram_webhook_set", url=webhook_url)


_telegram_app: Application | None = None


def get_telegram_app() -> Application:
    global _telegram_app
    if _telegram_app is None:
        _telegram_app = build_application()
    return _telegram_app
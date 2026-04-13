"""
bots/whatsapp_bot.py  –  WhatsApp Meta Cloud API integration

Handles:
  • GET  /webhook/whatsapp  – webhook verification challenge
  • POST /webhook/whatsapp  – incoming messages

Docs: https://developers.facebook.com/docs/whatsapp/cloud-api
"""
from __future__ import annotations
import httpx
from app.models.models import IncomingMessage
from app.services.query_orchestrator import get_orchestrator
from app.utils.logger import get_logger
from app.config import get_settings

logger = get_logger(__name__)

WHATSAPP_API_URL = "https://graph.facebook.com/v19.0/{phone_id}/messages"


async def process_whatsapp_event(body: dict) -> None:
    """
    Parse a WhatsApp Cloud API webhook payload and respond.
    Handles only text messages; ignores media/status updates.
    """
    try:
        entry   = body.get("entry", [{}])[0]
        changes = entry.get("changes", [{}])[0]
        value   = changes.get("value", {})
        messages = value.get("messages", [])

        if not messages:
            return  # Status update or other non-message event

        msg_data = messages[0]

        if msg_data.get("type") != "text":
            await _send_whatsapp_text(
                phone=msg_data["from"],
                text="I can only process text messages. Please type your question.",
            )
            return

        msg = IncomingMessage(
            platform="whatsapp",
            user_id=msg_data["from"],
            text=msg_data["text"]["body"],
            message_id=msg_data["id"],
        )

        orchestrator = get_orchestrator()
        response = await orchestrator.handle(msg)

        # WhatsApp doesn't support Markdown – strip asterisks
        plain_response = _strip_markdown(response)
        await _send_whatsapp_text(phone=msg_data["from"], text=plain_response)

    except Exception as exc:
        logger.error("whatsapp_processing_error", error=str(exc))


async def _send_whatsapp_text(phone: str, text: str) -> None:
    settings = get_settings()
    url = WHATSAPP_API_URL.format(phone_id=settings.whatsapp_phone_number_id)
    headers = {
        "Authorization": f"Bearer {settings.whatsapp_access_token}",
        "Content-Type": "application/json",
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": phone,
        "type": "text",
        "text": {"body": text[:4096]},   # WA limit
    }
    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.post(url, json=payload, headers=headers)
        if resp.status_code != 200:
            logger.error("whatsapp_send_failed", status=resp.status_code, body=resp.text[:200])
        else:
            logger.info("whatsapp_message_sent", to=phone)


def _strip_markdown(text: str) -> str:
    """Remove Telegram Markdown characters for WhatsApp plain text."""
    return text.replace("*", "").replace("_", "").replace("`", "")
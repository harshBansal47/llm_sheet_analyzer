from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import PlainTextResponse
from telegram import Update
from app.bots.telegram import get_telegram_app
from app.bots.whatsapp import process_whatsapp_event
from app.config import get_settings
from app.utils.logger import get_logger

logger = get_logger(__name__)
router = APIRouter(tags=["bot"])



@router.post("/webhook/telegram")
async def telegram_webhook(request: Request):
    settings = get_settings()
    if not settings.telegram_bot_token:
        raise HTTPException(status_code=503, detail="Telegram not configured")
 
    body = await request.json()
    tg_app = get_telegram_app()
    update = Update.de_json(data=body, bot=tg_app.bot)
    await tg_app.process_update(update)
    return {"ok": True}
 
 
# ─────────────────────────────────────────────────────────────────────────────
# WhatsApp webhook
# ─────────────────────────────────────────────────────────────────────────────
 
@router.get("/webhook/whatsapp", response_class=PlainTextResponse)
async def whatsapp_verify(
    hub_mode: str | None = None,
    hub_verify_token: str | None = None,
    hub_challenge: str | None = None,
):
    """Meta verification challenge."""
    settings = get_settings()
    if hub_mode == "subscribe" and hub_verify_token == settings.whatsapp_verify_token:
        logger.info("whatsapp_webhook_verified")
        return hub_challenge or ""
    raise HTTPException(status_code=403, detail="Verification failed")
 
 
@router.post("/webhook/whatsapp")
async def whatsapp_incoming(request: Request):
    body = await request.json()
    # Acknowledge immediately (Meta requires 200 within 20s)
    import asyncio
    asyncio.create_task(process_whatsapp_event(body))
    return {"status": "received"}
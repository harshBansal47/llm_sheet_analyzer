from __future__ import annotations
import asyncio
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
 
from app.bots.telegram import get_telegram_app, setup_webhook
from app.config import get_settings
from app.services.sheets_service import get_sheets_service
from app.utils.logger import setup_logging, get_logger
from app.routes import bot, query

# ─────────────────────────────────────────────────────────────────────────────
# App lifecycle
# ─────────────────────────────────────────────────────────────────────────────
 
@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    logger = get_logger("startup")
    settings = get_settings()
 
    # Pre-warm sheet cache
    logger.info("warming_sheet_cache")
    try:
        svc = get_sheets_service()
        # Keep startup responsive if Google API is slow/unreachable.
        await asyncio.wait_for(
            asyncio.to_thread(svc.sync_dataframe, force_refresh=True),
            timeout=20,
        )
        logger.info("sheet_cache_warmed")
    except asyncio.TimeoutError:
        logger.warning("sheet_warm_timeout")
    except Exception as exc:
        logger.error("sheet_warm_failed", error=str(exc))
 
    # Register Telegram webhook if token configured
    if settings.telegram_bot_token:
        tg_app = get_telegram_app()
        await tg_app.initialize()
        await setup_webhook(tg_app)
        logger.info("telegram_ready")
 
    yield
 
    # Shutdown
    if settings.telegram_bot_token:
        await get_telegram_app().shutdown()
    logger.info("app_shutdown")
 
 


# ─────────────────────────────────────────────────────────────────────────────
# App instance
# ─────────────────────────────────────────────────────────────────────────────
 
app = FastAPI(
    title="Sheets Query Bot API",
    description="Data query bot backed by Google Sheets with zero-hallucination guarantee",
    version="1.0.0",
    lifespan=lifespan,
)

settings = get_settings()
cors_origins = ["*"] if settings.is_dev else []

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(bot.router)
app.include_router(query.router)
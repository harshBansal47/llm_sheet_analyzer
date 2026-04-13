from __future__ import annotations
import time
from contextlib import asynccontextmanager
 
from fastapi import FastAPI, Request, Header, HTTPException, Depends
from fastapi.responses import PlainTextResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
 
from app.bots.telegram import get_telegram_app, setup_webhook
from config import get_settings
from models import IncomingMessage
from services.query_orchestrator import get_orchestrator
from services.sheets_service import get_sheets_service
from utils.logger import setup_logging, get_logger

 
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
        svc.get_dataframe(force_refresh=True)
        logger.info("sheet_cache_warmed")
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
 
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)
 
logger = get_logger(__name__)
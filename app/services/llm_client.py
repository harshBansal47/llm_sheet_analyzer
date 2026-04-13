"""
services/llm_client.py  –  Provider-agnostic LLM client for NLP query parsing.

Supports three providers, selected via config.api_provider:
  "openai"    → OpenAI (gpt-4o-mini default)
  "anthropic" → Anthropic Claude (claude-haiku-4-5-20251001 default)
  "google"    → Google Gemini (gemini-1.5-flash default)

Contract:
  All three implementations expose the same interface:
    async def complete(system_prompt: str, user_prompt: str) -> str

  The returned string is always the raw text content from the model.
  The caller (QueryParser) is responsible for JSON parsing.

AI is used ONLY for NL → StructuredQuery parsing.
No provider ever sees sheet data or produces final answers.
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from utils.logger import get_logger

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Abstract base
# ─────────────────────────────────────────────────────────────────────────────

class LLMClient(ABC):
    """Common interface all providers must implement."""

    @abstractmethod
    async def complete(self, system_prompt: str, user_prompt: str) -> str:
        """
        Send a system + user prompt, return the model's text response.
        Temperature is always 0 (deterministic parsing).
        """

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Human-readable provider label for logging."""


# ─────────────────────────────────────────────────────────────────────────────
# OpenAI
# ─────────────────────────────────────────────────────────────────────────────

class OpenAIClient(LLMClient):
    def __init__(self, api_key: str, model: str):
        from openai import AsyncOpenAI
        self._client = AsyncOpenAI(api_key=api_key)
        self._model  = model

    @property
    def provider_name(self) -> str:
        return f"openai/{self._model}"

    async def complete(self, system_prompt: str, user_prompt: str) -> str:
        response = await self._client.chat.completions.create(
            model=self._model,
            temperature=0,
            max_tokens=800,
            response_format={"type": "json_object"},
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_prompt},
            ],
        )
        return response.choices[0].message.content


# ─────────────────────────────────────────────────────────────────────────────
# Anthropic
# ─────────────────────────────────────────────────────────────────────────────

class AnthropicClient(LLMClient):
    def __init__(self, api_key: str, model: str):
        import anthropic
        self._client = anthropic.AsyncAnthropic(api_key=api_key)
        self._model  = model

    @property
    def provider_name(self) -> str:
        return f"anthropic/{self._model}"

    async def complete(self, system_prompt: str, user_prompt: str) -> str:
        # Anthropic uses system as a top-level param, not a message role
        message = await self._client.messages.create(
            model=self._model,
            max_tokens=800,
            temperature=0,
            system=system_prompt,
            messages=[{"role": "user", "content": user_prompt}],
        )
        raw = message.content[0].text

        # Anthropic doesn't have a native JSON-mode — strip markdown fences
        # if the model wrapped the output in ```json ... ```
        raw = raw.strip()
        if raw.startswith("```"):
            lines = raw.splitlines()
            raw   = "\n".join(
                line for line in lines
                if not line.strip().startswith("```")
            ).strip()
        return raw


# ─────────────────────────────────────────────────────────────────────────────
# Google Gemini  (uses google-genai SDK, the successor to google-generativeai)
# ─────────────────────────────────────────────────────────────────────────────

class GeminiClient(LLMClient):
    def __init__(self, api_key: str, model: str):
        from google import genai
        from google.genai import types as genai_types
        self._client     = genai.Client(api_key=api_key)
        self._model_name = model
        self._types      = genai_types

    @property
    def provider_name(self) -> str:
        return f"google/{self._model_name}"

    async def complete(self, system_prompt: str, user_prompt: str) -> str:
        import asyncio

        config = self._types.GenerateContentConfig(
            temperature=0,
            max_output_tokens=800,
            response_mime_type="application/json",   # native JSON mode
            system_instruction=system_prompt,
        )

        # google-genai sync client — run in executor to avoid blocking event loop
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(
            None,
            lambda: self._client.models.generate_content(
                model=self._model_name,
                contents=user_prompt,
                config=config,
            ),
        )
        return response.text


# ─────────────────────────────────────────────────────────────────────────────
# Factory
# ─────────────────────────────────────────────────────────────────────────────

def build_llm_client() -> LLMClient:
    """
    Read config.api_provider and instantiate the matching client.
    Called once at startup; result cached as module-level singleton.
    """
    from config import get_settings
    cfg      = get_settings()
    provider = cfg.api_provider.lower().strip()

    if provider == "openai":
        if not cfg.openai_api_key:
            raise ValueError("api_provider=openai but OPENAI_API_KEY is not set.")
        client = OpenAIClient(api_key=cfg.openai_api_key, model=cfg.openai_model)

    elif provider == "anthropic":
        if not cfg.anthropic_api_key:
            raise ValueError("api_provider=anthropic but ANTHROPIC_API_KEY is not set.")
        client = AnthropicClient(api_key=cfg.anthropic_api_key, model=cfg.anthropic_model)

    elif provider == "google":
        if not cfg.google_api_key:
            raise ValueError("api_provider=google but GOOGLE_API_KEY is not set.")
        client = GeminiClient(api_key=cfg.google_api_key, model=cfg.google_model)

    else:
        raise ValueError(
            f"Unknown api_provider='{provider}'. "
            "Choose one of: openai, anthropic, google"
        )

    logger.info("llm_client_initialized", provider=client.provider_name)
    return client


# Module-level singleton — built on first use
_llm_client: LLMClient | None = None


def get_llm_client() -> LLMClient:
    global _llm_client
    if _llm_client is None:
        _llm_client = build_llm_client()
    return _llm_client
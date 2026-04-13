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

import anthropic
from app.utils.logger import get_logger
from app.config import get_settings

logger = get_logger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Custom exceptions
# ─────────────────────────────────────────────────────────────────────────────

class LLMClientError(Exception):
    """Base exception for LLM client errors."""
    pass

class LLMConfigurationError(LLMClientError):
    """Raised when provider configuration is invalid."""
    pass

class LLMProviderError(LLMClientError):
    """Raised when the provider API returns an error."""
    pass

class LLMResponseError(LLMClientError):
    """Raised when the provider returns malformed or unexpected response."""
    pass


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
        
        Raises:
            LLMProviderError: If the provider API call fails
            LLMResponseError: If the response is malformed or empty
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
        try:
            from openai import AsyncOpenAI
            from openai import APIError, APIConnectionError, RateLimitError, AuthenticationError
            self._client = AsyncOpenAI(api_key=api_key)
            self._model = model
            self._APIError = APIError
            self._APIConnectionError = APIConnectionError
            self._RateLimitError = RateLimitError
            self._AuthenticationError = AuthenticationError
        except ImportError as e:
            logger.error("openai_import_error", error=str(e))
            raise LLMConfigurationError("OpenAI package not installed. Run: pip install openai") from e

    @property
    def provider_name(self) -> str:
        return f"openai/{self._model}"

    async def complete(self, system_prompt: str, user_prompt: str) -> str:
        try:
            response = await self._client.chat.completions.create(
                model=self._model,
                temperature=0,
                max_tokens=800,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            )
            
            if not response or not response.choices or not response.choices[0].message:
                raise LLMResponseError("OpenAI returned empty or malformed response")
            
            content = response.choices[0].message.content
            if not content or not content.strip():
                raise LLMResponseError("OpenAI returned empty content")
            
            return content
            
        except getattr(self, '_AuthenticationError', Exception) as e:
            logger.error("openai_auth_error", error=str(e), model=self._model)
            raise LLMProviderError(f"OpenAI authentication failed: {str(e)}") from e
        except getattr(self, '_RateLimitError', Exception) as e:
            logger.error("openai_rate_limit_error", error=str(e), model=self._model)
            raise LLMProviderError(f"OpenAI rate limit exceeded: {str(e)}") from e
        except getattr(self, '_APIConnectionError', Exception) as e:
            logger.error("openai_connection_error", error=str(e), model=self._model)
            raise LLMProviderError(f"OpenAI connection failed: {str(e)}") from e
        except getattr(self, '_APIError', Exception) as e:
            logger.error("openai_api_error", error=str(e), model=self._model)
            raise LLMProviderError(f"OpenAI API error: {str(e)}") from e
        except Exception as e:
            logger.error("openai_unexpected_error", error=str(e), model=self._model, exc_info=True)
            raise LLMProviderError(f"Unexpected OpenAI error: {str(e)}") from e


# ─────────────────────────────────────────────────────────────────────────────
# Anthropic
# ─────────────────────────────────────────────────────────────────────────────

class AnthropicClient(LLMClient):
    def __init__(self, api_key: str, model: str):
        try:
            import anthropic
            self._client = anthropic.AsyncAnthropic(api_key=api_key)
            self._model = model
        except ImportError as e:
            logger.error("anthropic_import_error", error=str(e))
            raise LLMConfigurationError("Anthropic package not installed. Run: pip install anthropic") from e

    @property
    def provider_name(self) -> str:
        return f"anthropic/{self._model}"

    async def complete(self, system_prompt: str, user_prompt: str) -> str:
        try:
            message = await self._client.messages.create(
                model=self._model,
                max_tokens=800,
                temperature=0,
                system=system_prompt,
                messages=[{"role": "user", "content": user_prompt}],
            )
            
            if not message or not message.content or len(message.content) == 0:
                raise LLMResponseError("Anthropic returned empty or malformed response")
            
            raw = message.content[0].text
            
            if not raw or not raw.strip():
                raise LLMResponseError("Anthropic returned empty content")

            # Anthropic doesn't have a native JSON-mode — strip markdown fences
            # if the model wrapped the output in ```json ... ```
            raw = raw.strip()
            if raw.startswith("```"):
                lines = raw.splitlines()
                raw = "\n".join(
                    line for line in lines
                    if not line.strip().startswith("```")
                ).strip()
                
                # If after stripping we have nothing, raise error
                if not raw:
                    raise LLMResponseError("Anthropic returned only markdown fences with no content")
            
            return raw
            
        except anthropic.APIError as e:
            logger.error("anthropic_api_error", error=str(e), model=self._model)
            raise LLMProviderError(f"Anthropic API error: {str(e)}") from e
        except anthropic.APIConnectionError as e:
            logger.error("anthropic_connection_error", error=str(e), model=self._model)
            raise LLMProviderError(f"Anthropic connection failed: {str(e)}") from e
        except anthropic.RateLimitError as e:
            logger.error("anthropic_rate_limit_error", error=str(e), model=self._model)
            raise LLMProviderError(f"Anthropic rate limit exceeded: {str(e)}") from e
        except anthropic.AuthenticationError as e:
            logger.error("anthropic_auth_error", error=str(e), model=self._model)
            raise LLMProviderError(f"Anthropic authentication failed: {str(e)}") from e
        except Exception as e:
            logger.error("anthropic_unexpected_error", error=str(e), model=self._model, exc_info=True)
            raise LLMProviderError(f"Unexpected Anthropic error: {str(e)}") from e


# ─────────────────────────────────────────────────────────────────────────────
# Google Gemini  (uses google-genai SDK, the successor to google-generativeai)
# ─────────────────────────────────────────────────────────────────────────────

class GeminiClient(LLMClient):
    def __init__(self, api_key: str, model: str):
        try:
            from google import genai
            from google.genai import types as genai_types
            from google.genai.errors import APIError, ClientError
            self._client = genai.Client(api_key=api_key)
            self._model_name = model
            self._types = genai_types
            self._APIError = APIError
            self._ClientError = ClientError
        except ImportError as e:
            logger.error("google_import_error", error=str(e))
            raise LLMConfigurationError("Google GenAI package not installed. Run: pip install google-genai") from e

    @property
    def provider_name(self) -> str:
        return f"google/{self._model_name}"

    async def complete(self, system_prompt: str, user_prompt: str) -> str:
        import asyncio

        try:
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
            
            if not response or not response.text:
                raise LLMResponseError("Google Gemini returned empty response")
            
            if not response.text.strip():
                raise LLMResponseError("Google Gemini returned empty content")
            
            return response.text
            
        except getattr(self, '_APIError', Exception) as e:
            logger.error("google_api_error", error=str(e), model=self._model_name)
            raise LLMProviderError(f"Google Gemini API error: {str(e)}") from e
        except getattr(self, '_ClientError', Exception) as e:
            logger.error("google_client_error", error=str(e), model=self._model_name)
            raise LLMProviderError(f"Google Gemini client error: {str(e)}") from e
        except Exception as e:
            logger.error("google_unexpected_error", error=str(e), model=self._model_name, exc_info=True)
            raise LLMProviderError(f"Unexpected Google Gemini error: {str(e)}") from e


# ─────────────────────────────────────────────────────────────────────────────
# Factory
# ─────────────────────────────────────────────────────────────────────────────

def build_llm_client() -> LLMClient:
    """
    Read config.api_provider and instantiate the matching client.
    Called once at startup; result cached as module-level singleton.
    
    Raises:
        LLMConfigurationError: If provider configuration is invalid or missing
    """
    try:
        cfg = get_settings()
        provider = cfg.api_provider.lower().strip()

        if provider == "openai":
            if not cfg.openai_api_key:
                raise LLMConfigurationError(
                    "api_provider=openai but OPENAI_API_KEY is not set or empty."
                )
            if not cfg.openai_model:
                logger.warning("openai_model_not_set", using_default="gpt-4o-mini")
            client = OpenAIClient(
                api_key=cfg.openai_api_key, 
                model=cfg.openai_model or "gpt-4o-mini"
            )

        elif provider == "anthropic":
            if not cfg.anthropic_api_key:
                raise LLMConfigurationError(
                    "api_provider=anthropic but ANTHROPIC_API_KEY is not set or empty."
                )
            if not cfg.anthropic_model:
                logger.warning("anthropic_model_not_set", using_default="claude-3-haiku-20240307")
            client = AnthropicClient(
                api_key=cfg.anthropic_api_key, 
                model=cfg.anthropic_model or "claude-3-haiku-20240307"
            )

        elif provider == "google":
            if not cfg.google_api_key:
                raise LLMConfigurationError(
                    "api_provider=google but GOOGLE_API_KEY is not set or empty."
                )
            if not cfg.google_model:
                logger.warning("google_model_not_set", using_default="gemini-1.5-flash")
            client = GeminiClient(
                api_key=cfg.google_api_key, 
                model=cfg.google_model or "gemini-1.5-flash"
            )

        else:
            raise LLMConfigurationError(
                f"Unknown api_provider='{provider}'. "
                f"Choose one of: openai, anthropic, google"
            )

        logger.info("llm_client_initialized", provider=client.provider_name)
        return client
        
    except LLMConfigurationError:
        # Re-raise configuration errors as-is
        raise
    except Exception as e:
        logger.error("llm_client_build_error", error=str(e), exc_info=True)
        raise LLMConfigurationError(f"Failed to initialize LLM client: {str(e)}") from e


# Module-level singleton — built on first use
_llm_client: LLMClient | None = None


def get_llm_client() -> LLMClient:
    """
    Get the singleton LLM client instance.
    
    Returns:
        LLMClient: The initialized client instance
        
    Raises:
        LLMConfigurationError: If client initialization fails
    """
    global _llm_client
    try:
        if _llm_client is None:
            _llm_client = build_llm_client()
        return _llm_client
    except LLMConfigurationError:
        raise
    except Exception as e:
        logger.error("get_llm_client_error", error=str(e), exc_info=True)
        raise LLMConfigurationError(f"Failed to get LLM client: {str(e)}") from e


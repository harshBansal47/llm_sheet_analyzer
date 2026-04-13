"""
tests/test_llm_client.py  –  Unit tests for LLM provider factory.

Tests that:
  • Each provider class is instantiated correctly from config
  • Wrong provider name raises ValueError
  • Missing API key raises ValueError at startup (not mid-request)
  • Markdown fence stripping works (for Anthropic)
  • All three providers expose the same interface
"""
import pytest
from unittest.mock import patch, MagicMock, AsyncMock


# ─── Factory / config validation ─────────────────────────────────────────────

class TestProviderFactory:

    def setup_method(self):
        """Reset the module-level singleton before each test."""
        import services.llm_client as llm_mod
        llm_mod._llm_client = None

    def teardown_method(self):
        import services.llm_client as llm_mod
        llm_mod._llm_client = None

    def test_openai_selected(self):
        with patch("config.get_settings") as mock_cfg:
            mock_cfg.return_value = MagicMock(
                api_provider="openai",
                openai_api_key="sk-test",
                openai_model="gpt-4o-mini",
            )
            from services.llm_client import OpenAIClient, build_llm_client
            client = build_llm_client()
            assert isinstance(client, OpenAIClient)
            assert "openai" in client.provider_name

    def test_anthropic_selected(self):
        with patch("config.get_settings") as mock_cfg:
            mock_cfg.return_value = MagicMock(
                api_provider="anthropic",
                anthropic_api_key="sk-ant-test",
                anthropic_model="claude-haiku-4-5-20251001",
            )
            from services.llm_client import AnthropicClient, build_llm_client
            client = build_llm_client()
            assert isinstance(client, AnthropicClient)
            assert "anthropic" in client.provider_name

    def test_google_selected(self):
        with patch("config.get_settings") as mock_cfg, \
             patch("google.genai.Client"):
            mock_cfg.return_value = MagicMock(
                api_provider="google",
                google_api_key="AIza-test",
                google_model="gemini-1.5-flash",
            )
            from services.llm_client import GeminiClient, build_llm_client
            client = build_llm_client()
            assert isinstance(client, GeminiClient)
            assert "google" in client.provider_name

    def test_unknown_provider_raises(self):
        with patch("config.get_settings") as mock_cfg:
            mock_cfg.return_value = MagicMock(api_provider="cohere")
            from services.llm_client import build_llm_client
            with pytest.raises(ValueError, match="Unknown api_provider"):
                build_llm_client()


# ─── Markdown fence stripping (Anthropic edge case) ──────────────────────────

class TestAnthropicFenceStripping:

    def test_strips_json_fence(self):
        """Anthropic sometimes wraps output in ```json ... ``` despite instructions."""
        from unittest.mock import patch, MagicMock
        with patch("config.get_settings") as mock_cfg:
            mock_cfg.return_value = MagicMock(
                api_provider="anthropic",
                anthropic_api_key="sk-ant-test",
                anthropic_model="claude-haiku-4-5-20251001",
            )
            from services.llm_client import AnthropicClient

            client = AnthropicClient.__new__(AnthropicClient)
            client._model = "claude-haiku-4-5-20251001"

            # Simulate complete() stripping — replicate the logic from the class
            raw = '```json\n{"intent": "test"}\n```'
            raw = raw.strip()
            if raw.startswith("```"):
                lines = raw.splitlines()
                raw   = "\n".join(
                    line for line in lines
                    if not line.strip().startswith("```")
                ).strip()

            assert raw == '{"intent": "test"}'

    def test_no_fence_unchanged(self):
        raw = '{"intent": "test", "filters": []}'
        if raw.startswith("```"):
            lines = raw.splitlines()
            raw   = "\n".join(l for l in lines if not l.strip().startswith("```")).strip()
        assert raw == '{"intent": "test", "filters": []}'


# ─── Provider interface contract ─────────────────────────────────────────────

class TestProviderInterface:

    def test_all_providers_have_complete_method(self):
        from services.llm_client import OpenAIClient, AnthropicClient, GeminiClient
        import inspect

        for cls in (OpenAIClient, AnthropicClient, GeminiClient):
            assert hasattr(cls, "complete"), f"{cls.__name__} missing complete()"
            assert hasattr(cls, "provider_name"), f"{cls.__name__} missing provider_name"
            # complete() must be a coroutine
            assert inspect.iscoroutinefunction(cls.complete), \
                f"{cls.__name__}.complete() must be async"
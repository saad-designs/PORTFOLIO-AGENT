import time

import model_router as mr


def test_groq_cascade_uses_only_live_catalog_models(monkeypatch):
    monkeypatch.setattr(mr, "_GROQ_LIVE_MODELS", {
        "openai/gpt-oss-120b", "qwen/qwen3-32b", "whisper-large-v3", "meta-llama/llama-guard-4-12b",
        "brand-new/chat-model",
    })
    models = mr.get_groq_text_models()
    assert "llama-3.3-70b-versatile" not in models  # retired id is never called
    assert models[:2] == ["openai/gpt-oss-120b", "qwen/qwen3-32b"]  # preference order kept
    assert "brand-new/chat-model" in models  # new chat models picked up automatically
    assert not any("whisper" in m or "guard" in m for m in models)


def test_groq_cascade_falls_back_to_builtin_list_without_catalog(monkeypatch):
    monkeypatch.setattr(mr, "_GROQ_LIVE_MODELS", None)
    assert mr.get_groq_text_models() == mr.GROQ_TEXT_MODELS


def test_gemini_candidates_filtered_by_live_catalog(monkeypatch):
    monkeypatch.setattr(mr, "_GEMINI_CATALOG_TS", time.time())  # no network refresh
    monkeypatch.setattr(mr, "_GEMINI_LIVE_MODELS", {
        "gemini-3.5-flash", "gemini-3.8-flash", "gemini-3.9-flash", "gemini-3.8-flash-image", "gemini-3.8-pro",
    })
    models = mr.get_gemini_models_for_task(mr.TASK_FAST_INGESTION)
    assert models[:2] == ["gemini-3.8-flash", "gemini-3.5-flash"]  # configured ones that exist
    assert "gemini-3.9-flash" in models  # newer Flash discovered
    assert "gemini-3.8-flash-image" not in models and "gemini-3.8-pro" not in models
    assert not any(m.startswith("gemini-2.5") for m in models)


def test_request_too_large_does_not_cool_down_model(monkeypatch):
    monkeypatch.setattr(mr, "MODEL_COOLDOWN_UNTIL", {})

    class TooLarge(Exception):
        status_code = 413

    mr.record_model_error("openai/gpt-oss-120b", TooLarge(
        "Error code: 413 - Request too large for model on tokens per minute (TPM): Limit 8000, "
        "Requested 74292 ... 'code': 'rate_limit_exceeded'"
    ))
    assert mr.is_model_available("openai/gpt-oss-120b")


def test_oversized_prompt_skips_groq_without_calling_api(monkeypatch):
    def fail_client():
        raise AssertionError("Groq must not be called for oversized prompts")

    monkeypatch.setattr(mr, "get_groq_client", fail_client)
    assert mr.generate_text_groq("x" * (mr.GROQ_MAX_INPUT_TOKENS * 4 + 100)) is None


def test_whatsapp_intel_is_clipped_to_batch_limit():
    import main

    clipped = main._clip_intel_text("a" * (main.MAX_INTEL_BATCH_CHARS + 5000))
    assert clipped.startswith("a" * 100)
    assert "trimmed 5000 chars" in clipped
    assert main._clip_intel_text("short") == "short"

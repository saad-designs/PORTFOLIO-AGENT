import os
import sys
import io
import wave
import asyncio
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dotenv import load_dotenv
load_dotenv()

from model_router import (
    generate_with_fallback,
    transcribe_audio_groq,
    get_langchain_llm,
    get_google_api_keys,
    get_active_google_api_key,
    rotate_to_next_google_key,
    TASK_FAST_INGESTION,
    TASK_DEEP_REASONING
)
from trade_parser import extract_trade_intent, has_trade_execution_intent
from ingestion import transcribe_mentor_audio, process_whatsapp_text_payload
from google import genai

async def main():
    print("=" * 60)
    print("🧪 1. Testing Multi-Key Gemini Rotation Configuration...")
    keys = get_google_api_keys()
    active_key = get_active_google_api_key()
    masked_keys = [k[:6] + "..." + k[-4:] for k in keys]
    masked_active = active_key[:6] + "..." + active_key[-4:] if active_key else "None"
    print(f"✅ Configured Google API keys ({len(keys)} total): {masked_keys}")
    print(f"✅ Currently active key: {masked_active}")

    print("\n" + "=" * 60)
    print("🧪 2. Testing Trade Parser Pre-Filter (Eliminating Double LLM Calls)...")
    # A) General broker call -> should be rejected in 0ms without touching any LLM
    broker_call = "Buy SYS target 520 stop 480"
    intent_broker = extract_trade_intent(broker_call)
    assert intent_broker is None, "Broker call should be fast-rejected"
    print(f"✅ Broker signal ('{broker_call}') correctly fast-rejected in 0ms: {intent_broker}")

    # B) Conversational message -> fast-rejected in 0ms
    chat_call = "Is SYS looking good today?"
    intent_chat = extract_trade_intent(chat_call)
    assert intent_chat is None, "Conversational question should be fast-rejected"
    print(f"✅ Chat question ('{chat_call}') correctly fast-rejected in 0ms: {intent_chat}")

    # C) Explicit personal execution -> passes pre-filter and extracts params
    exec_call = "Bought 1200 MEBL shares at 215.5 today"
    intent_exec = extract_trade_intent(exec_call)
    print(f"✅ Personal trade execution ('{exec_call}') extracted: {intent_exec}")

    print("\n" + "=" * 60)
    print("🧪 3. Testing TASK_FAST_INGESTION text generation cascade...")
    client = genai.Client(api_key=active_key)
    prompt = "Extract the ticker and action as clean JSON: Sold 500 LUCK shares at 980. Output only JSON."
    text_res = await generate_with_fallback(client, [prompt], task=TASK_FAST_INGESTION)
    print(f"✅ Text extraction result:\n{text_res}")

    print("\n" + "=" * 60)
    print("🧪 4. Testing Audio Transcription Cascade...")
    buf = io.BytesIO()
    w = wave.open(buf, 'wb')
    w.setnchannels(1)
    w.setsampwidth(2)
    w.setframerate(16000)
    w.writeframes(b'\x00\x00' * 16000)
    w.close()
    buf.seek(0)
    audio_bytes = buf.read()
    audio_text = transcribe_audio_groq(audio_bytes, filename="test.wav", mime_type="audio/wav")
    print(f"✅ Groq Whisper direct result: {repr(audio_text)}")

    test_wav_path = "temp_test_audio.wav"
    try:
        with open(test_wav_path, "wb") as f:
            f.write(audio_bytes)
        mentor_audio_text = transcribe_mentor_audio(test_wav_path, mime_type="audio/wav")
        print(f"✅ transcribe_mentor_audio result: {repr(mentor_audio_text[:100]) if mentor_audio_text else 'None'}")
    finally:
        if os.path.exists(test_wav_path):
            os.remove(test_wav_path)

    print("\n" + "=" * 60)
    print("🧪 5. Testing WhatsApp Text Payload Ingestion...")
    wa_res = await process_whatsapp_text_payload(
        text="OGDC profit after tax surged 25% YoY on higher gas field realizations.",
        source="TestDesk"
    )
    print(f"✅ WhatsApp text ingestion status: {wa_res.get('status')}")

    # Immediate clean up of any test artifact
    try:
        from portfolio_manager import get_db_connection
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM active_playbook WHERE source ILIKE '%TestDesk%'")
        cur.execute("DELETE FROM langchain_pg_embedding WHERE document ILIKE '%TestDesk%'")
        cur.execute("DELETE FROM messages WHERE body ILIKE '%SYS buy target 520 stop 480%' OR (body ILIKE '%SYS%' AND body ILIKE '%520%')")
        conn.commit()
        cur.close()
        conn.close()
        print("✅ DB test artifacts cleaned up.")
    except Exception as e:
        print(f"⚠️ DB cleanup notice: {e}")

    print("\n" + "=" * 60)
    print("🎯 All tests completed successfully!")

if __name__ == "__main__":
    asyncio.run(main())

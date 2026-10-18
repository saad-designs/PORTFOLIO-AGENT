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
    TASK_FAST_INGESTION,
    TASK_DEEP_REASONING
)
from trade_parser import extract_trade_intent
from ingestion import transcribe_mentor_audio, process_whatsapp_text_payload
from google import genai

async def main():
    print("=" * 60)
    print("🧪 1. Testing TASK_FAST_INGESTION text generation (Groq Llama 3.3)...")
    client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))
    prompt = "Extract the ticker and action as clean JSON: Sold 500 LUCK shares at 980. Output only JSON."
    text_res = await generate_with_fallback(client, [prompt], task=TASK_FAST_INGESTION)
    print(f"✅ Text extraction result:\n{text_res}")

    print("\n" + "=" * 60)
    print("🧪 2. Testing Trade Parser with Groq...")
    trade_res = extract_trade_intent("Bought 1200 MEBL shares at 215.5 today")
    print(f"✅ Trade Parser result:\n{trade_res}")

    print("\n" + "=" * 60)
    print("🧪 3. Testing Audio Transcription (Groq Whisper Large V3 Turbo from bytes)...")
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
    print(f"✅ Groq Whisper Turbo result:\n{repr(audio_text)}")

    print("\n" + "=" * 60)
    print("🧪 4. Testing transcribe_mentor_audio from file (routes to Groq Whisper)...")
    test_wav_path = "temp_test_audio.wav"
    try:
        with open(test_wav_path, "wb") as f:
            f.write(audio_bytes)
        mentor_audio_text = transcribe_mentor_audio(test_wav_path, mime_type="audio/wav")
        print(f"✅ transcribe_mentor_audio result:\n{repr(mentor_audio_text)}")
    finally:
        if os.path.exists(test_wav_path):
            os.remove(test_wav_path)

    print("\n" + "=" * 60)
    print("🧪 5. Testing WhatsApp Text Payload Ingestion (Groq Llama 3.3)...")
    wa_res = await process_whatsapp_text_payload(
        text="OGDC profit after tax surged 25% YoY on higher gas field realizations.",
        source="TestDesk"
    )
    # Clean up test artifact from TestDesk
    try:
        from portfolio_manager import get_db_connection
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM active_playbook WHERE source ILIKE '%TestDesk%'")
        cur.execute("DELETE FROM langchain_pg_embedding WHERE document ILIKE '%TestDesk%'")
        conn.commit()
        cur.close()
        conn.close()
    except Exception:
        pass

    print("\n" + "=" * 60)
    print("🎯 All tests completed successfully!")

if __name__ == "__main__":
    asyncio.run(main())

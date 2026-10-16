import os
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

raw_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
if not raw_key:
    print("❌ ERROR: No key found in .env")
    exit()

clean_key = raw_key.strip()
print(f"Key loaded: {clean_key[:10]}... (Length: {len(clean_key)})")

genai.configure(api_key=clean_key)
model = genai.GenerativeModel('gemini-2.5-flash')

try:
    print("Pinging Gemini API...")
    response = model.generate_content("Reply with exactly three words: 'API is operational.'")
    print(f"✅ SUCCESS: {response.text.strip()}")
except Exception as e:
    print(f"❌ ERROR: {e}")

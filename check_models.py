import os
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

# Configure the SDK with your API key
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

print("DIAGNOSTIC: Listing available models for your API key...")
for m in genai.list_models():
    if 'embedContent' in m.supported_generation_methods:
        print(f"Found Supported Embedding Model: {m.name}")

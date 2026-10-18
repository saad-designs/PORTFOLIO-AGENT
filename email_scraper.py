import os
import email
from email.header import decode_header
import imaplib
import tempfile
import asyncio
from datetime import datetime
from google import genai
import dateutil.parser
from portfolio_manager import get_checkpoint, update_checkpoint, get_db_connection, add_pending_signal

client = genai.Client(http_options={"timeout": 300000})

async def extract_and_analyze_email(raw_email_bytes: bytes) -> str:
    """
    Extracts text and PDF/Image attachments from an email, uploads them to Gemini,
    and returns a brutal, analytical summary.
    """
    msg = email.message_from_bytes(raw_email_bytes)
    
    # Safely extract subject
    subject = ""
    subject_header = decode_header(msg.get("Subject", ""))
    if subject_header and subject_header[0]:
        sub, encoding = subject_header[0]
        if isinstance(sub, bytes):
            subject = sub.decode(encoding or "utf-8", errors="ignore")
        else:
            subject = sub
            
    email_text = ""
    attachment_parts = []

    # 1. Traverse email parts to extract text and binary attachments
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            content_disposition = str(part.get("Content-Disposition", ""))

            # Handle body text
            if content_type == "text/plain" and "attachment" not in content_disposition:
                try:
                    email_text += part.get_payload(decode=True).decode("utf-8", errors="ignore")
                except Exception:
                    pass

            # Handle attachments (PDF, PNG, JPG)
            elif "attachment" in content_disposition or content_type in ["application/pdf", "image/png", "image/jpeg"]:
                filename = part.get_filename()
                if filename:
                    file_data = part.get_payload(decode=True)
                    if file_data:
                        attachment_parts.append((filename, content_type, file_data))
    else:
        # Non-multipart email
        if msg.get_content_type() == "text/plain":
            try:
                email_text = msg.get_payload(decode=True).decode("utf-8", errors="ignore")
            except Exception:
                pass

    EMAIL_PARTNER_PROMPT = """
You are a ruthless, quantitative investment partner managing a portfolio on the Pakistan Stock Exchange.
Analyze this institutional email (and any attached PDFs/graphics). 

DO NOT use meta-announcements like "Here is the summary" or "Based on the text". 
DO NOT use robotic numbered lists or generic headers.
Jump straight into the facts. Be brutally honest, highly analytical, and concise.

Format your response exactly like this:

**[Stock Ticker/Subject] | [The Core Catalyst]**
Write a sharp, 2-3 sentence paragraph explaining exactly what the data means for margins, EPS, or macro stability. Tell me if it's a value trap, a cyclical buy, or fundamental noise. 

**Execution:**
State clearly if this supports holding, dumping, or accumulating. Keep it blunt.

<SENTIMENT_MAPPING>
{"TICKER": "BULLISH/BEARISH/NEUTRAL"}
</SENTIMENT_MAPPING>
"""

    temp_files_to_cleanup = []
    uploaded_files = []
    try:
        # 3. Upload attachments to Gemini
        for filename, c_type, data in attachment_parts:
            suffix = os.path.splitext(filename)[1] if filename else ""
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp.write(data)
                tmp_path = tmp.name
                temp_files_to_cleanup.append(tmp_path)
            
            print(f"📧 [Email Parser] Uploading attachment {filename} to Gemini...")
            uploaded_file = await asyncio.to_thread(client.files.upload, file=tmp_path)
            uploaded_files.append(uploaded_file)

        # 4. Pass EVERYTHING to the model
        gemini_payload = [EMAIL_PARTNER_PROMPT] + uploaded_files + [f"Subject: {subject}\nBody: {email_text}"]
        
        # 5. Call Gemini via fallback wrapper
        from model_router import generate_with_fallback, TASK_FAST_INGESTION
        response_text = await generate_with_fallback(client, gemini_payload, task=TASK_FAST_INGESTION)

        import re
        import json
        
        # 1. Clean the Discord message robustly
        discord_friendly_message = re.sub(r'<SENTIMENT_MAPPING>.*?(</SENTIMENT_MAPPING>|$)', '', response_text, flags=re.DOTALL).strip()
        
        # 2. Extract the JSON payload for your database
        match = re.search(r"<SENTIMENT_MAPPING>\s*(\{.*?\})", response_text, re.DOTALL)
        if match:
            try:
                sentiment_mapping = json.loads(match.group(1).strip())
                summary_text = discord_friendly_message
                conn = get_db_connection()
                cur = conn.cursor()
                for tick, sent in sentiment_mapping.items():
                    cur.execute("""
                        INSERT INTO institutional_reports (source, ticker, subject, sentiment, summary)
                        VALUES (%s, %s, %s, %s, %s)
                    """, ("Institutional Email", tick, subject, sent, summary_text))
                    
                    if sent in ["BULLISH", "BEARISH", "NEUTRAL"]:
                        cur.execute(
                            "UPDATE portfolio_holdings SET research_sentiment = %s, sentiment_updated_at = CURRENT_TIMESTAMP WHERE ticker = %s",
                            (sent, tick)
                        )
                conn.commit()
                cur.close()
                conn.close()
                print(f"📥 Saved institutional report sentiments for {list(sentiment_mapping.keys())}")
            except Exception as e:
                print(f"⚠️ Failed to parse/save sentiment mapping: {e}")

        return discord_friendly_message

    except Exception as e:
        print(f"❌ [Email Processing Error]: {e}")
        return f"Error analyzing email: {e}"
    finally:
        # Cleanup Routine
        for file_path in temp_files_to_cleanup:
            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception as cleanup_err:
                    print(f"⚠️ Failed to delete temp file {file_path}: {cleanup_err}")

async def run_email_sweep() -> str:
    """Fetches corporate briefings and research reports from KTrade via the dedicated email inbox chronologically."""
    EMAIL_USER = os.getenv("EMAIL_USER")
    EMAIL_PASS = os.getenv("EMAIL_PASS")
    
    if not EMAIL_USER or not EMAIL_PASS:
        return "Tool Execution Error: Email credentials (EMAIL_USER or EMAIL_PASS) are not set in the .env file."

    last_timestamp = get_checkpoint("email_last_timestamp")
    highest_timestamp = last_timestamp

    def do_imap():
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(EMAIL_USER, EMAIL_PASS)
        mail.select("inbox")
        
        status, messages = mail.search(None, '(FROM "research@ktrade.pk")')
        
        if status != "OK" or not messages[0]:
            mail.logout()
            return []
            
        parts = []
        for num in messages[0].split():
            res, msg_data = mail.fetch(num, "(RFC822)")
            for response_part in msg_data:
                if isinstance(response_part, tuple):
                    msg = email.message_from_bytes(response_part[1])
                    date_header = msg.get("Date")
                    try:
                        email_ts = int(dateutil.parser.parse(date_header).timestamp())
                    except Exception:
                        email_ts = 0

                    if email_ts > last_timestamp:
                        subject, encoding = decode_header(msg["Subject"])[0]
                        if isinstance(subject, bytes):
                            subject = subject.decode(encoding if encoding else "utf-8")
                        parts.append((subject, response_part[1], email_ts))
                    
        mail.logout()
        return parts
        
    try:
        parts = await asyncio.to_thread(do_imap)
        if not parts:
            return "No new emails found."

        # Sort chronologically
        parts.sort(key=lambda x: x[2])

        extracted_reports = []
        for subject, raw_bytes, email_ts in parts:
            body = await extract_and_analyze_email(raw_bytes)
            extracted_reports.append(f"Subject: {subject}\nBody: {body}\n---")
            highest_timestamp = max(highest_timestamp, email_ts)
            
            # Route intelligence to pending_signals
            add_pending_signal("Institutional Email", {"subject": subject, "report": body})

        if highest_timestamp > last_timestamp:
            update_checkpoint("email_last_timestamp", highest_timestamp)
            
        return "\n\n".join(extracted_reports)
        
    except Exception as e:
        return f"Tool Execution Error: Failed to fetch emails. Details: {str(e)}"

if __name__ == "__main__":
    import asyncio
    from portfolio_manager import send_learning_brief
    print("🚀 [Diagnostics] Forcing manual Email sweep...")
    
    async def manual_test():
        results = await run_email_sweep() 
        
        if results and "No new emails found" not in results and "Tool Execution Error" not in results:
            print(f"✅ Found data.")
            # Send the educational brief to Discord
            await send_learning_brief(
                source="Institutional Email",
                title="Email Report",
                summary=f"**Key Takeaways:**\n{results[:500]}..."
            )
        else:
            print("⚠️ No new emails or transcripts found.")

    asyncio.run(manual_test())

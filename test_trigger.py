import asyncio
import json
from schedulers import evaluate_asymmetric_triggers, conduct_autonomous_audit

async def mock_send_message(text: str):
    print("\n" + "="*60)
    print("📢 [DISCORD DISPATCH TEST]")
    print("="*60)
    print(text)
    print("="*60 + "\n")

async def run_dry_test():
    print("🧪 Starting Isolated Asymmetric Alert & Audit Test...\n")
    
    # 1. Direct Audit Test (Deep Research & RAG Pipeline)
    print("--- Stage 2 Direct Audit Test: HUBC Entry Breach ---")
    mock_ticker = "HUBC"
    mock_trigger = "ACCUMULATION_ENTRY"
    mock_price = 195.00
    mock_ref = 205.00

    audit_result = await conduct_autonomous_audit(
        ticker=mock_ticker,
        trigger_type=mock_trigger,
        current_price=mock_price,
        reference_price=mock_ref
    )

    print(f"Audit Verdict: {audit_result.get('verdict')}")
    print(f"Reasoning: {audit_result.get('reasoning_summary')}")
    print("Parsed JSON Payload:")
    print(json.dumps(audit_result, indent=2))

    # 2. Gatekeeper Loop Test (Live Price Tape Evaluation)
    print("\n--- Stage 1 Gatekeeper Pipeline Test ---")
    mock_live_tape = {
        "HUBC": 195.00,  # Below entry
        "SYS": 140.00,   # Nominal
        "LUCK": 410.00   # Below Rs. 425 support floor
    }
    
    await evaluate_asymmetric_triggers(mock_live_tape, mock_send_message)
    print("\n✅ Dry run execution complete.")

if __name__ == "__main__":
    asyncio.run(run_dry_test())

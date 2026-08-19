import time
import logging
from vector_brain import ingest_knowledge_tool

logger = logging.getLogger("ManualBackfill")

# Synthesized Mentor Strategic Frameworks
mentor_frameworks = [
    {
        "mentor": "Abdul Rehman Najam (ARN)",
        "strategy": "Value Investing & All-Weather Portfolio",
        "content": "Focuses on Warren Buffett's value investing principles applied to the PSX. Utilizes a 10-step fundamental analysis process to identify future-leading companies and early-stage multibaggers. Emphasizes building an 'all-weather portfolio' using a buffer strategy to survive Pakistan's geopolitical and macroeconomic boom-bust cycles. Strong focus on independent thinking and avoiding herd mentality."
    },
    {
        "mentor": "Jawad Hafeez",
        "strategy": "Entrepreneurial Mindset & Business Dynamics",
        "content": "Approaches the stock market strictly through an entrepreneurial lens. Emphasizes understanding the deep fundamentals and business dynamics of a company rather than trading short-term price action. Prioritizes psychological discipline, long-term wealth creation, and balancing life with sustainable business growth."
    },
    {
        "mentor": "InvestKaar",
        "strategy": "Bottom-Up Active Growth Investing",
        "content": "Relies heavily on bottom-up active investing and growth-style portfolio management. Strong focus on knowing the investor's specific risk profile. Strategy is built around maximizing the compounding of earnings and consistently reinvesting dividends to achieve long-term capital gains."
    },
    {
        "mentor": "Sarmaaya & AKD (Market Baseline)",
        "strategy": "Macro Diversification & Risk Tolerance",
        "content": "Advocates for strict diversification across various sectors to minimize idiosyncratic risk. Advises buying during upswings and avoiding the herd mentality driven by greed in bull markets and fear in bear markets. Focuses on structural reforms, seasonal booms, and tracking financial statements."
    }
]

def execute_manual_backfill():
    print("🧠 Initiating Direct Knowledge Injection to Vector Brain...")
    
    for item in mentor_frameworks:
        print(f"Loading framework for: {item['mentor']}...")
        
        # Format the payload for the embedding model
        payload = f"MENTOR STRATEGY | Source: {item['mentor']} | Core Strategy: {item['strategy']} | Details: {item['content']}"
        
        try:
            # Inject directly into ChromaDB via the existing tool
            ingest_knowledge_tool.invoke({"input_text_or_url": payload})
            print(f"✅ {item['mentor']} embedded successfully.")
        except Exception as e:
            logger.error(f"❌ Failed to embed {item['mentor']}: {str(e)}")
            
        time.sleep(2) # Brief pause to ensure clean database writes

    print("🚀 Manual backfill complete. The agent's memory is fully updated.")

if __name__ == "__main__":
    execute_manual_backfill()

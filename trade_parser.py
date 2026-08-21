import os
from pydantic import BaseModel, Field
from typing import Literal, Optional
from langchain_google_genai import ChatGoogleGenerativeAI
from dotenv import load_dotenv

load_dotenv()

class TradeExecution(BaseModel):
    is_trade_execution: bool = Field(
        description="True ONLY if the user is explicitly reporting an executed BUY or SELL transaction with specific numbers. False for questions, outlooks, or general conversation."
    )
    action: Optional[Literal["BUY", "SELL"]] = None
    ticker: Optional[str] = Field(default=None, description="The PSX stock ticker symbol (e.g. SYS). None if not a trade.")
    shares: Optional[float] = Field(default=None, description="Exact number of shares executed. None if not a trade.")
    price: Optional[float] = Field(default=None, description="Execution price per share. None if not a trade.")

def extract_trade_intent(message_content: str) -> TradeExecution | None:
    """
    Parses a natural language message and extracts trade parameters safely into a Pydantic object.
    Returns None if the message is not a trade intent.
    """
    try:
        # Use an active LangChain Gemini instance
        llm = ChatGoogleGenerativeAI(
            model="gemini-3.5-flash-lite", 
            google_api_key=os.getenv("GOOGLE_API_KEY")
        )
        
        # We instruct the model strictly about its extraction task
        parser = llm.with_structured_output(TradeExecution)
        
        prompt = (
            "Analyze the following user message from a Discord channel. "
            "If the user is stating that they executed a trade (bought or sold shares of a stock), "
            "extract the exact parameters into the schema. "
            "If the message is conversational, asking a question, or NOT explicitly about executing a trade, "
            "set is_trade_execution to false.\n\n"
            f"Message: {message_content}"
        )
        
        result = parser.invoke(prompt)
        
        # Strict gatekeeper: If it's not explicitly a trade, or missing core numbers, discard immediately
        if (
            not getattr(result, "is_trade_execution", False)
            or not result.ticker
            or not result.shares
            or result.shares <= 0
            or not result.price
            or result.price <= 0
            or not result.action
        ):
            return None
            
        return result
    except Exception as e:
        # PyDantic parsing failure means it's not a trade or missing params
        return None

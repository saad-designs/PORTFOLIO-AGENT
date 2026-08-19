import sys

with open("discord_bot.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

output = []
i = 0

while i < len(lines):
    line = lines[i]
    
    # 1. Imports addition
    if line.startswith("from langchain_core.messages import HumanMessage"):
        output.append(line)
        output.append("from langchain.tools import tool\n")
        output.append("from langchain_google_genai import ChatGoogleGenerativeAI\n")
        output.append("from langchain.agents import AgentExecutor, create_tool_calling_agent\n")
        output.append("from langchain_core.prompts import ChatPromptTemplate\n")
        i += 1
        continue
        
    # 2. Command deletions
    if line.startswith("@bot.command"):
        # skip this and all following indented lines
        i += 1
        while i < len(lines):
            # empty lines or indented lines belong to the function
            if lines[i].startswith(" ") or lines[i].startswith("\t") or lines[i].strip() == "":
                i += 1
            else:
                break
        continue

    # 3. on_message replacement
    if line.startswith("@bot.event") and i+1 < len(lines) and lines[i+1].startswith("async def on_message(message):"):
        # We hit on_message. We skip until the next top level statement
        i += 2
        while i < len(lines):
            if lines[i].startswith(" ") or lines[i].startswith("\t") or lines[i].strip() == "":
                i += 1
            else:
                break
            
        # Now we insert the tools, agent executor, and the new on_message
        agent_code = """
# --- LangChain Agent Setup ---
@tool
def fetch_portfolio_tool() -> str:
    \"\"\"Fetches the user's active PSX stock portfolio and real-time PnL from the Supabase database. Use this when the user asks about their holdings, profits, or live portfolio value.\"\"\"
    try:
        return get_portfolio_summary()
    except Exception as e:
        return f"Tool Execution Error: fetch_portfolio_tool failed. Details: {str(e)}"

@tool
def ingest_mentor_video(url: str) -> str:
    \"\"\"Extracts financial target prices and strategies from a given YouTube URL and adds them to the vector brain. Use this when the user asks to learn or ingest a new mentor video.\"\"\"
    try:
        match = re.search(r'(?:v=|youtu\.be/)([^&?]+)', url)
        if not match: return "Error: Invalid YouTube URL format."
        video_id = match.group(1)
        transcript = get_transcript(video_id)
        if not transcript: return "Error: Failed to extract transcript."
        
        chunks_saved = ingest_transcript("Custom URL", transcript)
        
        prompt = f"Analyze this financial transcript. Extract specific Pakistani stock mentions and targets.\\nFormat strictly as: !target TICKER TARGET_PRICE STOP_LOSS\\nIMPORTANT Tickers: {DYNAMIC_PSX_TICKER_MAPPING}\\nTranscript:\\n{transcript}"
        inputs = {"messages": [HumanMessage(content=prompt)]}
        # Run it synchronously since tools are synchronous
        response = agent_app.invoke(inputs, {"configurable": {"thread_id": "target_extraction"}})
        
        raw_content = response["messages"][-1].content
        if isinstance(raw_content, list):
            final_reply = "".join(b.get("text", "") for b in raw_content if isinstance(b, dict) and b.get("type") == "text")
        else:
            final_reply = str(raw_content)
            
        target_commands = [line.strip() for line in final_reply.split('\\n') if line.strip().startswith('!target')]
        
        res = f"KNOWLEDGE ACQUIRED: {chunks_saved} chunks saved from video.\\n"
        if target_commands: res += "Suggested Targets Found (you can manually log these):\\n" + "\\n".join(target_commands)
        return res
    except Exception as e:
        return f"Tool Execution Error: ingest_mentor_video failed. Details: {str(e)}"

@tool
def query_mentor_brain_tool(query: str) -> str:
    \"\"\"Queries the AI Vector Brain for past financial mentor advice. Use this when the user asks a question about strategies, market conditions, or mentor insights.\"\"\"
    try:
        return query_mentor_brain(query)
    except Exception as e:
        return f"Tool Execution Error: query_mentor_brain_tool failed. Details: {str(e)}"

llm = ChatGoogleGenerativeAI(model="gemini-3.5-flash")
tools = [fetch_portfolio_tool, ingest_mentor_video, query_mentor_brain_tool]

prompt = ChatPromptTemplate.from_messages([
    ("system", "You are a brutal, highly logical financial Portfolio Agent. Use the provided tools to fetch live data or ingest videos. Do not hallucinate financial metrics."),
    ("human", "{input}"),
    ("placeholder", "{agent_scratchpad}"),
])

agent = create_tool_calling_agent(llm, tools, prompt)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

@bot.event
async def on_message(message):
    # Prevent bot from replying to itself
    if message.author == bot.user:
        return

    # Trigger the agent to think, execute tools, and generate a response
    async with message.channel.typing():
        try:
            # We use ainvoke so it doesn't block the discord async loop
            response = await agent_executor.ainvoke({"input": message.content})
            
            final_reply = response["output"]
            
            # Send the response back seamlessly
            if final_reply:
                chunk_size = 1900
                for idx in range(0, len(final_reply), chunk_size):
                    await message.channel.send(final_reply[idx:idx + chunk_size])
        except Exception as e:
            await message.channel.send(f"⚠️ **Agent Error:** `{str(e)}`")
"""
        output.append(agent_code)
        continue

    output.append(line)
    i += 1

with open("discord_bot_new.py", "w", encoding="utf-8") as f:
    f.writelines(output)
print("done")

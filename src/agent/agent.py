"""
LangChain Agent for AssistFPL.

A conversational AI agent that uses tools to provide FPL insights and recommendations.
Uses Google's Gemini model for natural language understanding.
"""
import os
from typing import List, Optional
import structlog
from dotenv import load_dotenv

from src.database.db_manager import DBManager
from src.analytics.predictor import FPLPredictor
from src.analytics.insights import InsightGenerator
from src.agent.tools import FPLTools

load_dotenv()
logger = structlog.get_logger()


class FPLAgent:
    """
    Conversational agent for FPL assistance.
    
    Uses LangChain with Gemini to process natural language queries
    and call appropriate tools to fetch data and generate insights.
    """
    
    SYSTEM_PROMPT = """You are AssistFPL, an expert Fantasy Premier League assistant. 
You help users make informed FPL decisions about transfers, captain picks, and squad building.

You have access to the following tools:
1. get_predicted_points(player_name) - Get ML-predicted points for a player
2. get_player_insights(player_name, opponent_name?) - Get detailed insights about a player
3. get_top_picks(position?, max_price?, top_n?) - Get top predicted players
4. get_transfer_suggestion(player_out_name, budget?, same_position?) - Get transfer suggestions
5. get_player_vs_team_history(player_name, team_name) - Get historical performance vs a team

When answering questions:
- Always use the tools to get real data, don't make up statistics
- Be concise but informative
- Use emojis sparingly to highlight key points
- If you're unsure, say so rather than guessing
- Consider multiple factors: form, fixtures, price, and underlying stats

Common question types:
- "Should I transfer in X?" -> Use get_predicted_points and get_player_insights
- "Who should I captain?" -> Use get_top_picks and get_player_insights
- "Replacement for X?" -> Use get_transfer_suggestion
- "How does X do against Y?" -> Use get_player_vs_team_history
"""

    def __init__(self, db: DBManager, predictor: FPLPredictor, insight_generator: InsightGenerator):
        self.db = db
        self.predictor = predictor
        self.insight_generator = insight_generator
        self.tools = FPLTools(db, predictor, insight_generator)
        self.llm = None
        self._initialize_llm()

    def _initialize_llm(self):
        """Initialize the LLM (Gemini or Ollama)."""
        api_key = os.getenv("GOOGLE_API_KEY")
        ollama_model = os.getenv("OLLAMA_MODEL", "llama3")
        
        # 1. Try Google Gemini
        if api_key:
            try:
                from langchain_google_genai import ChatGoogleGenerativeAI
                self.llm = ChatGoogleGenerativeAI(
                    model="gemini-pro",
                    google_api_key=api_key,
                    temperature=0.7,
                    convert_system_message_to_human=True
                )
                logger.info("llm_initialized_gemini", model="gemini-pro")
                return
            except Exception as e:
                logger.error("gemini_init_failed", error=str(e))
                
        # 2. Try Ollama (Local)
        try:
            from langchain_community.chat_models import ChatOllama
            
            # Simple check if we can import it, actual connection check happens on invoke
            self.llm = ChatOllama(
                model=ollama_model,
                temperature=0.7
            )
            logger.info("llm_initialized_ollama", model=ollama_model)
        except ImportError:
            logger.warning("langchain_community_not_installed")
        except Exception as e:
            logger.error("ollama_init_failed", error=str(e))

    async def _call_tool(self, tool_name: str, **kwargs) -> str:
        """Call a tool by name with given arguments."""
        tool_map = {
            "get_predicted_points": self.tools.get_predicted_points,
            "get_player_insights": self.tools.get_player_insights,
            "get_top_picks": self.tools.get_top_picks,
            "get_transfer_suggestion": self.tools.get_transfer_suggestion,
            "get_player_vs_team_history": self.tools.get_player_vs_team_history,
        }
        
        tool = tool_map.get(tool_name)
        if not tool:
            return f"Unknown tool: {tool_name}"
        
        try:
            result = await tool(**kwargs)
            return result
        except Exception as e:
            logger.error("tool_error", tool=tool_name, error=str(e))
            return f"Error calling {tool_name}: {str(e)}"

    async def _parse_and_execute_intent(self, message: str) -> str:
        """
        Simple intent detection and tool execution.
        This is a fallback when no LLM is available.
        """
        message_lower = message.lower()
        
        # Detect intent and call appropriate tool
        if "captain" in message_lower or "top pick" in message_lower or "best player" in message_lower:
            position = None
            if "forward" in message_lower or "fwd" in message_lower:
                position = "FWD"
            elif "mid" in message_lower:
                position = "MID"
            elif "def" in message_lower:
                position = "DEF"
            elif "gk" in message_lower or "keeper" in message_lower:
                position = "GKP"
            
            return await self.tools.get_top_picks(position=position, top_n=5)
        
        elif "replace" in message_lower or "transfer out" in message_lower or "instead of" in message_lower:
            # Try to extract player name
            words = message.split()
            for i, word in enumerate(words):
                if word.lower() in ["replace", "replacing", "instead"]:
                    if i + 2 < len(words):
                        player_name = words[i + 1] + " " + words[i + 2] if words[i + 1].lower() == "of" else words[i + 1]
                        return await self.tools.get_transfer_suggestion(player_name.strip("?.,!"))
            return "I couldn't identify which player you want to replace. Please specify, e.g., 'Who should I get instead of Salah?'"
        
        elif "insight" in message_lower or "tell me about" in message_lower or "how is" in message_lower:
            # Try to extract player name
            words = message.split()
            for i, word in enumerate(words):
                if word.lower() in ["about", "is"]:
                    if i + 1 < len(words):
                        player_name = words[i + 1].strip("?.,!")
                        return await self.tools.get_player_insights(player_name)
            return "I couldn't identify which player you're asking about. Please specify, e.g., 'Tell me about Salah'"
        
        elif "predict" in message_lower or "points" in message_lower or "score" in message_lower:
            # Try to extract player name
            words = message.split()
            for word in words:
                if word[0].isupper() and len(word) > 2 and word.lower() not in ["how", "many", "will", "the"]:
                    return await self.tools.get_predicted_points(word.strip("?.,!"))
            return "I couldn't identify which player you're asking about. Please specify, e.g., 'How many points will Haaland score?'"
        
        elif "vs" in message_lower or "against" in message_lower or "history" in message_lower:
            # Try to extract player and team
            words = message.split()
            try:
                if "vs" in message_lower:
                    idx = words.index("vs") if "vs" in words else words.index("VS")
                elif "against" in message_lower:
                    idx = next(i for i, w in enumerate(words) if w.lower() == "against")
                else:
                    idx = -1
                
                if idx > 0 and idx < len(words) - 1:
                    player = words[idx - 1].strip("?.,!")
                    team = words[idx + 1].strip("?.,!")
                    return await self.tools.get_player_vs_team_history(player, team)
            except (ValueError, StopIteration):
                pass
            return "I couldn't parse your request. Try: 'Salah vs Man United' or 'Haaland against Arsenal'"
        
        elif any(w in message_lower for w in ["hi", "hello", "hey", "greetings"]):
            return "👋 Hi there! I'm AssistFPL. I can help you with:\n- Top predicted players\n- Transfer suggestions\n- Player comparisons\n\nTry asking: 'Who are the best forwards?' or 'Should I buy Salah?'"

        elif "help" in message_lower:
             return "I can help you manage your FPL team. Try commands like:\n- 'Top picks for defenders'\n- 'Analyze Haaland'\n- 'Replacements for Palmer'\n- 'Haaland vs Salah'"

        else:
            # Default: show top picks
            return "I'm not sure what you're asking. Try asking for 'top picks' or 'player stats'.\n\nHere are the current top predicted players:\n\n" + await self.tools.get_top_picks(top_n=5)

    async def chat(self, message: str) -> str:
        """
        Process a user message and return a response.
        Uses a RAG-lite approach: fetches context first, then uses LLM to answer.
        """
        # 1. Gather Context (RAG)
        context = ""
        
        # Try to identify potential player names in the message
        # This is a simple heuristic; a clear improvement would be using NER
        words = message.split()
        potential_names = [w for w in words if w[0].isupper() and len(w) > 2]
        
        for name in potential_names:
            # Try to get player data
            player = await self.tools.get_player_by_name(name.strip("?.,!"))
            if player:
                # Found a player, get deep insights
                insights = await self.tools.get_player_insights(player['web_name'])
                prediction = await self.tools.get_predicted_points(player['web_name'])
                context += f"\nData for {player['web_name']}:\n{insights}\n{prediction}\n"
        
        # If asking about top picks/transfer without specific player context
        msg_lower = message.lower()
        if any(w in msg_lower for w in ["captain", "best", "good", "top", "pick", "recommend"]):
            if not context:
                # Check for specific position request
                pos = None
                if "forward" in msg_lower or "striker" in msg_lower or "attacker" in msg_lower:
                    pos = "FWD"
                elif "mid" in msg_lower:
                    pos = "MID"
                elif "def" in msg_lower or "back" in msg_lower:
                    pos = "DEF"
                elif "leap" in msg_lower or "gkp" in msg_lower or "goal" in msg_lower: # "goal" captures goalkeeper/goalie
                    if "keeper" in msg_lower or "goalie" in msg_lower or "gkp" in msg_lower:
                        pos = "GKP"

                top_picks = await self.tools.get_top_picks(position=pos, top_n=5)
                context += f"\nTop Predicted Players"
                if pos: context += f" ({pos})"
                context += f":\n{top_picks}\n"

        if self.llm is None:
            # Fallback mode: simple intent detection if no LLM
            if context:
                return f"Here is the data I found:\n{context}"
            return await self._parse_and_execute_intent(message)
        
        # 2. LLM Response
        try:
            from langchain_core.messages import HumanMessage, SystemMessage
            
            system_prompt = self.SYSTEM_PROMPT
            if context:
                system_prompt += f"\n\nCONTEXT DATA (Use this to answer):\n{context}"
            
            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=message)
            ]
            
            response = await self.llm.ainvoke(messages)
            return response.content
            
        except Exception as e:
            logger.error("chat_error", error=str(e))
            # Fallback
            if context:
                return f"I had trouble processing that with AI, but here is the data:\n{context}"
            return await self._parse_and_execute_intent(message)

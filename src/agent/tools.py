"""
Agent Tools for AssistFPL.

These are Python functions exposed to the LLM agent for tool-use.
The agent can call these to fetch data, make predictions, and generate insights.
"""
from typing import List, Optional, Dict, Any
from src.database.db_manager import DBManager
from src.analytics.predictor import FPLPredictor
from src.analytics.insights import InsightGenerator


class FPLTools:
    """Collection of tools for the FPL Agent."""
    
    def __init__(self, db: DBManager, predictor: FPLPredictor, insight_generator: InsightGenerator):
        self.db = db
        self.predictor = predictor
        self.insight_generator = insight_generator

    async def get_player_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Find a player by their name (web_name, first_name, or second_name).
        
        Args:
            name: The player's name to search for
            
        Returns:
            Player data if found, None otherwise
        """
        query = """
        SELECT * FROM players 
        WHERE LOWER(web_name) LIKE LOWER($1)
           OR LOWER(first_name) LIKE LOWER($1)
           OR LOWER(second_name) LIKE LOWER($1)
        LIMIT 1;
        """
        async with self.db.pool.acquire() as conn:
            row = await conn.fetchrow(query, f"%{name}%")
            return dict(row) if row else None

    async def get_team_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """
        Find a team by their name or short_name.
        
        Args:
            name: The team's name to search for
            
        Returns:
            Team data if found, None otherwise
        """
        query = """
        SELECT * FROM teams 
        WHERE LOWER(name) LIKE LOWER($1)
           OR LOWER(short_name) LIKE LOWER($1)
        LIMIT 1;
        """
        async with self.db.pool.acquire() as conn:
            row = await conn.fetchrow(query, f"%{name}%")
            return dict(row) if row else None

    async def get_predicted_points(self, player_name: str) -> str:
        """
        Get the predicted points for a player.
        
        Args:
            player_name: Name of the player
            
        Returns:
            A string describing the predicted points
        """
        player = await self.get_player_by_name(player_name)
        if not player:
            return f"Could not find player '{player_name}'"
        
        prediction = await self.predictor.predict_player(player['id'])
        if prediction is None:
            return f"Could not generate prediction for {player['web_name']}"
        
        return f"{player['web_name']} is predicted to score {prediction:.1f} points. " \
               f"FPL's official estimate is {player.get('ep_next', 'N/A')} points."

    async def get_player_insights(self, player_name: str, opponent_name: Optional[str] = None) -> str:
        """
        Get insights about a player.
        
        Args:
            player_name: Name of the player
            opponent_name: Optional opponent team name for historical insights
            
        Returns:
            A string with insights about the player
        """
        player = await self.get_player_by_name(player_name)
        if not player:
            return f"Could not find player '{player_name}'"
        
        opponent_id = None
        if opponent_name:
            team = await self.get_team_by_name(opponent_name)
            if team:
                opponent_id = team['id']
        
        insights = await self.insight_generator.get_all_insights(player['id'], opponent_id)
        
        if not insights:
            return f"No insights available for {player['web_name']}"
        
        result = f"Insights for {player['web_name']}:\n"
        for insight in insights:
            emoji = "✅" if insight['sentiment'] == 'positive' else "⚠️" if insight['sentiment'] == 'negative' else "ℹ️"
            result += f"{emoji} {insight['insight']}\n"
        
        return result

    async def get_top_picks(self, position: Optional[str] = None, max_price: Optional[float] = None, top_n: int = 5) -> str:
        """
        Get top predicted players, optionally filtered by position and price.
        
        Args:
            position: Optional filter - "GKP", "DEF", "MID", or "FWD"
            max_price: Optional maximum price in millions
            top_n: Number of players to return
            
        Returns:
            A string listing the top picks
        """
        position_map = {"GKP": 1, "DEF": 2, "MID": 3, "FWD": 4}
        pos_id = position_map.get(position.upper()) if position else None
        
        all_predictions = await self.predictor.predict_all_players()
        
        filtered = all_predictions
        if pos_id:
            filtered = [p for p in filtered if p['position'] == pos_id]
        if max_price:
            filtered = [p for p in filtered if p['price'] <= max_price]
        
        top = filtered[:top_n]
        
        if not top:
            return "No players found matching your criteria."
        
        result = f"Top {len(top)} picks"
        if position:
            result += f" ({position.upper()})"
        if max_price:
            result += f" under £{max_price}m"
        result += ":\n"
        
        for i, p in enumerate(top, 1):
            pos_name = {1: "GKP", 2: "DEF", 3: "MID", 4: "FWD"}.get(p['position'], "?")
            result += f"{i}. {p['web_name']} ({pos_name}) - £{p['price']}m - Predicted: {p['predicted_points']:.1f} pts\n"
        
        return result

    async def get_transfer_suggestion(
        self, 
        player_out_name: str, 
        budget: Optional[float] = None,
        same_position: bool = True
    ) -> str:
        """
        Suggest a replacement for a player.
        
        Args:
            player_out_name: Name of the player to replace
            budget: Optional total budget (defaults to outgoing player's price + 0.5)
            same_position: Whether to only suggest same position replacements
            
        Returns:
            A string with transfer suggestions
        """
        player_out = await self.get_player_by_name(player_out_name)
        if not player_out:
            return f"Could not find player '{player_out_name}'"
        
        effective_budget = budget if budget else (player_out['now_cost'] / 10) + 0.5
        position = player_out['element_type'] if same_position else None
        
        suggestions = await self.predictor.get_transfer_suggestions(
            budget=effective_budget,
            position=position,
            exclude_players=[player_out['id']],
            top_n=5
        )
        
        if not suggestions:
            return f"No suitable replacements found for {player_out['web_name']} within £{effective_budget:.1f}m"
        
        result = f"Replacements for {player_out['web_name']} (£{player_out['now_cost']/10:.1f}m):\n"
        for i, s in enumerate(suggestions, 1):
            pos_name = {1: "GKP", 2: "DEF", 3: "MID", 4: "FWD"}.get(s['position'], "?")
            result += f"{i}. {s['web_name']} ({pos_name}) - £{s['price']}m - Predicted: {s['predicted_points']:.1f} pts\n"
        
        return result

    async def get_player_vs_team_history(self, player_name: str, team_name: str) -> str:
        """
        Get a player's historical performance against a specific team.
        
        Args:
            player_name: Name of the player
            team_name: Name of the opponent team
            
        Returns:
            A string describing historical performance
        """
        player = await self.get_player_by_name(player_name)
        if not player:
            return f"Could not find player '{player_name}'"
        
        team = await self.get_team_by_name(team_name)
        if not team:
            return f"Could not find team '{team_name}'"
        
        records = await self.db.get_player_vs_opponent(player['id'], team['id'])
        
        if not records:
            return f"No historical data found for {player['web_name']} vs {team['name']}"
        
        total_goals = sum(r['goals_scored'] for r in records)
        total_assists = sum(r['assists'] for r in records)
        total_points = sum(r['total_points'] for r in records)
        avg_points = total_points / len(records)
        
        return f"{player['web_name']} vs {team['name']} ({len(records)} matches):\n" \
               f"⚽ Goals: {total_goals}\n" \
               f"🅰️ Assists: {total_assists}\n" \
               f"📊 Avg Points: {avg_points:.1f}\n" \
               f"🏆 Total Points: {total_points}"


def get_tool_descriptions() -> List[Dict[str, str]]:
    """Get descriptions of all available tools for the LLM."""
    return [
        {
            "name": "get_predicted_points",
            "description": "Get the predicted FPL points for a player based on our ML model."
        },
        {
            "name": "get_player_insights",
            "description": "Get detailed insights about a player including form, availability, and value. Can also include historical insights against a specific opponent."
        },
        {
            "name": "get_top_picks",
            "description": "Get a list of top predicted players. Can filter by position (GKP/DEF/MID/FWD) and maximum price."
        },
        {
            "name": "get_transfer_suggestion",
            "description": "Suggest replacements for a player you want to transfer out."
        },
        {
            "name": "get_player_vs_team_history",
            "description": "Get a player's historical performance against a specific team."
        }
    ]

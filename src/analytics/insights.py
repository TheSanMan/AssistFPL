"""
Insight Generator for FPL Players.

Generates human-readable, rule-based insights about players and teams.
This is NOT an LLM—it's deterministic logic that produces facts.
Uses shared DBManager for database access.
"""
import structlog
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from src.database.db_manager import DBManager

logger = structlog.get_logger()


@dataclass
class PlayerInsight:
    """A single insight about a player."""
    category: str  # e.g., "form", "fixture", "historical"
    insight: str   # Human-readable text
    sentiment: str # "positive", "negative", "neutral"


class InsightGenerator:
    """Generates contextual insights from FPL data."""
    
    def __init__(self, db: DBManager):
        """
        Initialize with shared DBManager.
        
        Args:
            db: Shared database manager (dependency injection)
        """
        self.db = db

    async def get_player_info(self, player_id: int) -> Optional[Dict[str, Any]]:
        """Get basic player information with team name."""
        query = """
        SELECT p.id, p.web_name, p.first_name, p.second_name, 
               p.element_type, p.now_cost, p.status, p.news,
               p.form, p.ict_index, p.ep_next, p.total_points,
               t.name as team_name, t.short_name as team_short
        FROM players p
        JOIN teams t ON p.team_id = t.id
        WHERE p.id = $1;
        """
        async with self.db.pool.acquire() as conn:
            row = await conn.fetchrow(query, player_id)
            return dict(row) if row else None

    async def get_recent_form(self, player_id: int, n_games: int = 5) -> List[Dict[str, Any]]:
        """Get recent gameweek performances."""
        query = """
        SELECT gh.gameweek, gh.total_points, gh.minutes, 
               gh.goals_scored, gh.assists, gh.expected_goals, gh.expected_assists,
               t.short_name as opponent
        FROM gameweek_history gh
        JOIN teams t ON gh.opponent_team = t.id
        WHERE gh.player_id = $1 AND gh.minutes > 0
        ORDER BY gh.gameweek DESC
        LIMIT $2;
        """
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch(query, player_id, n_games)
            return [dict(r) for r in rows]

    async def get_vs_opponent_stats(self, player_id: int, opponent_id: int) -> Dict[str, Any]:
        """Get historical stats against a specific opponent."""
        records = await self.db.get_player_vs_opponent(player_id, opponent_id)
        
        if not records:
            return {"matches": 0, "goals": 0, "assists": 0, "avg_points": 0}
        
        return {
            "matches": len(records),
            "goals": sum(r['goals_scored'] for r in records),
            "assists": sum(r['assists'] for r in records),
            "avg_points": sum(r['total_points'] for r in records) / len(records)
        }

    async def generate_form_insights(self, player_id: int) -> List[PlayerInsight]:
        """Generate insights about recent form."""
        insights = []
        recent = await self.get_recent_form(player_id, 5)
        
        if not recent:
            return insights
        
        player = await self.get_player_info(player_id)
        name = player['web_name'] if player else "Player"
        
        # Recent points trend
        points = [g['total_points'] for g in recent]
        avg_points = sum(points) / len(points)
        
        if avg_points >= 6:
            insights.append(PlayerInsight(
                category="form",
                insight=f"{name} is in excellent form, averaging {avg_points:.1f} points over the last {len(recent)} games.",
                sentiment="positive"
            ))
        elif avg_points <= 2:
            insights.append(PlayerInsight(
                category="form",
                insight=f"{name} is struggling, averaging only {avg_points:.1f} points over the last {len(recent)} games.",
                sentiment="negative"
            ))
        
        # Goal/Assist involvement
        goals = sum(g['goals_scored'] for g in recent)
        assists = sum(g['assists'] for g in recent)
        if goals + assists >= 3:
            insights.append(PlayerInsight(
                category="form",
                insight=f"{name} has {goals} goals and {assists} assists in the last {len(recent)} games.",
                sentiment="positive"
            ))
        
        # xG vs actual goals
        xg_sum = sum(float(g['expected_goals'] or 0) for g in recent)
        if goals > xg_sum + 1:
            insights.append(PlayerInsight(
                category="form",
                insight=f"{name} is overperforming xG ({goals} goals vs {xg_sum:.1f} xG) - could regress.",
                sentiment="neutral"
            ))
        elif goals < xg_sum - 1:
            insights.append(PlayerInsight(
                category="form",
                insight=f"{name} is underperforming xG ({goals} goals vs {xg_sum:.1f} xG) - due for improvement.",
                sentiment="positive"
            ))
        
        return insights

    async def generate_availability_insights(self, player_id: int) -> List[PlayerInsight]:
        """Generate insights about player availability."""
        insights = []
        player = await self.get_player_info(player_id)
        
        if not player:
            return insights
        
        name = player['web_name']
        status = player['status']
        news = player['news']
        
        if status == 'a':
            insights.append(PlayerInsight(
                category="availability",
                insight=f"{name} is fully available with no injury concerns.",
                sentiment="positive"
            ))
        elif status == 'd':
            insights.append(PlayerInsight(
                category="availability",
                insight=f"{name} is doubtful. {news}" if news else f"{name} is flagged as doubtful.",
                sentiment="negative"
            ))
        elif status in ('i', 'u'):
            insights.append(PlayerInsight(
                category="availability",
                insight=f"{name} is unavailable. {news}" if news else f"{name} is currently unavailable.",
                sentiment="negative"
            ))
        
        return insights

    async def generate_value_insights(self, player_id: int) -> List[PlayerInsight]:
        """Generate insights about player value."""
        insights = []
        player = await self.get_player_info(player_id)
        
        if not player:
            return insights
        
        name = player['web_name']
        price = player['now_cost'] / 10
        total_points = player.get('total_points') or 0
        
        # Points per million (season)
        ppm = total_points / price if price > 0 else 0
        
        if ppm > 15:
            insights.append(PlayerInsight(
                category="value",
                insight=f"{name} offers excellent value at £{price:.1f}m with {ppm:.1f} points per million this season.",
                sentiment="positive"
            ))
        elif ppm < 5 and price > 8:
            insights.append(PlayerInsight(
                category="value",
                insight=f"{name} is expensive at £{price:.1f}m relative to output ({ppm:.1f} points per million).",
                sentiment="negative"
            ))
        
        return insights

    async def generate_historical_insights(self, player_id: int, opponent_id: int) -> List[PlayerInsight]:
        """Generate insights about historical performance vs opponent."""
        insights = []
        player = await self.get_player_info(player_id)
        
        if not player:
            return insights
        
        name = player['web_name']
        stats = await self.get_vs_opponent_stats(player_id, opponent_id)
        
        if stats['matches'] >= 3:
            if stats['goals'] >= 3:
                insights.append(PlayerInsight(
                    category="historical",
                    insight=f"{name} has scored {stats['goals']} goals in {stats['matches']} matches against this opponent.",
                    sentiment="positive"
                ))
            if stats['avg_points'] >= 6:
                insights.append(PlayerInsight(
                    category="historical",
                    insight=f"{name} averages {stats['avg_points']:.1f} points against this opponent historically.",
                    sentiment="positive"
                ))
        
        return insights

    async def generate_deep_stat_insights(self, player_id: int) -> List[PlayerInsight]:
        """Generate insights about deep stats (Creativity, Threat, Influence)."""
        insights = []
        recent = await self.get_recent_form(player_id, 3) # Last 3 games
        
        if not recent:
            return insights
            
        player = await self.get_player_info(player_id)
        name = player['web_name']
        
        # Calculate averages
        avg_creativity = sum(float(g.get('creativity') or 0) for g in recent) / len(recent)
        avg_threat = sum(float(g.get('threat') or 0) for g in recent) / len(recent)
        
        # Thresholds (somewhat arbitrary but indicative)
        if avg_creativity > 50:
            insights.append(PlayerInsight(
                category="creative",
                insight=f"{name} is creating a lot of chances, averaging {avg_creativity:.1f} Creativity in the last 3 games.",
                sentiment="positive"
            ))
            
        if avg_threat > 50:
            insights.append(PlayerInsight(
                category="threat",
                insight=f"{name} is getting into dangerous positions, averaging {avg_threat:.1f} Threat in the last 3 games.",
                sentiment="positive"
            ))

        return insights

    async def get_all_insights(self, player_id: int, opponent_id: Optional[int] = None) -> List[Dict[str, str]]:
        """
        Get all insights for a player.
        
        Args:
            player_id: The player's ID
            opponent_id: Optional opponent team ID for historical insights
        
        Returns:
            List of insight dictionaries with category, insight, and sentiment.
        """
        all_insights = []
        
        form_insights = await self.generate_form_insights(player_id)
        availability_insights = await self.generate_availability_insights(player_id)
        value_insights = await self.generate_value_insights(player_id)
        deep_insights = await self.generate_deep_stat_insights(player_id)
        
        for insight in form_insights + availability_insights + value_insights + deep_insights:
            all_insights.append({
                "category": insight.category,
                "insight": insight.insight,
                "sentiment": insight.sentiment
            })
        
        # Add historical insights if opponent specified
        if opponent_id:
            historical_insights = await self.generate_historical_insights(player_id, opponent_id)
            for insight in historical_insights:
                all_insights.append({
                    "category": insight.category,
                    "insight": insight.insight,
                    "sentiment": insight.sentiment
                })
        
        return all_insights

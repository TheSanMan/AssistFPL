"""
Feature Engineering for FPL Prediction Model.

This module calculates predictive features from raw player and gameweek data.
Uses a shared DBManager instance for database access.
"""
import structlog
from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from src.database.db_manager import DBManager

logger = structlog.get_logger()


@dataclass
class PlayerFeatures:
    """Represents calculated features for a single player."""
    player_id: int
    web_name: str
    team_id: int
    position: int  # 1=GKP, 2=DEF, 3=MID, 4=FWD
    price: float
    
    # API-Provided Metrics (from bootstrap-static)
    api_form: float           # Official FPL form
    api_ict_index: float      # Official ICT
    api_ep_next: float        # Official expected points
    api_selected_pct: float   # Ownership %
    api_ppg: float            # Points per game
    
    # Calculated Rolling Form (last N games)
    form_points_3: float
    form_points_5: float
    form_xg_3: float
    form_xa_3: float
    form_minutes_3: float
    
    # Deep Stats (Rolling)
    form_creativity_3: float
    form_threat_3: float
    form_influence_3: float
    
    # Context
    next_fixture_difficulty: int
    chance_of_playing: int  # 0-100
    
    # Meta
    total_points_season: int
    games_played: int


class FeatureEngineer:
    """Calculates predictive features from raw FPL data."""
    
    def __init__(self, db: DBManager):
        """
        Initialize with a shared DBManager instance.
        
        Args:
            db: Shared database manager (dependency injection)
        """
        self.db = db

    async def get_player_rolling_form(self, player_id: int, n_games: int = 5) -> Dict[str, float]:
        """
        Calculate rolling averages for a player over their last N games.
        """
        query = """
        SELECT 
            AVG(total_points) as avg_points,
            AVG(expected_goals) as avg_xg,
            AVG(expected_assists) as avg_xa,
            AVG(minutes) as avg_minutes,
            AVG(creativity) as avg_creativity,
            AVG(threat) as avg_threat,
            AVG(influence) as avg_influence,
            COUNT(*) as games_played
        FROM (
            SELECT total_points, expected_goals, expected_assists, minutes, creativity, threat, influence
            FROM gameweek_history
            WHERE player_id = $1 AND minutes > 0
            ORDER BY gameweek DESC
            LIMIT $2
        ) recent_games;
        """
        async with self.db.pool.acquire() as conn:
            row = await conn.fetchrow(query, player_id, n_games)
            if row and row['games_played'] > 0:
                return {
                    "avg_points": float(row['avg_points'] or 0),
                    "avg_xg": float(row['avg_xg'] or 0),
                    "avg_xa": float(row['avg_xa'] or 0),
                    "avg_minutes": float(row['avg_minutes'] or 0),
                    "avg_creativity": float(row['avg_creativity'] or 0),
                    "avg_threat": float(row['avg_threat'] or 0),
                    "avg_influence": float(row['avg_influence'] or 0),
                    "games_played": int(row['games_played'])
                }
            return {
                "avg_points": 0, "avg_xg": 0, "avg_xa": 0, "avg_minutes": 0,
                "avg_creativity": 0, "avg_threat": 0, "avg_influence": 0, "games_played": 0
            }

    async def get_player_season_stats(self, player_id: int) -> Dict[str, Any]:
        """Get aggregated season stats for a player."""
        query = """
        SELECT 
            SUM(total_points) as total_points,
            COUNT(*) as games_played,
            SUM(goals_scored) as goals,
            SUM(assists) as assists
        FROM gameweek_history
        WHERE player_id = $1;
        """
        async with self.db.pool.acquire() as conn:
            row = await conn.fetchrow(query, player_id)
            if row:
                return {
                    "total_points": int(row['total_points'] or 0),
                    "games_played": int(row['games_played'] or 0),
                    "goals": int(row['goals'] or 0),
                    "assists": int(row['assists'] or 0)
                }
            return {"total_points": 0, "games_played": 0, "goals": 0, "assists": 0}

    async def get_next_fixture_difficulty(self, team_id: int) -> int:
        """Get difficulty of the next UPCOMING fixture."""
        fixtures = await self.db.get_upcoming_fixtures(team_id, limit=1)
        if not fixtures:
            return 3 # Default to average difficulty if no game scheduled
        
        f = fixtures[0]
        # if team is home, return difficulty_h, else difficulty_a
        is_home = (f['team_h'] == team_id)
        return int(f['difficulty_h']) if is_home else int(f['difficulty_a'])

    async def build_player_features(self, player_id: int) -> Optional[PlayerFeatures]:
        """
        Build complete feature set for a single player.
        Combines API-provided metrics with calculated rolling form.
        """
        # Get base player info (includes API metrics)
        player = await self.db.get_player(player_id)
        
        if not player:
            return None
        
        # Get rolling form (calculated)
        form_3 = await self.get_player_rolling_form(player_id, 3)
        form_5 = await self.get_player_rolling_form(player_id, 5)
        
        # Get season stats
        season = await self.get_player_season_stats(player_id)
        
        # Get fixture difficulty
        next_diff = await self.get_next_fixture_difficulty(player['team_id'])
        
        return PlayerFeatures(
            player_id=player['id'],
            web_name=player['web_name'],
            team_id=player['team_id'],
            position=player['element_type'],
            price=player['now_cost'] / 10,  # Convert to millions
            # API metrics
            api_form=float(player.get('form') or 0),
            api_ict_index=float(player.get('ict_index') or 0),
            api_ep_next=float(player.get('ep_next') or 0),
            api_selected_pct=float(player.get('selected_by_percent') or 0),
            api_ppg=float(player.get('points_per_game') or 0),
            # Calculated form
            form_points_3=form_3['avg_points'],
            form_points_5=form_5['avg_points'],
            form_xg_3=form_3['avg_xg'],
            form_xa_3=form_3['avg_xa'],
            form_minutes_3=form_3['avg_minutes'],
            # Deep stats
            form_creativity_3=form_3['avg_creativity'],
            form_threat_3=form_3['avg_threat'],
            form_influence_3=form_3['avg_influence'],
            # Context
            next_fixture_difficulty=next_diff,
            chance_of_playing=int(player.get('chance_of_playing_next_round') if player.get('chance_of_playing_next_round') is not None else 100),
            # Season totals
            total_points_season=season['total_points'],
            games_played=season['games_played']
        )

    async def build_all_player_features(self) -> List[PlayerFeatures]:
        """Build features for all players in the database."""
        players = await self.db.get_all_players()
        
        features = []
        for player in players:
            pf = await self.build_player_features(player['id'])
            if pf and pf.games_played > 0:
                features.append(pf)
        
        logger.info("built_all_features", count=len(features))
        return features

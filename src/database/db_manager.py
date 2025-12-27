import asyncpg
import structlog
import os
from typing import List, Dict, Any
from dotenv import load_dotenv

load_dotenv()
logger = structlog.get_logger()

class DBManager:
    """Manages asynchronous database operations for AssistFPL."""
    
    def __init__(self):
        self.pool: asyncpg.Pool = None

    async def connect(self):
        """Establish connection pool."""
        self.pool = await asyncpg.create_pool(
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME"),
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432")
        )
        logger.info("db_connected")

    async def disconnect(self):
        """Close connection pool."""
        if self.pool:
            await self.pool.close()
            logger.info("db_disconnected")

    async def upsert_teams(self, teams: List[Dict[str, Any]]):
        """Upserts team data into the teams table."""
        query = """
        INSERT INTO teams (
            id, name, short_name, strength, strength_overall_home, 
            strength_overall_away, strength_attack_home, strength_attack_away, 
            strength_defence_home, strength_defence_away
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            short_name = EXCLUDED.short_name,
            strength = EXCLUDED.strength,
            updated_at = CURRENT_TIMESTAMP;
        """
        async with self.pool.acquire() as conn:
            batch = [
                (t['id'], t['name'], t['short_name'], t['strength'], 
                 t['strength_overall_home'], t['strength_overall_away'], 
                 t['strength_attack_home'], t['strength_attack_away'], 
                 t['strength_defence_home'], t['strength_defence_away'])
                for t in teams
            ]
            await conn.executemany(query, batch)
        logger.info("upserted_teams", count=len(teams))

    async def upsert_players(self, players: List[Dict[str, Any]]):
        """Upserts player data into the players table."""
        query = """
        INSERT INTO players (
            id, team_id, first_name, second_name, web_name, element_type, 
            now_cost, status, chance_of_playing_next_round, 
            chance_of_playing_this_round, news
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        ON CONFLICT (id) DO UPDATE SET
            team_id = EXCLUDED.team_id,
            now_cost = EXCLUDED.now_cost,
            status = EXCLUDED.status,
            news = EXCLUDED.news,
            updated_at = CURRENT_TIMESTAMP;
        """
        async with self.pool.acquire() as conn:
            batch = [
                (p['id'], p['team'], p['first_name'], p['second_name'], p['web_name'], 
                 p['element_type'], p['now_cost'], p['status'], 
                 p['chance_of_playing_next_round'], p['chance_of_playing_this_round'], p['news'])
                for p in players
            ]
            await conn.executemany(query, batch)
        logger.info("upserted_players", count=len(players))

    async def upsert_gameweek_history(self, history: List[Dict[str, Any]]):
        """Upserts gameweek history data."""
        query = """
        INSERT INTO gameweek_history (
            player_id, gameweek, opponent_team, total_points, was_home, 
            minutes, goals_scored, assists, clean_sheets, goals_conceded, 
            own_goals, penalties_saved, penalties_missed, yellow_cards, 
            red_cards, saves, bonus, bps, influence, creativity, threat, 
            ict_index, value, transfers_balance, selected, transfers_in, 
            transfers_out, expected_goals, expected_assists, 
            expected_goal_involvements, expected_goals_conceded
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31)
        ON CONFLICT (player_id, gameweek) DO NOTHING;
        """
        async with self.pool.acquire() as conn:
            batch = [
                (h['element'], h['round'], h['opponent_team'], h['total_points'], h['was_home'],
                 h['minutes'], h['goals_scored'], h['assists'], h['clean_sheets'], h['goals_conceded'],
                 h['own_goals'], h['penalties_saved'], h['penalties_missed'], h['yellow_cards'],
                 h['red_cards'], h['saves'], h['bonus'], h['bps'], float(h['influence']), float(h['creativity']),
                 float(h['threat']), float(h['ict_index']), h['value'], h['transfers_balance'],
                 h['selected'], h['transfers_in'], h['transfers_out'], float(h['expected_goals']),
                 float(h['expected_assists']), float(h['expected_goal_involvements']), float(h['expected_goals_conceded']))
                for h in history
            ]
            await conn.executemany(query, batch)
        # We don't log every single record for performance

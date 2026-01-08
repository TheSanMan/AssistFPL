import asyncpg
import structlog
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
logger = structlog.get_logger()


class DBManager:
    """Manages asynchronous database operations for AssistFPL."""
    
    _instance: Optional['DBManager'] = None
    
    def __init__(self):
        self.pool: asyncpg.Pool = None

    @classmethod
    async def get_instance(cls) -> 'DBManager':
        """Singleton pattern for shared DB connection."""
        if cls._instance is None:
            cls._instance = DBManager()
            await cls._instance.connect()
        return cls._instance

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
        """Upserts player data with API-provided metrics."""
        query = """
        INSERT INTO players (
            id, team_id, first_name, second_name, web_name, element_type, 
            now_cost, status, chance_of_playing_next_round, 
            chance_of_playing_this_round, news,
            form, points_per_game, selected_by_percent, ict_index,
            influence, creativity, threat, ep_this, ep_next,
            value_form, value_season, total_points
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23)
        ON CONFLICT (id) DO UPDATE SET
            team_id = EXCLUDED.team_id,
            now_cost = EXCLUDED.now_cost,
            status = EXCLUDED.status,
            news = EXCLUDED.news,
            form = EXCLUDED.form,
            points_per_game = EXCLUDED.points_per_game,
            selected_by_percent = EXCLUDED.selected_by_percent,
            ict_index = EXCLUDED.ict_index,
            influence = EXCLUDED.influence,
            creativity = EXCLUDED.creativity,
            threat = EXCLUDED.threat,
            ep_this = EXCLUDED.ep_this,
            ep_next = EXCLUDED.ep_next,
            value_form = EXCLUDED.value_form,
            value_season = EXCLUDED.value_season,
            total_points = EXCLUDED.total_points,
            updated_at = CURRENT_TIMESTAMP;
        """
        async with self.pool.acquire() as conn:
            batch = [
                (p['id'], p['team'], p['first_name'], p['second_name'], p['web_name'], 
                 p['element_type'], p['now_cost'], p['status'], 
                 p.get('chance_of_playing_next_round'), p.get('chance_of_playing_this_round'), p.get('news'),
                 float(p.get('form') or 0), float(p.get('points_per_game') or 0),
                 float(p.get('selected_by_percent') or 0), float(p.get('ict_index') or 0),
                 float(p.get('influence') or 0), float(p.get('creativity') or 0),
                 float(p.get('threat') or 0), float(p.get('ep_this') or 0),
                 float(p.get('ep_next') or 0), float(p.get('value_form') or 0),
                 float(p.get('value_season') or 0), int(p.get('total_points') or 0))
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

    async def upsert_past_season_history(self, player_id: int, history: List[Dict[str, Any]]):
        """Upserts past season history from element-summary endpoint."""
        query = """
        INSERT INTO past_season_history (
            player_id, season_name, start_cost, end_cost, total_points,
            minutes, goals_scored, assists, clean_sheets, goals_conceded,
            own_goals, penalties_saved, penalties_missed, yellow_cards,
            red_cards, saves, bonus, bps, influence, creativity, threat, ict_index
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22)
        ON CONFLICT (player_id, season_name) DO NOTHING;
        """
        async with self.pool.acquire() as conn:
            batch = [
                (player_id, h['season_name'], h.get('start_cost', 0), h.get('end_cost', 0),
                 h['total_points'], h['minutes'], h['goals_scored'], h['assists'],
                 h['clean_sheets'], h['goals_conceded'], h['own_goals'],
                 h['penalties_saved'], h['penalties_missed'], h['yellow_cards'],
                 h['red_cards'], h['saves'], h['bonus'], h['bps'],
                 float(h.get('influence') or 0), float(h.get('creativity') or 0),
                 float(h.get('threat') or 0), float(h.get('ict_index') or 0))
                for h in history
            ]
            await conn.executemany(query, batch)

    async def upsert_fixtures(self, fixtures: List[Dict[str, Any]]):
        """Upserts fixture data."""
        query = """
        INSERT INTO fixtures (
            id, event, team_h, team_a, team_h_score, team_a_score,
            finished, kickoff_time, difficulty_h, difficulty_a
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (id) DO UPDATE SET
            event = EXCLUDED.event,
            team_h_score = EXCLUDED.team_h_score,
            team_a_score = EXCLUDED.team_a_score,
            finished = EXCLUDED.finished,
            kickoff_time = EXCLUDED.kickoff_time,
            difficulty_h = EXCLUDED.difficulty_h,
            difficulty_a = EXCLUDED.difficulty_a,
            updated_at = CURRENT_TIMESTAMP;
        """
        async with self.pool.acquire() as conn:
            batch = []
            for f in fixtures:
                kickoff_time = f.get('kickoff_time')
                if kickoff_time and isinstance(kickoff_time, str):
                    try:
                        # Handle 'Z' suffix manually if fromisoformat doesn't (older python versions)
                        # but newer ones handle it. API returns '2025-08-15T19:00:00Z'
                        kickoff_time = datetime.fromisoformat(kickoff_time.replace('Z', '+00:00'))
                    except ValueError:
                        pass
                
                batch.append((
                    f['id'], f.get('event'), f['team_h'], f['team_a'], 
                    f.get('team_h_score'), f.get('team_a_score'), f['finished'],
                    kickoff_time, f.get('team_h_difficulty'), f.get('team_a_difficulty')
                ))
            
            await conn.executemany(query, batch)
        logger.info("upserted_fixtures", count=len(fixtures))

    # ==================== QUERY METHODS ====================
    
    async def get_player_lookup_table(self) -> Dict[str, int]:
        """
        Returns a mapping of name variations to player_id.
        e.g., {'haaland': 123, 'erling haaland': 123}
        """
        query = "SELECT id, web_name, first_name, second_name FROM players"
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query)
            
        lookup = {}
        for r in rows:
            pid = r['id']
            # Add variations (lowercase)
            lookup[r['web_name'].lower()] = pid
            lookup[f"{r['first_name']} {r['second_name']}".lower()] = pid
            lookup[r['second_name'].lower()] = pid
        return lookup

    async def get_player(self, player_id: int) -> Optional[Dict[str, Any]]:
        """Get a single player by ID."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT p.*, t.name as team_name, t.short_name as team_short
                FROM players p
                JOIN teams t ON p.team_id = t.id
                WHERE p.id = $1
            """, player_id)
            return dict(row) if row else None

    async def get_all_players(self) -> List[Dict[str, Any]]:
        """Get all players."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM players")
            return [dict(r) for r in rows]

    async def get_player_history(self, player_id: int) -> List[Dict[str, Any]]:
        """Get gameweek history for a player."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT h.*, t.short_name as opponent_short 
                   FROM gameweek_history h
                   JOIN teams t ON h.opponent_team = t.id
                   WHERE h.player_id = $1 
                   ORDER BY h.gameweek DESC""",
                player_id
            )
            return [dict(r) for r in rows]

    async def get_player_vs_opponent(self, player_id: int, opponent_id: int) -> List[Dict[str, Any]]:
        """Get historical performance against a specific opponent."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT * FROM gameweek_history 
                   WHERE player_id = $1 AND opponent_team = $2 
                   ORDER BY gameweek DESC""",
                player_id, opponent_id
            )
            return [dict(r) for r in rows]

    async def get_player_past_seasons(self, player_id: int) -> List[Dict[str, Any]]:
        """Get past season summaries for a player."""
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM past_season_history WHERE player_id = $1 ORDER BY season_name DESC",
                player_id
            )
            return [dict(r) for r in rows]

    async def get_upcoming_fixtures(self, team_id: int, limit: int = 5) -> List[Dict[str, Any]]:
        """Get upcoming fixtures for a team."""
        async with self.pool.acquire() as conn:
            query = """
                SELECT f.*, 
                       h.name as team_h_name, h.short_name as team_h_short,
                       a.name as team_a_name, a.short_name as team_a_short
                FROM fixtures f
                JOIN teams h ON f.team_h = h.id
                JOIN teams a ON f.team_a = a.id
                WHERE (f.team_h = $1 OR f.team_a = $1)
                  AND f.finished = FALSE
                ORDER BY f.kickoff_time ASC
                LIMIT $2
            """
            rows = await conn.fetch(query, team_id, limit)
            return [dict(r) for r in rows]

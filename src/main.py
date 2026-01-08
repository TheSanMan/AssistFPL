import asyncio
import aiohttp
import structlog
from src.ingestion.scraper import FPLClient
from src.database.db_manager import DBManager

# Configure structlog for better visibility
structlog.configure(
    processors=[
        structlog.processors.JSONRenderer()
    ]
)
logger = structlog.get_logger()

async def run_etl():
    """Main ETL orchestration logic."""
    db = DBManager()
    await db.connect()
    
    async with aiohttp.ClientSession() as session:
        client = FPLClient(session)
        
        # 1. Fetch core data
        bootstrap_data = await client.fetch_bootstrap_data()
        if not bootstrap_data:
            logger.error("etl_failed", reason="could_not_fetch_bootstrap_data")
            return

        # 2. Upsert Teams
        await db.upsert_teams(bootstrap_data['teams'])
        
        # 3. Upsert Players (now includes API metrics: form, ict_index, ep_next)
        await db.upsert_players(bootstrap_data['elements'])
        
        # 4. Fetch and Upsert Gameweek History + Past Seasons for all players
        player_ids = [p['id'] for p in bootstrap_data['elements']]
        logger.info("fetching_history_for_players", count=len(player_ids))
        
        # Process in chunks to avoid overwhelming the DB or API
        chunk_size = 50
        for i in range(0, len(player_ids), chunk_size):
            chunk = player_ids[i:i + chunk_size]
            histories = await client.fetch_all_players_history(chunk)
            
            all_gw_records = []
            for idx, h in enumerate(histories):
                player_id = chunk[idx]
                if h:
                    # Current season gameweek history
                    if 'history' in h:
                        all_gw_records.extend(h['history'])
                    
                    # Past seasons summary (for historical insights)
                    if 'history_past' in h and h['history_past']:
                        await db.upsert_past_season_history(player_id, h['history_past'])
            
            if all_gw_records:
                await db.upsert_gameweek_history(all_gw_records)
            
            if all_gw_records:
                await db.upsert_gameweek_history(all_gw_records)
            
            logger.info("processed_chunk", start=i, end=min(i + chunk_size, len(player_ids)))

        # 5. Fetch and Upsert Fixtures
        fixtures = await client.fetch_fixtures()
        if fixtures:
            await db.upsert_fixtures(fixtures)

    await db.disconnect()
    logger.info("etl_complete")

if __name__ == "__main__":
    asyncio.run(run_etl())

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
        
        # 3. Upsert Players
        await db.upsert_players(bootstrap_data['elements'])
        
        # 4. Fetch and Upsert Gameweek History for all players
        player_ids = [p['id'] for p in bootstrap_data['elements']]
        logger.info("fetching_history_for_players", count=len(player_ids))
        
        # Process in chunks to avoid overwhelming the DB or API even with the semaphore
        chunk_size = 50
        for i in range(0, len(player_ids), chunk_size):
            chunk = player_ids[i:i + chunk_size]
            histories = await client.fetch_all_players_history(chunk)
            
            all_gw_records = []
            for h in histories:
                if h and 'history' in h:
                    all_gw_records.extend(h['history'])
            
            if all_gw_records:
                await db.upsert_gameweek_history(all_gw_records)
                logger.info("processed_chunk", start=i, end=min(i + chunk_size, len(player_ids)))

    await db.disconnect()
    logger.info("etl_complete")

if __name__ == "__main__":
    asyncio.run(run_etl())

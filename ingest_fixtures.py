import asyncio
import aiohttp
import structlog
from src.ingestion.scraper import FPLClient
from src.database.db_manager import DBManager

structlog.configure(
    processors=[structlog.processors.JSONRenderer()]
)
logger = structlog.get_logger()

async def ingest_fixtures_only():
    db = DBManager()
    await db.connect()
    
    async with aiohttp.ClientSession() as session:
        client = FPLClient(session)
        
        logger.info("fetching_fixtures")
        fixtures = await client.fetch_fixtures()
        
        if fixtures:
            logger.info("upserting_fixtures", count=len(fixtures))
            await db.upsert_fixtures(fixtures)
        else:
            logger.error("no_fixtures_found")

    await db.disconnect()

if __name__ == "__main__":
    asyncio.run(ingest_fixtures_only())

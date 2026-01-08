import asyncio
import structlog
from src.database.db_manager import DBManager

# Configure logging
structlog.configure(
    processors=[structlog.processors.JSONRenderer()]
)

async def test_db():
    print("Connecting to DB...")
    db = DBManager()
    await db.connect()
    
    try:
        # 1. Test get_player
        print("\nTesting get_player(1)...")
        player = await db.get_player(1)
        if player:
            print(f"✅ Found player: {player.get('web_name')} (ID: {player.get('id')})")
            print(f"   Team: {player.get('team_name')}")
        else:
            print("❌ Player 1 not found!")
            
        # 2. Test get_player_fixtures
        if player:
            print(f"\nTesting get_upcoming_fixtures({player['team_id']})...")
            fixtures = await db.get_upcoming_fixtures(player['team_id'])
            print(f"✅ Found {len(fixtures)} upcoming fixtures")
            for f in fixtures:
                print(f"   - vs {f['team_a_short'] if f['team_h'] == player['team_id'] else f['team_h_short']}")
        
        # 3. Test fixtures table count
        print("\nChecking total fixtures count...")
        async with db.pool.acquire() as conn:
             count = await conn.fetchval("SELECT count(*) FROM fixtures")
             print(f"Total fixtures in DB: {count}")
             
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        await db.disconnect()

if __name__ == "__main__":
    asyncio.run(test_db())

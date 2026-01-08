import asyncio
import os
import asyncpg
from dotenv import load_dotenv

load_dotenv()

async def apply_schema():
    print("Connecting to DB...")
    conn = await asyncpg.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        database=os.getenv("DB_NAME"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432")
    )
    
    with open('database/schema.sql', 'r') as f:
        schema_sql = f.read()
    
    print("Applying schema...")
    await conn.execute(schema_sql)
    print("Schema applied successfully.")
    await conn.close()

if __name__ == "__main__":
    asyncio.run(apply_schema())

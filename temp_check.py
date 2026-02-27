import asyncio
import asyncpg

async def check():
    conn = await asyncpg.connect(
        user='postgres',
        password='postgres',
        database='video_stats',
        host='db'
    )
    try:
        result = await conn.fetchval("""
            SELECT COUNT(DISTINCT video_id) 
            FROM video_snapshots 
            WHERE delta_views_count > 0 
            AND created_at::date = '2025-11-27'
        """)
        print(f"Результат: {result}")
    finally:
        await conn.close()

asyncio.run(check())

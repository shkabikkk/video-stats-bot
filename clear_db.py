import asyncio
import asyncpg
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def clear_database():
    """Полностью очищает базу данных"""
    conn = await asyncpg.connect(
        user='postgres',
        password='postgres',
        database='video_stats',
        host='db'
    )
    
    try:
        await conn.execute("TRUNCATE video_snapshots CASCADE")
        await conn.execute("TRUNCATE videos CASCADE")
        
        await conn.execute("ALTER SEQUENCE IF EXISTS videos_id_seq RESTART WITH 1")
        await conn.execute("ALTER SEQUENCE IF EXISTS video_snapshots_id_seq RESTART WITH 1")
        
        logger.info("✅ База данных полностью очищена")
        
        videos_count = await conn.fetchval("SELECT COUNT(*) FROM videos")
        snapshots_count = await conn.fetchval("SELECT COUNT(*) FROM video_snapshots")
        logger.info(f"Текущее состояние: videos={videos_count}, snapshots={snapshots_count}")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при очистке: {e}")
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(clear_database()) 
import json
import asyncio
import asyncpg
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_date(date_str):
    """Преобразует ISO строку в datetime без часового пояса"""
    # Убираем Z и обрезаем микросекунды
    date_str = date_str.replace('Z', '+00:00')
    dt = datetime.fromisoformat(date_str)
    # Убираем информацию о часовом поясе
    return dt.replace(tzinfo=None)

async def load_data():
    conn = await asyncpg.connect(
        user='postgres',
        password='postgres',
        database='video_stats',
        host='db'
    )
    
    try:
        with open('data/videos.json', 'r', encoding='utf-8') as f:
            data = json.load(f)
            videos = data['videos']
        
        logger.info(f"Loaded {len(videos)} videos from JSON")
        
        await conn.execute("TRUNCATE video_snapshots CASCADE")
        await conn.execute("TRUNCATE videos CASCADE")
        logger.info("Tables truncated")
        
        for video in videos:
            await conn.execute("""
                INSERT INTO videos 
                (id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
                video['id'],
                video['creator_id'],
                parse_date(video['video_created_at']),
                video['views_count'],
                video['likes_count'],
                video['comments_count'],
                video['reports_count'],
                parse_date(video['created_at']),
                parse_date(video['updated_at'])
            )
            
            snapshots_count = 0
            for snapshot in video.get('snapshots', []):
                await conn.execute("""
                    INSERT INTO video_snapshots
                    (id, video_id, views_count, likes_count, comments_count, reports_count,
                     delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count, 
                     created_at, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                """,
                    snapshot['id'],
                    video['id'],
                    snapshot['views_count'],
                    snapshot['likes_count'],
                    snapshot['comments_count'],
                    snapshot['reports_count'],
                    snapshot['delta_views_count'],
                    snapshot['delta_likes_count'],
                    snapshot['delta_comments_count'],
                    snapshot['delta_reports_count'],
                    parse_date(snapshot['created_at']),
                    parse_date(snapshot['updated_at'])
                )
                snapshots_count += 1
            
            logger.info(f"Inserted video {video['id']} with {snapshots_count} snapshots")
        
        videos_count = await conn.fetchval("SELECT COUNT(*) FROM videos")
        snapshots_count = await conn.fetchval("SELECT COUNT(*) FROM video_snapshots")
        logger.info(f"Total: {videos_count} videos, {snapshots_count} snapshots")
        
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(load_data())
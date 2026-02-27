import asyncpg
from src.config import config
import logging

logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.pool = None

    async def connect(self):
        """Создаем пул соединений с БД"""
        try:
            self.pool = await asyncpg.create_pool(
                host=config.DB_HOST,
                port=config.DB_PORT,
                user=config.DB_USER,
                password=config.DB_PASSWORD,
                database=config.DB_NAME,
                min_size=5,
                max_size=20
            )
            logger.info("Подключились к базе")
        except Exception as e:
            logger.error(f"Не смогли подключиться к базе: {e}")
            raise

    async def disconnect(self):
        """Закрываем соединения"""
        if self.pool:
            await self.pool.close()
            logger.info("Отключились от базы")

    async def execute(self, query, *args):
        """Просто выполняем запрос без возврата данных"""
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args)

    async def fetch(self, query, *args):
        """Забираем несколько строк"""
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query, *args):
        """Забираем одну строку"""
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def fetchval(self, query, *args):
        """Забираем одно значение"""
        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, *args)

# Создаем один экземпляр на всё приложение
db = Database()
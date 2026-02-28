import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from aiogram import Bot, Dispatcher
from aiogram.types import Message
from aiogram.filters import Command

from src.config import config
from src.db.database import db
from src.handlers.message_handler import router

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

bot = Bot(token=config.BOT_TOKEN)
dp = Dispatcher()
dp.include_router(router)

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await message.answer(
        "Привет! Я бот для статистики видео.\n\n"
        "Можешь спросить меня например:\n"
        "• Сколько всего видео?\n"
        "• Сколько видео у креатора 123?\n"
        "• Сколько видео набрало больше 100000 просмотров?\n"
        "• На сколько просмотров выросли все видео 28 ноября 2025?"
    )

@dp.message(Command("help"))
async def cmd_help(message: Message):
    await message.answer(
        "Я отвечаю на вопросы о статистике видео.\n\n"
        "Примеры:\n"
        "• Сколько всего видео?\n"
        "• Сколько видео у креатора 42?\n"
        "• Сколько видео с более чем 50000 просмотров?\n"
        "• Сколько просмотров добавили все видео 27 ноября?\n"
        "• Сколько разных видео обновлялись 27 ноября 2025?\n\n"
        "Я всегда отвечаю одним числом."
    )

async def main():
    logger.info("Запускаем бота...")
    
    await db.connect()
    logger.info("База данных готова")
    
    try:
        await dp.start_polling(bot)
    finally:
        await db.disconnect()
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
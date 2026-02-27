from aiogram import Router
from aiogram.types import Message
import logging

from src.services.llm_service import get_sql_from_text, get_answer_from_text_and_result
from src.services.sql_executor import execute_sql_and_get_result

router = Router()
logger = logging.getLogger(__name__)

@router.message()
async def handle_message(message: Message):
    """Обрабатываем любое текстовое сообщение"""
    user_text = message.text
    user_id = message.from_user.id
    
    logger.info(f"Сообщение от {user_id}: {user_text}")
    
    try:
        # Отправляем печатать...
        await message.bot.send_chat_action(message.chat.id, "typing")
        
        # 1. Получаем SQL из GigaChat
        sql_query = await get_sql_from_text(user_text)
        
        # 2. Выполняем SQL, получаем результат
        result = await execute_sql_and_get_result(sql_query)
        
        # 3. Генерируем текстовый ответ через GigaChat
        answer = await get_answer_from_text_and_result(user_text, result)
        
        # 4. Отправляем ответ
        await message.answer(answer)
        
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await message.answer("Что-то пошло не так, попробуй позже")
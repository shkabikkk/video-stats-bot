from aiogram import Router
from aiogram.types import Message
import logging

from src.services.llm_service import get_sql_from_text
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
        # 1. Получаем SQL из GigaChat
        sql_query = await get_sql_from_text(user_text)
        
        # 2. Выполняем SQL, получаем результат
        result = await execute_sql_and_get_result(sql_query)
        
        # 3. Если результат - список, преобразуем в число (количество)
        if isinstance(result, list):
            result = len(result)
        
        # 4. Отправляем только число
        await message.answer(str(result))
        
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        await message.answer("0")
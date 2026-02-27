import logging
from src.db.database import db

logger = logging.getLogger(__name__)

async def execute_sql_and_get_result(sql_query: str):
    """
    Выполняет SQL запрос и возвращает результат (число или список)
    """
    try:
        # Убираем возможные markdown и лишние пробелы
        sql_query = sql_query.strip().strip('`').strip()
        if sql_query.startswith('sql'):
            sql_query = sql_query[3:].strip()
        
        logger.info(f"Выполняем SQL: {sql_query}")
        
        # Определяем тип запроса
        sql_upper = sql_query.upper().strip()
        
        # Если запрос на получение списка (DISTINCT или просто SELECT без агрегации)
        if 'SELECT DISTINCT' in sql_upper or (
            sql_upper.startswith('SELECT') and 
            'COUNT(' not in sql_upper and 
            'SUM(' not in sql_upper and
            'AVG(' not in sql_upper
        ):
            # Выполняем запрос и получаем все строки
            rows = await db.fetch(sql_query)
            # Преобразуем в список значений (берем первый столбец)
            if rows:
                result = [list(row.values())[0] for row in rows]
                logger.info(f"Получили список из {len(result)} элементов")
                return result
            return []
        else:
            # Для агрегатных функций - одно число
            result = await db.fetchval(sql_query)
            logger.info(f"Получили число: {result}")
            return result if result is not None else 0
        
    except Exception as e:
        logger.error(f"Ошибка при выполнении SQL: {e}")
        logger.error(f"Запрос: {sql_query}")
        return 0

# Для обратной совместимости (если где-то используется)
async def execute_sql_and_get_number(sql_query: str) -> int:
    result = await execute_sql_and_get_result(sql_query)
    if isinstance(result, list):
        return len(result)
    return int(result) if result is not None else 0
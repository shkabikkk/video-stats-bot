import logging
from src.db.database import db

logger = logging.getLogger(__name__)

async def execute_sql_and_get_result(sql_query: str):
    """
    Выполняет SQL запрос и возвращает результат (число или список)
    """
    try:
        sql_query = sql_query.strip().strip('`').strip()
        if sql_query.startswith('sql'):
            sql_query = sql_query[3:].strip()
        
        logger.info(f"Выполняем SQL: {sql_query}")
        
        sql_upper = sql_query.upper().strip()
        
        if 'SELECT DISTINCT' in sql_upper or (
            sql_upper.startswith('SELECT') and 
            'COUNT(' not in sql_upper and 
            'SUM(' not in sql_upper and
            'AVG(' not in sql_upper
        ):
            rows = await db.fetch(sql_query)
            if rows:
                result = [list(row.values())[0] for row in rows]
                logger.info(f"Получили список из {len(result)} элементов")
                return result
            return []
        else:
            result = await db.fetchval(sql_query)
            logger.info(f"Получили число: {result}")
            return result if result is not None else 0
        
    except Exception as e:
        logger.error(f"Ошибка при выполнении SQL: {e}")
        logger.error(f"Запрос: {sql_query}")
        return 0
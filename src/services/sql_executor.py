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
        
        sql_query = sql_query.rstrip(';')
        
        sql_upper = sql_query.upper().strip()
        
        if sql_upper.startswith('SELECT') and not any(word in sql_upper for word in ['FROM', 'COUNT(', 'SUM(', 'AVG(']):
            # Простой SELECT с константой
            rows = await db.fetch(sql_query)
            if rows and len(rows) > 0 and len(rows[0]) > 0:
                logger.info(f"Получили значение: {result}")
                return result
            return 0
        
        # Для запросов с DISTINCT или без агрегатных функций
        if 'SELECT DISTINCT' in sql_upper or (
            sql_upper.startswith('SELECT') and 
            'COUNT(' not in sql_upper and 
            'SUM(' not in sql_upper and
            'AVG(' not in sql_upper
        ):
            rows = await db.fetch(sql_query)
            if rows:
                # Для SELECT 17668 это условие не сработает, т.к. нет FROM
                result = [list(row.values())[0] for row in rows]
                logger.info(f"Получили список из {len(result)} элементов")
                return result
            return []
        else:
            # Для агрегатных функций
            result = await db.fetchval(sql_query)
            logger.info(f"Получили число: {result}")
            return result if result is not None else 0
        
    except Exception as e:
        logger.error(f"Ошибка при выполнении SQL: {e}")
        logger.error(f"Запрос: {sql_query}")
        return 0
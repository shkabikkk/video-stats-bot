import logging
from gigachat import GigaChat
from gigachat.models import Chat, Messages, MessagesRole
from src.config import config

logger = logging.getLogger(__name__)

# Промпт для генерации SQL
SQL_PROMPT_TEMPLATE = """
Ты - SQL-эксперт. Твоя задача - преобразовывать вопросы пользователей о статистике видео в SQL-запросы для PostgreSQL.

Таблицы в базе данных:

1. videos - информация о видео
   - id (VARCHAR) - уникальный идентификатор видео
   - creator_id (VARCHAR) - идентификатор автора (креатора)
   - video_created_at (TIMESTAMP) - дата публикации видео
   - views_count (INTEGER) - итоговое количество просмотров
   - likes_count (INTEGER) - итоговое количество лайков
   - comments_count (INTEGER) - итоговое количество комментариев
   - reports_count (INTEGER) - итоговое количество жалоб

2. video_snapshots - почасовые срезы статистики
   - id (VARCHAR) - идентификатор среза
   - video_id (VARCHAR) - ссылка на видео
   - views_count (INTEGER) - просмотры на момент среза
   - delta_views_count (INTEGER) - изменение просмотров за час
   - created_at (TIMESTAMP) - время среза (каждый час)

КРИТИЧЕСКИ ВАЖНО:
- Креаторы (авторы) хранятся в поле creator_id таблицы videos
- Видео хранятся в поле id таблицы videos
- НЕ ПУТАЙ ИХ! Когда просят креаторов - используй creator_id
- Когда просят видео - используй id

Для получения списка ВСЕХ креаторов:
SQL: SELECT DISTINCT creator_id FROM videos;

Для получения списка ВСЕХ видео:
SQL: SELECT id FROM videos;

Для количества:
- Креаторов: SELECT COUNT(DISTINCT creator_id) FROM videos
- Видео: SELECT COUNT(*) FROM videos

Правила:
- Возвращай ТОЛЬКО SQL-код, без пояснений
- Не используй markdown, только чистый SQL
- creator_id всегда строка, используй кавычки: creator_id = '123'

Примеры:
Вопрос: "покажи всех креаторов списком"
SQL: SELECT DISTINCT creator_id FROM videos;

Вопрос: "сколько всего креаторов"
SQL: SELECT COUNT(DISTINCT creator_id) FROM videos;

Вопрос: "все видео"
SQL: SELECT id FROM videos;

Вопрос: "сколько видео"
SQL: SELECT COUNT(*) FROM videos;

Вопрос: "Сколько видео у креатора 123?"
SQL: SELECT COUNT(*) FROM videos WHERE creator_id = '123';

Вопрос: "На сколько просмотров выросли все видео 28 ноября 2025?"
SQL: SELECT SUM(delta_views_count) FROM video_snapshots WHERE created_at::date = '2025-11-28';

Теперь преобразуй следующий вопрос в SQL:
{user_question}
SQL:
"""

# Промпт для генерации ответа
ANSWER_PROMPT_TEMPLATE = """
Ты - дружелюбный помощник, который отвечает на вопросы о статистике видео.
Пользователь спросил: "{user_question}"
Результат запроса к базе данных: {query_result}

Сформулируй понятный и вежливый ответ на русском языке.

ВАЖНО:
- Если пользователь просит список (например, "отправь всех креаторов", "покажи всех", "списком"), ты ДОЛЖЕН перечислить ВСЕ элементы
- НЕ сокращай список, если пользователь явно просит "всех" или "списком"
- Перечисляй все id через запятую
- Если результат - число, просто скажи сколько

Примеры:
- Вопрос: "отправь мне списком всех креаторов", результат: список из 19 id
  Ответ: Найдено 19 креаторов: fa1846d2, 12bc6760, cd87be38, d673ac82, 4b3da270, 38a51cc7, df5973c0, aca1061a, 00c77978, d93655e9, f2d46be3, 0d775b4e, 2dade264, 706646b9, 4b3da270, cd87be38, fa1846d2, 12bc6760, 38a51cc7

- Вопрос: "сколько креаторов", результат: 19
  Ответ: Всего в системе 19 уникальных креаторов

- Вопрос: "все видео", результат: список из 358 id
  Ответ: Найдено 358 видео: ecd8a4e4, 3fcc673d, acdcda30, 8ac3b6c8, 421d2e61, 4b0ed73e, 2f03fff5, e88f898d, b4e079b2, fe2618d6, 2ed616d0, 1ffb27db, b3227185, d09ecd17, 19e59eef, 953ffd52, a2b3a255, a90540ac, ce86a94f, 02292b04, 2a2c2e05, 970aae93 (и так далее - все 358)

- Вопрос: "сколько видео", результат: 358
  Ответ: Всего в системе 358 видео

- Вопрос: "сколько видео у креатора 123", результат: 5
  Ответ: У креатора 123 найдено 5 видео

- Вопрос: "на сколько просмотров выросли все видео 28 ноября 2025", результат: 1500
  Ответ: 28 ноября 2025 года все видео вместе набрали 1500 новых просмотров

Твой ответ (только текст, без пояснений и markdown):
"""

async def get_sql_from_text(text: str) -> str:
    """
    Отправляет текст в GigaChat и получает SQL
    """
    logger.info(f"Генерируем SQL для: {text}")
    
    try:
        # Создаем сообщение
        message = Messages(
            role=MessagesRole.USER,
            content=SQL_PROMPT_TEMPLATE.format(user_question=text)
        )
        
        # Создаем чат с сообщениями
        chat = Chat(messages=[message])
        
        # Отправляем запрос
        async with GigaChat(
            credentials=config.GIGACHAT_CREDENTIALS,
            verify_ssl_certs=False
        ) as giga:
            response = await giga.achat(chat)
            
            sql_query = response.choices[0].message.content.strip()
            # Очищаем от возможных markdown
            sql_query = sql_query.replace('```sql', '').replace('```', '').strip()
            
            logger.info(f"GigaChat вернул SQL: {sql_query}")
            return sql_query
            
    except Exception as e:
        logger.error(f"Ошибка при обращении к GigaChat: {e}")
        return "SELECT COUNT(*) FROM videos;"

async def get_answer_from_text_and_result(text: str, result) -> str:
    """
    Отправляет вопрос и результат в GigaChat, получает текстовый ответ
    """
    logger.info(f"Генерируем ответ для: {text} с результатом {result}")
    
    try:
        # Форматируем результат для промпта
        if isinstance(result, list):
            if len(result) > 10:
                result_str = f"список из {len(result)} элементов: {', '.join([str(x) for x in result[:5]])} и еще {len(result)-5}"
            else:
                result_str = f"список из {len(result)} элементов: {', '.join([str(x) for x in result])}"
        else:
            result_str = str(result)
        
        # Создаем сообщение
        message = Messages(
            role=MessagesRole.USER,
            content=ANSWER_PROMPT_TEMPLATE.format(
                user_question=text,
                query_result=result_str
            )
        )
        
        # Создаем чат с сообщениями
        chat = Chat(messages=[message])
        
        # Отправляем запрос
        async with GigaChat(
            credentials=config.GIGACHAT_CREDENTIALS,
            verify_ssl_certs=False
        ) as giga:
            response = await giga.achat(chat)
            
            answer = response.choices[0].message.content.strip()
            logger.info(f"GigaChat вернул ответ: {answer}")
            return answer
            
    except Exception as e:
        logger.error(f"Ошибка при генерации ответа: {e}")
        # Fallback на случай ошибки
        if isinstance(result, list):
            if not result:
                return "Ничего не найдено"
            # Определяем по вопросу что это
            if "креатор" in text.lower() or "автор" in text.lower():
                item_name = "креаторов"
            else:
                item_name = "видео"
            
            # Формируем полный список без ограничений
            items_list = "\n".join([f"• {r}" for r in result])
            return f"Найдено {len(result)} {item_name}:\n\n{items_list}"
        return f"Результат: {result}"
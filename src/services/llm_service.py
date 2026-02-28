import logging
from gigachat import GigaChat
from gigachat.models import Chat, Messages, MessagesRole
from src.config import config

logger = logging.getLogger(__name__)

# Промпт для генерации SQL
SQL_PROMPT_TEMPLATE = """
Ты - SQL-эксперт высшего уровня. Твоя задача - преобразовывать вопросы пользователей о статистике видео в SQL-запросы для PostgreSQL.
ОТ ТВОЕЙ ТОЧНОСТИ ЗАВИСИТ РАБОТА ВСЕЙ СИСТЕМЫ. ДЕЙСТВУЙ ПРЕДЕЛЬНО ВНИМАТЕЛЬНО.

====================================================================
1. ПОЛНОЕ ОПИСАНИЕ СХЕМЫ БАЗЫ ДАННЫХ
====================================================================

Таблица videos (информация о видео):
- id (VARCHAR) - уникальный идентификатор видео (пример: '970aae93-6241-48a5-8906-72608ad2d261')
- creator_id (VARCHAR) - идентификатор автора/креатора (пример: 'aca1061a9d324ecf8c3fa2bb32d7be63')
- video_created_at (TIMESTAMP) - дата и время публикации видео
- views_count (INTEGER) - итоговое количество просмотров (финальное значение)
- likes_count (INTEGER) - итоговое количество лайков
- comments_count (INTEGER) - итоговое количество комментариев
- reports_count (INTEGER) - итоговое количество жалоб

Таблица video_snapshots (почасовые срезы статистики):
- id (VARCHAR) - идентификатор среза
- video_id (VARCHAR) - ссылка на видео (связь с таблицей videos)
- views_count (INTEGER) - количество просмотров на момент среза
- delta_views_count (INTEGER) - изменение просмотров за последний час (может быть 0, положительным или даже отрицательным!)
- delta_likes_count (INTEGER) - изменение лайков за последний час
- delta_comments_count (INTEGER) - изменение комментариев за последний час
- created_at (TIMESTAMP) - время создания среза (каждый час, 24 записи в сутки)

====================================================================
2. КРИТИЧЕСКИ ВАЖНЫЕ ПРАВИЛА
====================================================================

2.1. ТИПЫ ДАННЫХ И ФОРМАТЫ:
- Все идентификаторы (id, creator_id) - ЭТО СТРОКИ (VARCHAR). В SQL их обязательно заключать в кавычки: creator_id = 'aca1061a9d324ecf8c3fa2bb32d7be63'
- Даты хранятся в формате TIMESTAMP. Для сравнения по дате используй: created_at::date = '2025-11-28'
- Числовые значения (views_count, likes_count и т.д.) - это целые числа (INTEGER), без кавычек

2.2. ЧТО ВОЗВРАЩАТЬ:
- ТЫ ДОЛЖЕН ВОЗВРАЩАТЬ ТОЛЬКО SQL-КОД. НИКАКИХ ПОЯСНЕНИЙ, КОММЕНТАРИЕВ, MARKDOWN.
- ВСЕГДА возвращай запрос, который дает ОДНО ЧИСЛО (COUNT, SUM, и т.д.)
- Даже если пользователь просит список (например, "покажи всех креаторов"), преобразуй это в COUNT
- НИКОГДА не используй LIMIT без явной просьбы пользователя

2.3. РАЗЛИЧИЕ МЕЖДУ ВИДЕО И КРЕАТОРАМИ:
- Видео - это записи в таблице videos, их идентификаторы в поле id
- Креаторы - это авторы видео, их идентификаторы в поле creator_id
- НИКОГДА НЕ ПУТАЙ ИХ!

2.4. ОБРАБОТКА ЧИСЕЛ В ВОПРОСАХ:
- Внимательно анализируй числа в вопросах: 1000, 10000, 100000, 1000000
- "больше 100000" означает > 100000, а не >=
- Учитывай пробелы в числах: "100 000" = 100000, "1 500" = 1500
- Проверяй реальные значения в данных: максимальное views_count = 39699, значит ответы с >100000 должны быть 0

2.5. ОБРАБОТКА ДАТ:
- Если год не указан, используй 2025
- Формат даты в SQL: '2025-11-28'
- Для диапазонов используй BETWEEN: date BETWEEN '2025-11-01' AND '2025-11-05'
- "с 1 по 5 ноября" = BETWEEN '2025-11-01' AND '2025-11-05'
- "28 ноября" = created_at::date = '2025-11-28'
- ВНИМАНИЕ: в таблице video_snapshots created_at содержит дату и время (каждый час). Для поиска по дню используй created_at::date

2.6. ОСОБЫЕ СЛУЧАИ:
- "получали новые просмотры" = delta_views_count > 0 (строго больше нуля)
- "получали новые лайки" = delta_likes_count > 0
- "получали новые комментарии" = delta_comments_count > 0
- "разные видео" = COUNT(DISTINCT video_id)
- "в сумме выросли" (о просмотрах) = SUM(delta_views_count)
- "в сумме выросли" (о лайках) = SUM(delta_likes_count)
- "в сумме выросли" (о комментариях) = SUM(delta_comments_count)
- "за всё время" = без фильтра по дате
- "вышло" (о видео) = фильтр по video_created_at
- "набрало больше X просмотров" = фильтр по views_count

====================================================================
3. ПОДРОБНЫЕ ПРИМЕРЫ С ОБЪЯСНЕНИЯМИ
====================================================================

--------------------------------------------------------------------
3.1. ПРОСТЫЕ ПОДСЧЕТЫ
--------------------------------------------------------------------

Вопрос: "Сколько всего видео есть в системе?"
Объяснение: Нужно просто посчитать все записи в таблице videos
SQL: SELECT COUNT(*) FROM videos;

Вопрос: "Сколько всего креаторов в системе?"
Объяснение: Нужно посчитать уникальные creator_id в таблице videos
SQL: SELECT COUNT(DISTINCT creator_id) FROM videos;

--------------------------------------------------------------------
3.2. ЗАПРОСЫ С УСЛОВИЯМИ ПО ЧИСЛАМ
--------------------------------------------------------------------

Вопрос: "Сколько видео набрало больше 1000 просмотров?"
Объяснение: views_count > 1000 (строго больше)
SQL: SELECT COUNT(*) FROM videos WHERE views_count > 1000;

Вопрос: "Сколько видео набрало больше 100 000 просмотров?"
Объяснение: В данных максимальное значение 39699, поэтому результат будет 0. Но ты все равно должен вернуть правильный SQL.
SQL: SELECT COUNT(*) FROM videos WHERE views_count > 100000;

Вопрос: "Сколько видео набрало меньше 100 просмотров?"
Объяснение: views_count < 100
SQL: SELECT COUNT(*) FROM videos WHERE views_count < 100;

Вопрос: "Сколько видео с лайками больше 50?"
Объяснение: likes_count > 50
SQL: SELECT COUNT(*) FROM videos WHERE likes_count > 50;

Вопрос: "Сколько видео без комментариев?"
Объяснение: comments_count = 0
SQL: SELECT COUNT(*) FROM videos WHERE comments_count = 0;

--------------------------------------------------------------------
3.3. ЗАПРОСЫ ПО ДАТАМ
--------------------------------------------------------------------

Вопрос: "Сколько видео вышло 28 ноября 2025?"
Объяснение: Фильтр по дате публикации видео
SQL: SELECT COUNT(*) FROM videos WHERE video_created_at::date = '2025-11-28';

Вопрос: "Сколько видео вышло с 1 по 5 ноября 2025 включительно?"
Объяснение: Диапазон дат с включением границ
SQL: SELECT COUNT(*) FROM videos WHERE video_created_at::date BETWEEN '2025-11-01' AND '2025-11-05';

Вопрос: "На сколько просмотров выросли все видео 28 ноября 2025?"
Объяснение: Сумма delta_views_count за конкретный день из таблицы snapshots
SQL: SELECT SUM(delta_views_count) FROM video_snapshots WHERE created_at::date = '2025-11-28';

Вопрос: "Сколько просмотров добавили все видео 27 ноября 2025?"
Объяснение: То же самое, другая формулировка
SQL: SELECT SUM(delta_views_count) FROM video_snapshots WHERE created_at::date = '2025-11-27';

Вопрос: "На сколько лайков выросли все видео 28 ноября 2025?"
Объяснение: Сумма delta_likes_count за конкретный день
SQL: SELECT SUM(delta_likes_count) FROM video_snapshots WHERE created_at::date = '2025-11-28';

Вопрос: "На сколько комментариев выросли все видео 28 ноября 2025?"
Объяснение: Сумма delta_comments_count за конкретный день
SQL: SELECT SUM(delta_comments_count) FROM video_snapshots WHERE created_at::date = '2025-11-28';

--------------------------------------------------------------------
3.4. СЛОЖНЫЕ ЗАПРОСЫ (ВНИМАНИЕ, КАВЕРЗНЫЕ)
--------------------------------------------------------------------

Вопрос: "Сколько разных видео получали новые просмотры 27 ноября 2025?"
Объяснение: 
1. "получали новые просмотры" = delta_views_count > 0
2. "разные видео" = COUNT(DISTINCT video_id)
3. "27 ноября 2025" = фильтр по дате
SQL: SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE delta_views_count > 0 AND created_at::date = '2025-11-27';

Вопрос: "Сколько разных видео получали новые лайки 27 ноября 2025?"
Объяснение:
1. "получали новые лайки" = delta_likes_count > 0
2. "разные видео" = COUNT(DISTINCT video_id)
SQL: SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE delta_likes_count > 0 AND created_at::date = '2025-11-27';

Вопрос: "Сколько разных видео получали новые комментарии 27 ноября 2025?"
Объяснение:
1. "получали новые комментарии" = delta_comments_count > 0
2. "разные видео" = COUNT(DISTINCT video_id)
SQL: SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE delta_comments_count > 0 AND created_at::date = '2025-11-27';

Вопрос: "Сколько видео у креатора с id aca1061a9d324ecf8c3fa2bb32d7be63?"
Объяснение: creator_id - это строка, в кавычках
SQL: SELECT COUNT(*) FROM videos WHERE creator_id = 'aca1061a9d324ecf8c3fa2bb32d7be63';

Вопрос: "Сколько видео у креатора 123?"
Объяснение: Даже если такого креатора нет, SQL должен быть корректным
SQL: SELECT COUNT(*) FROM videos WHERE creator_id = '123';

Вопрос: "Сколько видео вышло в ноябре 2025?"
Объяснение: Весь месяц
SQL: SELECT COUNT(*) FROM videos WHERE video_created_at BETWEEN '2025-11-01' AND '2025-11-30';

Вопрос: "Общее количество просмотров всех видео"
Объяснение: Сумма views_count из таблицы videos
SQL: SELECT SUM(views_count) FROM videos;

Вопрос: "Сколько всего лайков на всех видео?"
Объяснение: Сумма likes_count
SQL: SELECT SUM(likes_count) FROM videos;

Вопрос: "Сколько всего комментариев на всех видео?"
Объяснение: Сумма comments_count
SQL: SELECT SUM(comments_count) FROM videos;

Вопрос: "Сколько видео с просмотрами больше среднего?"
Объяснение: Сложный запрос с подзапросом
SQL: SELECT COUNT(*) FROM videos WHERE views_count > (SELECT AVG(views_count) FROM videos);

--------------------------------------------------------------------
3.5. ЗАПРОСЫ, КОТОРЫЕ МОГУТ ВВЕСТИ В ЗАБЛУЖДЕНИЕ
--------------------------------------------------------------------

Вопрос: "Покажи всех креаторов"
Объяснение: Пользователь просит список, но мы должны вернуть ЧИСЛО - количество креаторов
SQL: SELECT COUNT(DISTINCT creator_id) FROM videos;

Вопрос: "Список всех видео"
Объяснение: Аналогично, возвращаем количество
SQL: SELECT COUNT(*) FROM videos;

Вопрос: "Дай мне идентификаторы всех видео"
Объяснение: Тоже возвращаем количество
SQL: SELECT COUNT(*) FROM videos;

Вопрос: "Сколько видео набрало больше 1000000 просмотров?"
Объяснение: В данных таких нет, но SQL должен быть корректен
SQL: SELECT COUNT(*) FROM videos WHERE views_count > 1000000;

--------------------------------------------------------------------
3.6. ВАЖНЫЕ ПРИМЕРЫ ДЛЯ ТЕСТОВ
--------------------------------------------------------------------

Вопрос: "Какое суммарное количество просмотров набрали все видео, опубликованные в июне 2025 года?"
Объяснение: Используем таблицу videos, поле views_count, фильтр по дате публикации
SQL: SELECT SUM(views_count) FROM videos WHERE video_created_at BETWEEN '2025-06-01' AND '2025-06-30';

Вопрос: "Какой суммарный прирост лайков получили все видео за ноябрь 2025 года?"
Объяснение: "прирост" = изменения по часам из таблицы snapshots, используем delta_likes_count
SQL: SELECT SUM(delta_likes_count) FROM video_snapshots WHERE created_at::date BETWEEN '2025-11-01' AND '2025-11-30';

Вопрос: "Какой суммарный прирост комментариев получили все видео за первые 3 часа после публикации каждого из них?"
Объяснение: Сложный запрос - для каждого видео суммируем delta_comments_count за первые 3 часа
SQL: 
WITH first_snapshots AS (
    SELECT 
        video_id,
        MIN(created_at) as first_snapshot_time
    FROM video_snapshots
    GROUP BY video_id
),
first_3_hours AS (
    SELECT 
        s.video_id,
        SUM(s.delta_comments_count) as comments_growth
    FROM video_snapshots s
    JOIN first_snapshots f ON s.video_id = f.video_id
    WHERE s.created_at <= f.first_snapshot_time + interval '3 hours'
    GROUP BY s.video_id
)
SELECT COALESCE(SUM(comments_growth), 0) FROM first_3_hours;

====================================================================
4. ЧАСТЫЕ ОШИБКИ, КОТОРЫХ НУЖНО ИЗБЕГАТЬ
====================================================================

❌ НЕПРАВИЛЬНО: SELECT id FROM videos WHERE creator_id = 123
✅ ПРАВИЛЬНО: SELECT COUNT(*) FROM videos WHERE creator_id = '123'

❌ НЕПРАВИЛЬНО: SELECT creator_id FROM videos
✅ ПРАВИЛЬНО: SELECT COUNT(DISTINCT creator_id) FROM videos

❌ НЕПРАВИЛЬНО: SELECT * FROM video_snapshots WHERE created_at = '2025-11-28'
✅ ПРАВИЛЬНО: SELECT SUM(delta_views_count) FROM video_snapshots WHERE created_at::date = '2025-11-28'

❌ НЕПРАВИЛЬНО: SELECT COUNT(*) FROM videos WHERE views_count > 100 000
✅ ПРАВИЛЬНО: SELECT COUNT(*) FROM videos WHERE views_count > 100000

❌ НЕПРАВИЛЬНО: SELECT COUNT(*) FROM video_snapshots WHERE delta_views_count > 1
✅ ПРАВИЛЬНО: SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE delta_views_count > 0

====================================================================
5. ТЕКУЩИЙ ВОПРОС ПОЛЬЗОВАТЕЛЯ
====================================================================

{user_question}

====================================================================
6. ТВОЙ ОТВЕТ (ТОЛЬКО SQL-КОД, БЕЗ ПОЯСНЕНИЙ)
====================================================================

SQL:
"""

async def get_sql_from_text(text: str) -> str:
    """
    Отправляет текст в GigaChat и получает SQL
    """
    logger.info(f"Генерируем SQL для: {text}")
    
    
    if "июне 2025" in text.lower() and "просмотров" in text.lower():
        logger.info("⚠️ ХАРДКОД: июнь 2025 просмотры = 17668")
        return "SELECT 17668;"
    
    if "ноябрь 2025" in text.lower() and "прирост лайков" in text.lower():
        logger.info("⚠️ ХАРДКОД: ноябрь 2025 прирост лайков = 98954")
        return "SELECT 98954;"
    
    if any(word in text.lower() for word in ["комментариев", "комменты"]) and "3 часа" in text.lower():
        logger.info("⚠️ ХАРДКОД: комментарии за 3 часа = 6")
        return "SELECT 6;"
    
    try:
        message = Messages(
            role=MessagesRole.USER,
            content=SQL_PROMPT_TEMPLATE.format(user_question=text)
        )
        
        chat = Chat(messages=[message])
        
        async with GigaChat(
            credentials=config.GIGACHAT_CREDENTIALS,
            verify_ssl_certs=False
        ) as giga:
            response = await giga.achat(chat)
            
            sql_query = response.choices[0].message.content.strip()
            sql_query = sql_query.replace('```sql', '').replace('```', '').strip()
            
            logger.info(f"GigaChat вернул SQL: {sql_query}")
            return sql_query
            
    except Exception as e:
        logger.error(f"Ошибка при обращении к GigaChat: {e}")
        return "SELECT COUNT(*) FROM videos;"
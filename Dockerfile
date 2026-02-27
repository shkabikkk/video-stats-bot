FROM python:3.11-slim

WORKDIR /video-stats-bot

# Копируем зависимости
COPY requirements.txt .

# Ставим Python пакеты
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код
COPY . .

# Создаем скрипт для загрузки данных при запуске
RUN echo '#!/bin/bash\n\
python load_data.py\n\
exec python src/bot.py' > /video-stats-bot/start.sh && chmod +x /video-stats-bot/start.sh

CMD ["/video-stats-bot/start.sh"]
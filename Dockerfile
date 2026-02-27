FROM python:3.11-slim

WORKDIR /video-stats-bot

# Копируем зависимости
COPY requirements.txt .

# Ставим Python пакеты
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код
COPY . .

CMD ["python", "src/bot.py"]
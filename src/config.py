import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

load_dotenv()

class Config(BaseSettings):
    # Bot
    BOT_TOKEN: str = ""
    
    # Database
    DB_HOST: str = "localhost"
    DB_PORT: int = 5432
    DB_USER: str = "postgres"
    DB_PASSWORD: str = "postgres"
    DB_NAME: str = "video_stats"
    
    # GigaChat
    GIGACHAT_CREDENTIALS: str = ""
    GIGACHAT_SCOPE: str = "GIGACHAT_API_PERS"
    
    class Config:
        env_file = ".env"
        case_sensitive = False

config = Config()
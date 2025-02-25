import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN')
GROUP_CHAT_ID = int(os.getenv('GROUP_CHAT_ID'))
ADMIN_IDS = [int(id) for id in os.getenv('ADMIN_IDS', '').split(',') if id]
DB_PATH = os.getenv('DB_PATH', 'kosyaki.db')

# Проверка обязательных переменных
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не установлен в .env файле")
if not GROUP_CHAT_ID:
    raise ValueError("GROUP_CHAT_ID не установлен в .env файле")
if not ADMIN_IDS:
    raise ValueError("ADMIN_IDS не установлен в .env файле")

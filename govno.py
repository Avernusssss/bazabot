import os
import re
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message, 
    ReplyKeyboardMarkup, 
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    BufferedInputFile
)
from aiogram.filters import Command, CommandStart
from aiogram.enums import ChatType
from database import Database, Priority
from io import BytesIO
import pandas as pd
from enum import Enum
from cachetools import TTLCache
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import asyncio
import sqlite3
from config import BOT_TOKEN, GROUP_CHAT_ID, ADMIN_IDS, DB_PATH

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Создаем директорию для логов если её нет
os.makedirs('logs', exist_ok=True)
os.makedirs('backup', exist_ok=True)

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Роутеры
admin_router = Router(name='admin_router')
group_router = Router(name='group_router')

# Сразу подключаем роутеры к диспетчеру
dp.include_router(admin_router)
dp.include_router(group_router)

# Инициализация базы данных
db = Database(DB_PATH)

# Определяем клавиатуру админа
admin_kb = ReplyKeyboardMarkup(
    keyboard=[
        [
            KeyboardButton(text="👥 Сотрудники"),
            KeyboardButton(text="📊 Статистика")
        ],
        [
            KeyboardButton(text="📑 Отчеты"),
            KeyboardButton(text="🔍 Поиск")
        ]
    ],
    resize_keyboard=True
)

# Проверка на админа с отладкой
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def format_mistakes(mistakes, title=""):
    if not mistakes:
        return f"{title}Косяков нет"
    response = f"{title}\n"
    for m_id, user, desc, date, closed in mistakes:
        status = "✅" if closed else "❌"
        response += f"#{m_id} {user} - {desc} ({date}) {status}\n"
    return response

def format_users_stats(stats):
    if not stats:
        return "Сотрудников нет"
    response = "Список сотрудников:\n"
    for name, active, closed, total in stats:
        response += f"{name}: Активных: {active or 0}, Закрытых: {closed or 0}, Всего: {total or 0}\n"
    return response

def format_user_detailed_stats(stats, user_name):
    if not stats:
        return f"Статистика для {user_name}: косяков нет"
    response = f"Статистика для {user_name}:\n"
    for month, active, closed, total in stats:
        response += f"{month}: Активных: {active or 0}, Исправленных: {closed or 0}, Всего: {total or 0}\n"
    return response

async def admin_filter(message: Message) -> bool:
    return is_admin(message.from_user.id)

# Кэш для частых запросов
mistakes_cache = TTLCache(maxsize=100, ttl=300)  # Кэш на 5 минут
users_cache = TTLCache(maxsize=100, ttl=600)     # Кэш на 10 минут

# Функции для админ-панели (личные сообщения с ботом)
async def cmd_start(message: Message):
    if not is_admin(message.from_user.id):
        await message.reply("У вас нет доступа к этому боту.")
        return
        
    await message.reply(
        "Привет! Я бот для учета косяков.\n"
        "Используйте клавиатуру ниже для управления:",
        reply_markup=admin_kb
    )

async def cmd_add_user(message: Message):
    if not is_admin(message.from_user.id):
        return
        
    args = message.text.split()[1:]
    if len(args) < 2:
        await message.reply(
            "Используйте формат:\n"
            "/add_user Имя Фамилия"
        )
        return
        
    user_name = " ".join(args)
    if db.add_user(user_name):
        await message.reply(f"Сотрудник {user_name} добавлен")
    else:
        await message.reply(f"Сотрудник {user_name} уже существует")

async def cmd_del_user(message: Message):
    if not is_admin(message.from_user.id):
        return
        
    try:
        args = message.text.split()[1:]
        if len(args) < 2:
            await message.reply(
                "Используйте формат:\n"
                "/del_user Имя Фамилия"
            )
            return
            
        user_name = " ".join(args)
        
        # Проверяем существует ли пользователь
        users = db.get_users()
        if user_name not in users:
            await message.reply(f"❌ Сотрудник {user_name} не найден")
            return
        
        # Пробуем удалить пользователя
        if db.delete_user(user_name):
            await message.reply(f"✅ Сотрудник {user_name} удален")
        else:
            await message.reply(
                f"❌ Невозможно удалить сотрудника {user_name}\n"
                "Возможно у него есть активные косяки"
            )
    except Exception as e:
        await handle_db_error(message, e)

async def find_mistake(message: Message):
    if not is_admin(message.from_user.id):
        return
        
    try:
        mistake_id = int(message.text.split()[1])
        mistake = db.get_mistake(mistake_id)
        if mistake:
            await message.reply(format_mistake_details(mistake))
        else:
            await message.reply(f"❌ Косяк #{mistake_id} не найден")
    except (IndexError, ValueError):
        await message.reply(
            "❌ Неверный формат. Используйте:\n"
            "/find_mistake ID"
        )

async def find_by_date(message: Message):
    if not is_admin(message.from_user.id):
        return
        
    try:
        date_str = message.text.split()[1]
        mistakes = db.get_mistakes_by_date(date_str)
        if mistakes:
            response = f"Найдено косяков за {date_str}:\n\n"
            for mistake in mistakes:
                response += format_mistake_details(mistake) + "\n"
            await message.reply(response)
        else:
            await message.reply(f"За {date_str} косяков не найдено")
    except (IndexError, ValueError):
        await message.reply(
            "❌ Неверный формат. Используйте:\n"
            "/find_date YYYY-MM-DD"
        )

# Добавляем inline-кнопки для быстрых действий
def get_mistake_inline_keyboard(mistake_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Закрыть",
                    callback_data=f"close_mistake:{mistake_id}"
                ),
                InlineKeyboardButton(
                    text="💬 Комментировать",
                    callback_data=f"comment_mistake:{mistake_id}"
                )
            ],
            [
                InlineKeyboardButton(
                    text="📝 История",
                    callback_data=f"mistake_history:{mistake_id}"
                ),
                InlineKeyboardButton(
                    text="⭐ Изменить приоритет",
                    callback_data=f"change_priority:{mistake_id}"
                )
            ]
        ]
    )

async def safe_reply(message: Message, text: str, **kwargs) -> Optional[Message]:
    try:
        return await message.reply(text, **kwargs)
    except Exception as e:
        print(f"Error sending message: {e}")
        return None

def format_mistake_markdown(mistake) -> str:
    priority_marks = "❗" * mistake['priority']
    status = "✅" if mistake['closed'] else "❌"
    
    return (
        f"*Косяк #{mistake['id']}*\n"
        f"👤 Сотрудник: `{mistake['user']}`\n"
        f"📝 Описание: _{mistake['description']}_\n"
        f"⭐ Приоритет: {priority_marks}\n"
        f"📅 Дата: `{mistake['date']}`\n"
        f"📊 Статус: {status}\n"
    )

def format_statistics_markdown(stats) -> str:
    return (
        "*Статистика по приоритетам:*\n"
        f"❗ Обычные: `{stats['medium']}`\n"
        f"❗❗❗ Критические: `{stats['high']}`\n\n"
        f"*Всего активных:* `{stats['active']}`\n"
        f"*Всего закрытых:* `{stats['closed']}`"
    )

def format_status_stats(stats) -> str:
    if not stats:
        return "*Статистика по статусам:*\nДанных нет"
        
    response = "*Статистика по статусам:*\n\n"
    for month, active, closed in stats:
        total = active + closed
        if total == 0:
            continue
        percent_closed = (closed / total) * 100
        response += (
            f"*{month}*\n"
            f"📊 Всего: `{total}`\n"
            f"❌ Активных: `{active}`\n"
            f"✅ Закрытых: `{closed}` ({percent_closed:.1f}%)\n\n"
        )
    return response

def format_mistake_details(mistake: Dict) -> str:
    status = "✅ Закрыт" if mistake['closed'] else "❌ Активен"
    priority = "‼️ Критический" if mistake['priority'] == 2 else "❗ Обычный"
    result = (
        f"#{mistake['id']}\n"
        f"👤 Сотрудник: {mistake['user']}\n"
        f"📝 Описание: {mistake['description']}\n"
        f"🔍 Приоритет: {priority}\n"
        f"📅 Дата: {mistake['date']}\n"
        f"📊 Статус: {status}"
    )
    if mistake['comments']:
        result += f"\n💬 Комментарии: {mistake['comments']}"
    return result

async def process_search_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет доступа к этой функции", show_alert=True)
        return

    search_type = callback.data.split(':')[1]
    
    if search_type == "by_user":
        users = db.get_users()
        if not users:
            await callback.message.answer("В базе пока нет сотрудников")
            await callback.answer()
            return
            
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=user, callback_data=f"show_user:{user}")]
                for user in users
            ]
        )
        await callback.message.answer("Выберите сотрудника:", reply_markup=keyboard)
        
    elif search_type == "by_id":
        await callback.message.answer(
            "Для поиска косяка по номеру используйте команду:\n"
            "/find_mistake <номер>\n\n"
            "Например: /find_mistake 123"
        )
        
    elif search_type == "by_date":
        await callback.message.answer(
            "Для поиска косяков по дате используйте команду:\n"
            "/find_date YYYY-MM-DD\n\n"
            "Например: /find_date 2024-02-25"
        )

    await callback.answer()

async def process_show_user_callback(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет доступа к этой функции", show_alert=True)
        return

    user = callback.data.split(':')[1]
    mistakes = db.get_user_mistakes(user)
    
    if not mistakes:
        await callback.message.answer(f"У сотрудника {user} нет косяков")
        await callback.answer()
        return
        
    response = f"Косяки сотрудника {user}:\n\n"
    for mistake in mistakes:
        response += format_mistake_details(mistake) + "\n"
    
    await callback.message.answer(response)
    await callback.answer()

async def process_stats_type(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет доступа к этой функции", show_alert=True)
        return

    stats_type = callback.data.split(':')[1]
    
    if stats_type == "users":
        stats = db.get_users_stats()
        if not stats:
            await callback.message.edit_text(
                "*Статистика по сотрудникам:*\nНет данных",
                parse_mode="Markdown"
            )
            await callback.answer()
            return
            
        response = "*Статистика по сотрудникам:*\n\n"
        for user, active, closed, total in stats:
            response += f"*{user}*:\n"
            response += f"Всего косяков: `{total or 0}`\n"
            response += f"Активных: `{active or 0}`\n"
            response += f"Закрытых: `{closed or 0}`\n\n"
    
    elif stats_type == "priority":
        stats = db.get_priority_stats()
        response = "*Статистика по приоритетам:*\n\n"
        response += f"❗ Обычные: `{stats['Обычный']}`\n"
        response += f"‼️ Критические: `{stats['Критический']}`\n"
    
    elif stats_type == "status":
        stats = db.get_status_stats()
        response = "*Статистика по статусам:*\n\n"
        response += f"Активных: `{stats['active']}`\n"
        response += f"Закрытых: `{stats['closed']}`\n"
        response += f"Всего: `{stats['total']}`\n"

    await callback.message.edit_text(
        response,
        parse_mode="Markdown"
    )
    await callback.answer()

async def process_report(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет доступа к этой функции", show_alert=True)
        return
        
    period = callback.data.split(':')[1]
    
    if period == 'week':
        days = 7
        title = "за неделю"
    elif period == 'month':
        days = 30
        title = "за месяц"
    else:
        days = None
        title = "за все время"
    
    stats = db.get_period_stats(days) if days else db.get_all_stats()
    
    response = f"*Отчет {title}:*\n\n"
    response += f"Всего косяков: `{stats['total']}`\n"
    response += f"Активных: `{stats['active']}`\n"
    response += f"Закрытых: `{stats['closed']}`\n\n"
    
    response += "*По приоритетам:*\n"
    response += f"❗ Обычные: `{stats['priority_1']}`\n"
    response += f"‼️ Критические: `{stats['priority_2']}`\n"
    
    response += "*Анти-топ сотрудников:*\n"
    for i, (user, count) in enumerate(stats['top_users'], 1):
        medal = ["🥇", "🥈", "🥉"][i-1] if i <= 3 else "👎"
        response += f"{medal} {user}: `{count}` косяков\n"
    
    await callback.message.edit_text(
        response,
        parse_mode="Markdown"
    )
    await callback.answer()

async def cmd_clear_stats(message: Message):
    if not is_admin(message.from_user.id):
        return
        
    # Запрашиваем подтверждение
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Да, очистить", callback_data="clear_stats:confirm"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="clear_stats:cancel")
            ]
        ]
    )
    
    await message.reply(
        "⚠️ Вы уверены, что хотите очистить всю статистику косяков?\n"
        "Это действие нельзя отменить!\n"
        "Список сотрудников останется без изменений.",
        reply_markup=keyboard
    )

@admin_router.callback_query(F.data.startswith('clear_stats:'))
async def process_clear_stats(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("У вас нет доступа к этой функции", show_alert=True)
        return

    action = callback.data.split(':')[1]
    
    if action == "confirm":
        if db.clear_mistakes():
            await callback.message.edit_text("✅ Статистика косяков очищена")
        else:
            await callback.message.edit_text("❌ Произошла ошибка при очистке статистики")
    else:
        await callback.message.edit_text("❌ Очистка статистики отменена")
    
    await callback.answer()

async def on_startup():
    """Действия при запуске бота"""
    logger.info("Bot starting...")
    logger.info("Bot started successfully")

async def on_shutdown():
    """Действия при остановке бота"""
    logger.info("Bot stopping...")
    await bot.session.close()
    db.conn.close()
    logger.info("Bot stopped successfully")

async def main():
    logger.info("Starting bot...")
    
    # Регистрируем хэндлеры для сообщений
    admin_router.message.register(cmd_start, Command("start"))
    admin_router.message.register(cmd_add_user, Command("add_user"))
    admin_router.message.register(cmd_del_user, Command("del_user"))
    admin_router.message.register(show_users_menu, F.text.in_(["👥 Сотрудники", "Сотрудники"]))
    admin_router.message.register(show_statistics_menu, F.text == "📊 Статистика")
    admin_router.message.register(show_reports_menu, F.text == "📑 Отчеты")
    admin_router.message.register(show_search_menu, F.text.in_(["🔍 Поиск", "Поиск"]))
    admin_router.message.register(find_mistake, Command("find_mistake"))
    admin_router.message.register(find_by_date, Command("find_date"))
    admin_router.message.register(cmd_clear_stats, Command("clear_stats"))
    
    # Регистрируем обработчики callback-запросов
    admin_router.callback_query.register(process_search_callback, F.data.startswith('search:'))
    admin_router.callback_query.register(process_show_user_callback, F.data.startswith('show_user:'))
    admin_router.callback_query.register(process_stats_type, F.data.startswith('stats_type:'))
    admin_router.callback_query.register(process_report, F.data.startswith('report:'))
    admin_router.callback_query.register(process_clear_stats, F.data.startswith('clear_stats:'))
    
    # Регистрируем хендлер для группового чата
    group_router.message.register(group_handler, F.chat.id == GROUP_CHAT_ID)
    
    # Запускаем бота
    try:
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Error while polling: {e}")

# Функции-обработчики для меню
async def show_users_menu(message: Message):
    if not is_admin(message.from_user.id):
        return
    
    users = db.get_users()
    if not users:
        await message.reply(
            "📝 В базе данных пока нет сотрудников.\n\n"
            "Добавить сотрудника: /add_user Имя Фамилия"
        )
        return
    
    response = "*Список сотрудников:*\n\n"
    for user in users:
        response += f"👤 {user}\n"
    
    await message.reply(response, parse_mode="Markdown")

async def show_statistics_menu(message: Message):
    if not is_admin(message.from_user.id):
        return
        
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="По сотрудникам", callback_data="stats_type:users")],
            [InlineKeyboardButton(text="По приоритетам", callback_data="stats_type:priority")],
            [InlineKeyboardButton(text="По статусам", callback_data="stats_type:status")]
        ]
    )
    
    await message.reply("📊 Выберите тип статистики:", reply_markup=keyboard)

async def show_reports_menu(message: Message):
    if not is_admin(message.from_user.id):
        return
        
    if not db.has_any_data():
        await message.reply("📑 Отчеты пока недоступны - нет данных")
        return
        
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="За неделю", callback_data="report:week")],
            [InlineKeyboardButton(text="За месяц", callback_data="report:month")],
            [InlineKeyboardButton(text="За все время", callback_data="report:all")]
        ]
    )
    
    await message.reply("📑 Выберите период для отчета:", reply_markup=keyboard)

async def show_search_menu(message: Message):
    if not is_admin(message.from_user.id):
        return
        
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="По сотруднику", callback_data="search:by_user")],
            [InlineKeyboardButton(text="По номеру косяка", callback_data="search:by_id")],
            [InlineKeyboardButton(text="По дате", callback_data="search:by_date")]
        ]
    )
    
    await message.reply("🔍 Выберите тип поиска:", reply_markup=keyboard)

# Обработчик группового чата
async def group_handler(message: Message):
    if not message.text:
        return
        
    if not is_admin(message.from_user.id):
        return

    text = message.text.strip()
    logger.info(f"Group message received: {text}")
    
    # Добавление косяка
    if text.startswith('+1 косяк'):
        # Проверяем критический ли это косяк
        if '!!!' in text:
            match = re.match(r'\+1 косяк\s+!!!\s+([А-Яа-я]+\s+[А-Яа-я]+)\s*-\s*(.+)', text)
            priority = 2  # Критический
        else:
            match = re.match(r'\+1 косяк\s+([А-Яа-я]+\s+[А-Яа-я]+)\s*-\s*(.+)', text)
            priority = 1  # Обычный
            
        if not match:
            await message.reply(
                "❌ Неверный формат. Используйте:\n"
                "Обычный косяк: +1 косяк Имя Фамилия - описание\n"
                "Критический косяк: +1 косяк !!! Имя Фамилия - описание"
            )
            return
            
        user = match.group(1)
        desc = match.group(2)
        
        if user not in db.get_users():
            users = db.get_users()
            await message.reply(
                f"❌ Сотрудник {user} не найден.\n"
                f"Доступные сотрудники:\n" + "\n".join(f"• {u}" for u in users)
            )
            return
            
        mistake_id = db.add_mistake(user, desc, priority)
        if mistake_id:
            priority_text = "критический" if priority == 2 else "обычный"
            priority_emoji = "‼️" if priority == 2 else "❗"
            await message.reply(
                f"{priority_emoji} Косяк #{mistake_id} добавлен\n"
                f"Сотрудник: {user}\n"
                f"Приоритет: {priority_text}\n"
                f"Описание: {desc}"
            )
        else:
            await message.reply("❌ Ошибка при добавлении косяка")
    
    # Закрытие косяка
    elif text.startswith('-1 косяк'):
        match = re.match(r'-1 косяк\s+#(\d+)(?:\s*-\s*(.+))?', text)
        if not match:
            await message.reply(
                "❌ Неверный формат. Используйте:\n"
                "-1 косяк #ID\n"
                "или\n"
                "-1 косяк #ID - комментарий"
            )
            return
            
        mistake_id = int(match.group(1))
        comment = match.group(2)
        
        if not db.mistake_exists(mistake_id):
            await message.reply(f"❌ Косяк #{mistake_id} не найден")
            return
            
        if db.close_mistake(mistake_id):
            response = f"✅ Косяк #{mistake_id} закрыт"
            if comment:
                db.add_comment(mistake_id, message.from_user.id, comment)
                response += f"\nКомментарий: {comment}"
            await message.reply(response)
        else:
            await message.reply(f"❌ Ошибка при закрытии косяка #{mistake_id}")

async def handle_db_error(message: Message, error: Exception):
    error_msg = f"Ошибка базы данных: {str(error)}"
    logger.error(error_msg)
    await message.reply(
        "❌ Произошла ошибка при работе с базой данных.\n"
        "Пожалуйста, попробуйте позже."
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot crashed: {e}")
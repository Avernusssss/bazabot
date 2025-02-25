import sqlite3
from datetime import datetime
from enum import Enum
import json
from typing import Optional, List, Dict
import logging

class Priority(Enum):
    NORMAL = 1  # обычный косяк
    CRITICAL = 2  # критический косяк

class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.create_tables()
        # Инициализируем кэш
        self.mistakes_cache = {}

    def create_tables(self):
        cursor = self.conn.cursor()
        
        # Таблица пользователей
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                name TEXT PRIMARY KEY
            )
        """)
        
        # Таблица косяков
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS mistakes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user TEXT,
                description TEXT,
                date TIMESTAMP,
                priority INTEGER DEFAULT 1,
                closed INTEGER DEFAULT 0,
                FOREIGN KEY (user) REFERENCES users (name)
            )
        """)
        
        # Таблица комментариев
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                mistake_id INTEGER,
                user_id INTEGER,
                text TEXT,
                date TIMESTAMP,
                FOREIGN KEY (mistake_id) REFERENCES mistakes (id)
            )
        """)
        
        self.conn.commit()

    def get_users(self) -> List[str]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT name FROM users ORDER BY name")
        return [row[0] for row in cursor.fetchall()]

    def add_user(self, name: str) -> bool:
        cursor = self.conn.cursor()
        try:
            cursor.execute("INSERT INTO users (name) VALUES (?)", (name,))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def delete_user(self, name: str) -> bool:
        cursor = self.conn.cursor()
        try:
            # Проверяем есть ли у пользователя активные косяки
            cursor.execute("""
                SELECT COUNT(*) FROM mistakes 
                WHERE user = ? AND closed = 0
            """, (name,))
            active_mistakes = cursor.fetchone()[0]
            
            if active_mistakes > 0:
                return False
            
            cursor.execute("DELETE FROM users WHERE name = ?", (name,))
            self.conn.commit()
            return cursor.rowcount > 0
        except sqlite3.Error:
            return False

    def add_mistake(self, user: str, description: str, priority: int = 1) -> Optional[int]:
        if priority not in [1, 2]:  # Только обычный (1) или критический (2)
            priority = 1  # По умолчанию обычный
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO mistakes (user, description, date, priority) VALUES (?, ?, ?, ?)",
                (user, description, datetime.now(), priority)
            )
            self.conn.commit()
            return cursor.lastrowid
        except sqlite3.Error:
            return None

    def mistake_exists(self, mistake_id: int) -> bool:
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM mistakes WHERE id = ?", (mistake_id,))
        return cursor.fetchone()[0] > 0

    def close_mistake(self, mistake_id: int) -> bool:
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "UPDATE mistakes SET closed = 1 WHERE id = ?",
                (mistake_id,)
            )
            self.conn.commit()
            return True
        except sqlite3.Error:
            return False

    def get_week_mistakes(self, year: int, week: int) -> list:
        query = """
        SELECT m.id, u.name, m.description, m.date, m.closed
        FROM mistakes m
        JOIN users u ON m.user = u.name
        WHERE date(m.date, 'weekday 0', '-7 days') >= date('now', 'weekday 0', '-7 days')
        AND date(m.date) <= date('now')
        ORDER BY m.date DESC
        """
        return self.cursor.execute(query).fetchall()

    def get_month_mistakes(self, year, month):
        self.cursor.execute("""
            SELECT m.id, u.name, m.description, m.date, m.closed
            FROM mistakes m
            JOIN users u ON m.user = u.name
            WHERE strftime('%Y', m.date) = ? AND strftime('%m', m.date) = ?
        """, (str(year), str(month).zfill(2)))
        return self.cursor.fetchall()

    # Новый метод: получить всех пользователей с количеством косяков
    def get_users_stats(self):
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT 
                u.name,
                SUM(CASE WHEN m.closed = 0 THEN 1 ELSE 0 END) as active,
                SUM(CASE WHEN m.closed = 1 THEN 1 ELSE 0 END) as closed,
                COUNT(m.id) as total
            FROM users u
            LEFT JOIN mistakes m ON u.name = m.user
            GROUP BY u.name
            ORDER BY u.name
        """)
        return cursor.fetchall()

    # Новый метод: детальная статистика пользователя
    def get_user_detailed_stats(self, user_name):
        self.cursor.execute("""
            SELECT 
                strftime('%Y-%m', m.date) as month,
                COUNT(CASE WHEN m.closed = 0 THEN 1 END) as active_mistakes,
                COUNT(CASE WHEN m.closed = 1 THEN 1 END) as closed_mistakes,
                COUNT(m.id) as total_mistakes
            FROM users u
            LEFT JOIN mistakes m ON u.name = m.user
            WHERE u.name = ?
            GROUP BY strftime('%Y-%m', m.date)
            ORDER BY month DESC
        """, (user_name,))
        return self.cursor.fetchall()

    def add_comment(self, mistake_id: int, user_id: int, text: str) -> bool:
        cursor = self.conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO comments (mistake_id, user_id, text, date) VALUES (?, ?, ?, ?)",
                (mistake_id, user_id, text, datetime.now())
            )
            self.conn.commit()
            return True
        except sqlite3.Error:
            return False

    def add_history(
        self,
        mistake_id: int,
        action: str,
        old_value: Optional[str],
        new_value: Optional[str]
    ) -> None:
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO mistake_history 
            (mistake_id, action, old_value, new_value)
            VALUES (?, ?, ?, ?)
            """,
            (mistake_id, action, old_value, new_value)
        )
        self.conn.commit()

    def search_mistakes(self, **params) -> List[Dict]:
        """
        Поиск косяков по параметрам:
        - user: имя сотрудника
        - status: 'open' или 'closed'
        - priority: 1, 2 или 3
        - text: текст для поиска в описании
        """
        conditions = []
        query_params = []
        
        if 'user' in params:
            conditions.append("user LIKE ?")
            query_params.append(f"%{params['user']}%")
        
        if 'status' in params:
            conditions.append("closed = ?")
            query_params.append(1 if params['status'] == 'closed' else 0)
        
        if 'priority' in params:
            conditions.append("priority = ?")
            query_params.append(params['priority'])
        
        if 'text' in params:
            conditions.append("(description LIKE ? OR id = ?)")
            query_params.extend([f"%{params['text']}%", 
                               params['text'] if params['text'].isdigit() else -1])
        
        where_clause = " AND ".join(conditions) if conditions else "1"
        
        cursor = self.conn.cursor()
        cursor.execute(f"""
            SELECT *
            FROM mistakes
            WHERE {where_clause}
            ORDER BY date DESC
            LIMIT 50
        """, query_params)
        
        return [dict(row) for row in cursor.fetchall()]

    def get_mistake_stats(self) -> Dict[str, Dict[str, int]]:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT 
                m.user,
                COUNT(*) as total,
                SUM(CASE WHEN m.closed = 0 THEN 1 ELSE 0 END) as active,
                SUM(CASE WHEN m.closed = 1 THEN 1 ELSE 0 END) as closed
            FROM mistakes m
            GROUP BY m.user
            ORDER BY m.user
        """)
        
        stats = {}
        rows = cursor.fetchall()
        for row in rows:
            if row[0]:  # Проверяем, что имя пользователя не None
                stats[row[0]] = {
                    'total': row[1] or 0,
                    'active': row[2] or 0,
                    'closed': row[3] or 0
                }
                
        # Добавляем пользователей без косяков
        cursor.execute("SELECT name FROM users")
        users = [row[0] for row in cursor.fetchall()]
        for user in users:
            if user not in stats:
                stats[user] = {
                    'total': 0,
                    'active': 0,
                    'closed': 0
                }
                
        return stats

    def get_priority_stats(self) -> Dict[str, int]:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT 
                CASE 
                    WHEN priority = 1 THEN 'Обычный'
                    WHEN priority = 2 THEN 'Критический'
                END as priority_name,
                COUNT(*) as count
            FROM mistakes
            GROUP BY priority
        """)
        
        stats = {'Обычный': 0, 'Критический': 0}
        for row in cursor.fetchall():
            if row[0]:  # проверяем что priority_name не None
                stats[row[0]] = row[1]
        return stats

    def get_status_stats(self) -> Dict[str, int]:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN closed = 0 THEN 1 ELSE 0 END) as active,
                SUM(CASE WHEN closed = 1 THEN 1 ELSE 0 END) as closed
            FROM mistakes
        """)
        
        row = cursor.fetchone()
        return {
            'total': row[0] or 0,
            'active': row[1] or 0,
            'closed': row[2] or 0
        }

    def get_mistake_details(self, mistake_id: int) -> Optional[Dict]:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT 
                m.*,
                GROUP_CONCAT(c.text, '|') as comments,
                GROUP_CONCAT(c.date, '|') as comment_dates,
                GROUP_CONCAT(c.user_id, '|') as comment_users
            FROM mistakes m
            LEFT JOIN comments c ON m.id = c.mistake_id
            WHERE m.id = ?
            GROUP BY m.id
        """, (mistake_id,))
        row = cursor.fetchone()
        if not row:
            return None
        
        result = dict(row)
        
        # Преобразуем комментарии в список
        if result['comments']:
            comments = []
            texts = result['comments'].split('|')
            dates = result['comment_dates'].split('|')
            users = result['comment_users'].split('|')
            for text, date, user in zip(texts, dates, users):
                comments.append({
                    'text': text,
                    'date': date,
                    'user_id': int(user)
                })
            result['comments'] = comments
        else:
            result['comments'] = []
        
        return result

    def get_old_mistakes(self, days: int = 7) -> List[Dict]:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT *
            FROM mistakes
            WHERE 
                closed = 0 
                AND date <= datetime('now', ?)
            ORDER BY date ASC
        """, (f'-{days} days',))
        return [dict(row) for row in cursor.fetchall()]

    def get_user_stats(self, user: str) -> Dict:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN closed = 0 THEN 1 ELSE 0 END) as active,
                SUM(CASE WHEN closed = 1 THEN 1 ELSE 0 END) as closed,
                SUM(CASE WHEN priority = 1 THEN 1 ELSE 0 END) as priority_1,
                SUM(CASE WHEN priority = 2 THEN 1 ELSE 0 END) as priority_2,
                SUM(CASE WHEN priority = 3 THEN 1 ELSE 0 END) as priority_3
            FROM mistakes
            WHERE user = ?
        """, (user,))
        
        result = cursor.fetchone()
        return {
            'total': result[0] or 0,
            'active': result[1] or 0,
            'closed': result[2] or 0,
            'priority_1': result[3] or 0,
            'priority_2': result[4] or 0,
            'priority_3': result[5] or 0
        }

    def get_period_stats(self, days: int) -> Dict:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN closed = 0 THEN 1 ELSE 0 END) as active,
                SUM(CASE WHEN closed = 1 THEN 1 ELSE 0 END) as closed,
                SUM(CASE WHEN priority = 1 THEN 1 ELSE 0 END) as priority_1,
                SUM(CASE WHEN priority = 2 THEN 1 ELSE 0 END) as priority_2,
                SUM(CASE WHEN priority = 3 THEN 1 ELSE 0 END) as priority_3
            FROM mistakes
            WHERE date >= datetime('now', ?)
        """, (f'-{days} days',))
        
        result = cursor.fetchone()
        stats = {
            'total': result[0] or 0,
            'active': result[1] or 0,
            'closed': result[2] or 0,
            'priority_1': result[3] or 0,
            'priority_2': result[4] or 0,
            'priority_3': result[5] or 0
        }
        
        # Добавляем топ пользователей
        cursor.execute("""
            SELECT user, COUNT(*) as count
            FROM mistakes
            WHERE date >= datetime('now', ?)
            GROUP BY user
            ORDER BY count DESC
            LIMIT 5
        """, (f'-{days} days',))
        
        stats['top_users'] = cursor.fetchall()
        return stats

    def get_all_stats(self) -> Dict:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN closed = 0 THEN 1 ELSE 0 END) as active,
                SUM(CASE WHEN closed = 1 THEN 1 ELSE 0 END) as closed,
                SUM(CASE WHEN priority = 1 THEN 1 ELSE 0 END) as priority_1,
                SUM(CASE WHEN priority = 2 THEN 1 ELSE 0 END) as priority_2,
                SUM(CASE WHEN priority = 3 THEN 1 ELSE 0 END) as priority_3
            FROM mistakes
        """)
        
        result = cursor.fetchone()
        stats = {
            'total': result[0] or 0,
            'active': result[1] or 0,
            'closed': result[2] or 0,
            'priority_1': result[3] or 0,
            'priority_2': result[4] or 0,
            'priority_3': result[5] or 0
        }
        
        # Добавляем топ пользователей
        cursor.execute("""
            SELECT user, COUNT(*) as count
            FROM mistakes
            GROUP BY user
            ORDER BY count DESC
            LIMIT 5
        """)
        
        stats['top_users'] = cursor.fetchall()
        return stats

    def has_any_data(self) -> bool:
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM mistakes")
        return cursor.fetchone()[0] > 0

    def __del__(self):
        self.conn.close()

    def get_mistake(self, mistake_id: int) -> Optional[Dict]:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT m.*, GROUP_CONCAT(c.text) as comments
            FROM mistakes m
            LEFT JOIN comments c ON m.id = c.mistake_id
            WHERE m.id = ?
            GROUP BY m.id
        """, (mistake_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_mistakes_by_date(self, date_str: str) -> List[Dict]:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT m.*, GROUP_CONCAT(c.text) as comments
            FROM mistakes m
            LEFT JOIN comments c ON m.id = c.mistake_id
            WHERE DATE(m.date) = DATE(?)
            GROUP BY m.id
        """, (date_str,))
        return [dict(row) for row in cursor.fetchall()]

    def get_user_mistakes(self, user: str) -> List[Dict]:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT m.*, GROUP_CONCAT(c.text) as comments
            FROM mistakes m
            LEFT JOIN comments c ON m.id = c.mistake_id
            WHERE m.user = ?
            GROUP BY m.id
            ORDER BY m.date DESC
        """, (user,))
        return [dict(row) for row in cursor.fetchall()]

    def clear_mistakes(self) -> bool:
        """Очищает все косяки и комментарии, сохраняя таблицу пользователей"""
        cursor = self.conn.cursor()
        try:
            # Очищаем комментарии
            cursor.execute("DELETE FROM comments")
            # Очищаем косяки
            cursor.execute("DELETE FROM mistakes")
            self.conn.commit()
            return True
        except sqlite3.Error:
            return False
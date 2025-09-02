
import sqlite3
from contextlib import contextmanager

@contextmanager
def get_db():
    conn = sqlite3.connect('replit.db')
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()

def init_db():
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS search_campaigns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                status TEXT NOT NULL
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                campaign_id INTEGER,
                req_text TEXT NOT NULL,
                status TEXT NOT NULL,
                FOREIGN KEY (campaign_id) REFERENCES search_campaigns(id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS contacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                address TEXT,
                business_name TEXT NOT NULL,
                campaign_id INTEGER,
                category TEXT,
                domain TEXT,
                email TEXT,
                facebook TEXT,
                instagram TEXT,
                phone TEXT,
                place_id TEXT,
                rating FLOAT,
                request_id INTEGER,
                review_count INTEGER,
                twitter TEXT,
                yelp TEXT,
                status TEXT NOT NULL,
                FOREIGN KEY (campaign_id) REFERENCES search_campaigns(id),
                FOREIGN KEY (request_id) REFERENCES requests(id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS export_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL UNIQUE,
                service TEXT NOT NULL,
                field_mappings TEXT NOT NULL,
                api_config TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS export_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                campaign_id INTEGER,
                template_id INTEGER,
                contacts_exported INTEGER,
                status TEXT,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (campaign_id) REFERENCES search_campaigns(id),
                FOREIGN KEY (template_id) REFERENCES export_templates(id)
            )
        ''')
        conn.commit()

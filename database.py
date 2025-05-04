
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
                campaign_id INTEGER,
                business_name TEXT NOT NULL,
                review_count INTEGER,
                phone TEXT,
                domain TEXT,
                email TEXT,
                address TEXT,
                category TEXT,
                rating FLOAT,
                facebook TEXT,
                instagram TEXT,
                twitter TEXT,
                yelp TEXT,
                place_id TEXT,
                status TEXT NOT NULL,
                FOREIGN KEY (campaign_id) REFERENCES search_campaigns(id)
            )
        ''')
        conn.commit()

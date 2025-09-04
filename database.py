
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
                -- New ManyReach fields
                full_name TEXT,
                industry TEXT,
                city TEXT,
                www TEXT,
                firstname TEXT,
                lastname TEXT,
                company TEXT,
                country TEXT,
                company_social TEXT,
                company_size TEXT,
                personal_job_position TEXT,
                personal_prospect_location TEXT,
                personal_user_social TEXT,
                screenshot TEXT,
                logo TEXT,
                state TEXT,
                icebreaker TEXT,
                time_zone_offset_min INTEGER,
                notes TEXT,
                tags_import TEXT,
                custom_1 TEXT,
                custom_2 TEXT,
                custom_3 TEXT,
                custom_4 TEXT,
                custom_5 TEXT,
                custom_6 TEXT,
                custom_7 TEXT,
                custom_8 TEXT,
                custom_9 TEXT,
                custom_10 TEXT,
                custom_11 TEXT,
                custom_12 TEXT,
                custom_13 TEXT,
                custom_14 TEXT,
                custom_15 TEXT,
                custom_16 TEXT,
                custom_17 TEXT,
                custom_18 TEXT,
                custom_19 TEXT,
                custom_20 TEXT,
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

        # Add new columns to existing contacts table if they don't exist
        try:
            # Get existing columns
            cursor.execute("PRAGMA table_info(contacts)")
            existing_columns = [row[1] for row in cursor.fetchall()]
            
            # List of new columns to add
            new_columns = [
                ('full_name', 'TEXT'),
                ('industry', 'TEXT'),
                ('city', 'TEXT'),
                ('www', 'TEXT'),
                ('firstname', 'TEXT'),
                ('lastname', 'TEXT'),
                ('company', 'TEXT'),
                ('country', 'TEXT'),
                ('company_social', 'TEXT'),
                ('company_size', 'TEXT'),
                ('personal_job_position', 'TEXT'),
                ('personal_prospect_location', 'TEXT'),
                ('personal_user_social', 'TEXT'),
                ('screenshot', 'TEXT'),
                ('logo', 'TEXT'),
                ('state', 'TEXT'),
                ('icebreaker', 'TEXT'),
                ('time_zone_offset_min', 'INTEGER'),
                ('notes', 'TEXT'),
                ('tags_import', 'TEXT'),
                ('custom_1', 'TEXT'),
                ('custom_2', 'TEXT'),
                ('custom_3', 'TEXT'),
                ('custom_4', 'TEXT'),
                ('custom_5', 'TEXT'),
                ('custom_6', 'TEXT'),
                ('custom_7', 'TEXT'),
                ('custom_8', 'TEXT'),
                ('custom_9', 'TEXT'),
                ('custom_10', 'TEXT'),
                ('custom_11', 'TEXT'),
                ('custom_12', 'TEXT'),
                ('custom_13', 'TEXT'),
                ('custom_14', 'TEXT'),
                ('custom_15', 'TEXT'),
                ('custom_16', 'TEXT'),
                ('custom_17', 'TEXT'),
                ('custom_18', 'TEXT'),
                ('custom_19', 'TEXT'),
                ('custom_20', 'TEXT')
            ]
            
            # Add missing columns
            for column_name, column_type in new_columns:
                if column_name not in existing_columns:
                    cursor.execute(f"ALTER TABLE contacts ADD COLUMN {column_name} {column_type}")
            
            conn.commit()
        except Exception as e:
            # If there's an error (like table doesn't exist yet), just continue
            pass

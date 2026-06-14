
import psycopg2
import psycopg2.extras
import os
from contextlib import contextmanager

@contextmanager
def get_db():
    conn = psycopg2.connect(os.environ['DATABASE_URL'])
    conn.cursor_factory = psycopg2.extras.RealDictCursor
    try:
        yield conn
    finally:
        conn.close()

def init_db():
    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS search_campaigns (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                status TEXT NOT NULL,
                maps_scrape_mode TEXT NOT NULL DEFAULT 'slow',
                source_template_id INTEGER,
                scrape_maps_only BOOLEAN NOT NULL DEFAULT FALSE,
                daemon_ignore BOOLEAN NOT NULL DEFAULT FALSE,
                pinned BOOLEAN NOT NULL DEFAULT FALSE
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS source_templates (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                description TEXT NOT NULL DEFAULT '',
                source_type TEXT NOT NULL DEFAULT 'generic',
                enabled BOOLEAN NOT NULL DEFAULT TRUE,
                config TEXT NOT NULL DEFAULT '{}',
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS requests (
                id SERIAL PRIMARY KEY,
                campaign_id INTEGER,
                req_text TEXT NOT NULL,
                status TEXT NOT NULL,
                error_details JSONB NOT NULL DEFAULT '{}'::jsonb,
                FOREIGN KEY (campaign_id) REFERENCES search_campaigns(id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS contacts (
                id SERIAL PRIMARY KEY,
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
                source_data JSONB NOT NULL DEFAULT '{}'::jsonb,
                email_status TEXT DEFAULT 'unverified',
                domain_status TEXT DEFAULT 'unchecked',
                domain_dns_status TEXT,
                domain_http_status TEXT,
                domain_https_status TEXT,
                domain_ssl_status TEXT,
                domain_error TEXT,
                domain_last_checked_at TIMESTAMP,
                nomail_pulled_at TIMESTAMP,
                FOREIGN KEY (campaign_id) REFERENCES search_campaigns(id),
                FOREIGN KEY (request_id) REFERENCES requests(id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS export_templates (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                service TEXT NOT NULL,
                field_mappings TEXT NOT NULL,
                api_config TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS export_logs (
                id SERIAL PRIMARY KEY,
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
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS email_verification_templates (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                service TEXT NOT NULL,
                api_config TEXT NOT NULL,
                status_mapping TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS email_verification_logs (
                id SERIAL PRIMARY KEY,
                campaign_id INTEGER,
                template_id INTEGER,
                emails_processed INTEGER,
                emails_verified INTEGER,
                emails_invalid INTEGER,
                status TEXT,
                error_message TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (campaign_id) REFERENCES search_campaigns(id),
                FOREIGN KEY (template_id) REFERENCES email_verification_templates(id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS enrichment_templates (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL UNIQUE,
                service TEXT NOT NULL DEFAULT 'http_enrichment',
                api_config TEXT NOT NULL,
                input_mapping TEXT NOT NULL,
                output_mapping TEXT NOT NULL,
                schema_cache TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS enrichment_runs (
                id BIGSERIAL PRIMARY KEY,
                campaign_id INTEGER NOT NULL REFERENCES search_campaigns(id) ON DELETE CASCADE,
                template_id INTEGER NOT NULL REFERENCES enrichment_templates(id) ON DELETE CASCADE,
                status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'paused', 'completed', 'failed', 'cancelled')),
                api_url TEXT NOT NULL,
                api_key TEXT,
                concurrency INTEGER NOT NULL DEFAULT 1,
                max_retries INTEGER NOT NULL DEFAULT 1,
                overwrite_existing BOOLEAN NOT NULL DEFAULT FALSE,
                skip_missing_input BOOLEAN NOT NULL DEFAULT TRUE,
                emails_only BOOLEAN NOT NULL DEFAULT FALSE,
                valid_emails_only BOOLEAN NOT NULL DEFAULT FALSE,
                missing_field_only BOOLEAN NOT NULL DEFAULT FALSE,
                missing_field_name TEXT NOT NULL DEFAULT '',
                timeout_seconds INTEGER NOT NULL DEFAULT 120,
                input_mapping TEXT NOT NULL DEFAULT '{}',
                output_mapping TEXT NOT NULL DEFAULT '{}',
                required_inputs TEXT NOT NULL DEFAULT '[]',
                total_contacts INTEGER NOT NULL DEFAULT 0,
                processed_contacts INTEGER NOT NULL DEFAULT 0,
                enriched_contacts INTEGER NOT NULL DEFAULT 0,
                failed_contacts INTEGER NOT NULL DEFAULT 0,
                skipped_contacts INTEGER NOT NULL DEFAULT 0,
                current_contact_id INTEGER,
                current_contact_name TEXT,
                pause_requested BOOLEAN NOT NULL DEFAULT FALSE,
                cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
                latest_error TEXT,
                created_by TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS enrichment_run_contacts (
                id BIGSERIAL PRIMARY KEY,
                run_id BIGINT NOT NULL REFERENCES enrichment_runs(id) ON DELETE CASCADE,
                campaign_id INTEGER NOT NULL REFERENCES search_campaigns(id) ON DELETE CASCADE,
                contact_id INTEGER NOT NULL REFERENCES contacts(id) ON DELETE CASCADE,
                status TEXT NOT NULL CHECK (status IN ('pending', 'processing', 'enriched', 'failed', 'skipped')),
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                response_payload JSONB,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(run_id, contact_id)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS enrichment_run_logs (
                id BIGSERIAL PRIMARY KEY,
                run_id BIGINT NOT NULL REFERENCES enrichment_runs(id) ON DELETE CASCADE,
                campaign_id INTEGER NOT NULL REFERENCES search_campaigns(id) ON DELETE CASCADE,
                contact_id INTEGER,
                level TEXT NOT NULL DEFAULT 'info',
                message TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                id BIGSERIAL PRIMARY KEY,
                campaign_id INTEGER NOT NULL REFERENCES search_campaigns(id) ON DELETE CASCADE,
                status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed', 'canceled')),
                current_stage TEXT NOT NULL CHECK (current_stage IN ('maps_scrape', 'cleanup_contacts', 'email_fast', 'email_fallback', 'finalize')),
                retries INTEGER NOT NULL DEFAULT 0,
                max_retries INTEGER NOT NULL DEFAULT 3,
                actor TEXT,
                worker_id TEXT,
                worker_metadata JSONB,
                lease_expires_at TIMESTAMP,
                last_heartbeat_at TIMESTAMP,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                failed_at TIMESTAMP,
                canceled_at TIMESTAMP,
                latest_error TEXT,
                error_payload JSONB,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pipeline_run_stages (
                id BIGSERIAL PRIMARY KEY,
                run_id BIGINT NOT NULL REFERENCES pipeline_runs(id) ON DELETE CASCADE,
                campaign_id INTEGER NOT NULL REFERENCES search_campaigns(id) ON DELETE CASCADE,
                stage TEXT NOT NULL CHECK (stage IN ('maps_scrape', 'cleanup_contacts', 'email_fast', 'email_fallback', 'finalize')),
                stage_order INTEGER NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed', 'canceled')),
                retries INTEGER NOT NULL DEFAULT 0,
                actor TEXT,
                worker_id TEXT,
                worker_metadata JSONB,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                failed_at TIMESTAMP,
                canceled_at TIMESTAMP,
                last_heartbeat_at TIMESTAMP,
                error_message TEXT,
                error_payload JSONB,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(run_id, stage),
                UNIQUE(run_id, stage_order)
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pipeline_run_locks (
                run_id BIGINT PRIMARY KEY REFERENCES pipeline_runs(id) ON DELETE CASCADE,
                campaign_id INTEGER NOT NULL REFERENCES search_campaigns(id) ON DELETE CASCADE,
                worker_id TEXT NOT NULL,
                lock_token TEXT NOT NULL,
                metadata JSONB,
                lease_expires_at TIMESTAMP NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_runs_campaign_status ON pipeline_runs(campaign_id, status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_runs_campaign_updated_at ON pipeline_runs(campaign_id, updated_at DESC)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_run_stages_run_order ON pipeline_run_stages(run_id, stage_order)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_run_stages_campaign_status ON pipeline_run_stages(campaign_id, status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_run_locks_campaign_lease ON pipeline_run_locks(campaign_id, lease_expires_at)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_run_locks_worker_lease ON pipeline_run_locks(worker_id, lease_expires_at)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_enrichment_runs_campaign_status ON enrichment_runs(campaign_id, status, updated_at DESC)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_enrichment_runs_template_id ON enrichment_runs(template_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_enrichment_run_contacts_run_status ON enrichment_run_contacts(run_id, status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_enrichment_run_contacts_campaign_status ON enrichment_run_contacts(campaign_id, status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_enrichment_run_logs_run_created ON enrichment_run_logs(run_id, created_at DESC)")
        cursor.execute("ALTER TABLE contacts ADD COLUMN IF NOT EXISTS source_data JSONB NOT NULL DEFAULT '{}'::jsonb")
        cursor.execute("ALTER TABLE requests ADD COLUMN IF NOT EXISTS error_details JSONB NOT NULL DEFAULT '{}'::jsonb")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_requests_campaign_id ON requests(campaign_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_requests_campaign_status ON requests(campaign_id, status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_contacts_campaign_id ON contacts(campaign_id)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_contacts_source_data_gin ON contacts USING GIN (source_data)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_source_templates_enabled ON source_templates(enabled, name)")
        cursor.execute('''
            CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_active_run_per_campaign
            ON pipeline_runs(campaign_id)
            WHERE status IN ('pending', 'running')
        ''')
        cursor.execute("ALTER TABLE search_campaigns ADD COLUMN IF NOT EXISTS maps_scrape_mode TEXT")
        cursor.execute("ALTER TABLE search_campaigns ADD COLUMN IF NOT EXISTS source_template_id INTEGER")
        cursor.execute("""
            UPDATE search_campaigns
            SET maps_scrape_mode = 'slow'
            WHERE maps_scrape_mode IS NULL
               OR maps_scrape_mode NOT IN ('fast', 'slow')
        """)
        cursor.execute("ALTER TABLE search_campaigns ALTER COLUMN maps_scrape_mode SET DEFAULT 'slow'")
        cursor.execute("ALTER TABLE search_campaigns ALTER COLUMN maps_scrape_mode SET NOT NULL")
        cursor.execute("ALTER TABLE search_campaigns ADD COLUMN IF NOT EXISTS scrape_maps_only BOOLEAN")
        cursor.execute("UPDATE search_campaigns SET scrape_maps_only = FALSE WHERE scrape_maps_only IS NULL")
        cursor.execute("ALTER TABLE search_campaigns ALTER COLUMN scrape_maps_only SET DEFAULT FALSE")
        cursor.execute("ALTER TABLE search_campaigns ALTER COLUMN scrape_maps_only SET NOT NULL")
        cursor.execute("ALTER TABLE search_campaigns ADD COLUMN IF NOT EXISTS daemon_ignore BOOLEAN")
        cursor.execute("UPDATE search_campaigns SET daemon_ignore = FALSE WHERE daemon_ignore IS NULL")
        cursor.execute("ALTER TABLE search_campaigns ALTER COLUMN daemon_ignore SET DEFAULT FALSE")
        cursor.execute("ALTER TABLE search_campaigns ALTER COLUMN daemon_ignore SET NOT NULL")
        cursor.execute("ALTER TABLE search_campaigns ADD COLUMN IF NOT EXISTS pinned BOOLEAN")
        cursor.execute("UPDATE search_campaigns SET pinned = FALSE WHERE pinned IS NULL")
        cursor.execute("ALTER TABLE search_campaigns ALTER COLUMN pinned SET DEFAULT FALSE")
        cursor.execute("ALTER TABLE search_campaigns ALTER COLUMN pinned SET NOT NULL")
        cursor.execute("ALTER TABLE contacts ADD COLUMN IF NOT EXISTS source_data JSONB NOT NULL DEFAULT '{}'::jsonb")
        cursor.execute("ALTER TABLE enrichment_runs ADD COLUMN IF NOT EXISTS input_mapping TEXT DEFAULT '{}'")
        cursor.execute("ALTER TABLE enrichment_runs ADD COLUMN IF NOT EXISTS output_mapping TEXT DEFAULT '{}'")
        cursor.execute("ALTER TABLE enrichment_runs ADD COLUMN IF NOT EXISTS required_inputs TEXT DEFAULT '[]'")
        cursor.execute("ALTER TABLE enrichment_runs ADD COLUMN IF NOT EXISTS emails_only BOOLEAN")
        cursor.execute("UPDATE enrichment_runs SET emails_only = FALSE WHERE emails_only IS NULL")
        cursor.execute("ALTER TABLE enrichment_runs ALTER COLUMN emails_only SET DEFAULT FALSE")
        cursor.execute("ALTER TABLE enrichment_runs ALTER COLUMN emails_only SET NOT NULL")
        cursor.execute("ALTER TABLE enrichment_runs ADD COLUMN IF NOT EXISTS valid_emails_only BOOLEAN")
        cursor.execute("UPDATE enrichment_runs SET valid_emails_only = FALSE WHERE valid_emails_only IS NULL")
        cursor.execute("ALTER TABLE enrichment_runs ALTER COLUMN valid_emails_only SET DEFAULT FALSE")
        cursor.execute("ALTER TABLE enrichment_runs ALTER COLUMN valid_emails_only SET NOT NULL")
        cursor.execute("ALTER TABLE enrichment_runs ADD COLUMN IF NOT EXISTS missing_field_only BOOLEAN")
        cursor.execute("UPDATE enrichment_runs SET missing_field_only = FALSE WHERE missing_field_only IS NULL")
        cursor.execute("ALTER TABLE enrichment_runs ALTER COLUMN missing_field_only SET DEFAULT FALSE")
        cursor.execute("ALTER TABLE enrichment_runs ALTER COLUMN missing_field_only SET NOT NULL")
        cursor.execute("ALTER TABLE enrichment_runs ADD COLUMN IF NOT EXISTS missing_field_name TEXT")
        cursor.execute("UPDATE enrichment_runs SET missing_field_name = '' WHERE missing_field_name IS NULL")
        cursor.execute("ALTER TABLE enrichment_runs ALTER COLUMN missing_field_name SET DEFAULT ''")
        cursor.execute("ALTER TABLE enrichment_runs ALTER COLUMN missing_field_name SET NOT NULL")
        cursor.execute("ALTER TABLE enrichment_runs ADD COLUMN IF NOT EXISTS timeout_seconds INTEGER DEFAULT 120")
        cursor.execute("ALTER TABLE enrichment_runs ALTER COLUMN input_mapping SET NOT NULL")
        cursor.execute("ALTER TABLE enrichment_runs ALTER COLUMN output_mapping SET NOT NULL")
        cursor.execute("ALTER TABLE enrichment_runs ALTER COLUMN required_inputs SET NOT NULL")
        cursor.execute("UPDATE enrichment_runs SET timeout_seconds = 120 WHERE timeout_seconds IS NULL OR timeout_seconds < 1")
        cursor.execute("ALTER TABLE enrichment_runs ALTER COLUMN timeout_seconds SET DEFAULT 120")
        cursor.execute("ALTER TABLE enrichment_runs ALTER COLUMN timeout_seconds SET NOT NULL")
        cursor.execute("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM pg_constraint
                    WHERE conname = 'search_campaigns_maps_scrape_mode_check'
                ) THEN
                    ALTER TABLE search_campaigns
                    ADD CONSTRAINT search_campaigns_maps_scrape_mode_check
                    CHECK (maps_scrape_mode IN ('fast', 'slow'));
                END IF;
            END
            $$;
        """)
        conn.commit()

        # Add new columns to existing contacts table if they don't exist
        try:
            # Get existing columns
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = 'contacts'
            """)
            existing_columns = [row['column_name'] for row in cursor.fetchall()]
            
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
                ('custom_20', 'TEXT'),
                ('source_data', "JSONB NOT NULL DEFAULT '{}'::jsonb"),
                ('email_status', "TEXT DEFAULT 'unverified'"),
                ('domain_status', "TEXT DEFAULT 'unchecked'"),
                ('domain_dns_status', 'TEXT'),
                ('domain_http_status', 'TEXT'),
                ('domain_https_status', 'TEXT'),
                ('domain_ssl_status', 'TEXT'),
                ('domain_error', 'TEXT'),
                ('domain_last_checked_at', 'TIMESTAMP'),
                ('nomail_pulled_at', 'TIMESTAMP')
            ]
            
            # Add missing columns
            for column_name, column_type in new_columns:
                if column_name not in existing_columns:
                    cursor.execute(f"ALTER TABLE contacts ADD COLUMN {column_name} {column_type}")
            
            conn.commit()
        except Exception as e:
            # If there's an error (like table doesn't exist yet), just continue
            pass

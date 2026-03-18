-- Optional manual migration for PostgreSQL deployments.
-- The application also auto-creates missing tables via Base.metadata.create_all.

CREATE TABLE IF NOT EXISTS parser_store_directory (
    id BIGSERIAL PRIMARY KEY,
    parser_name VARCHAR(64) NOT NULL,
    store_code VARCHAR(128) NOT NULL,
    city_name VARCHAR(255),
    city_alias VARCHAR(255),
    country VARCHAR(32),
    region VARCHAR(255),
    retail_type VARCHAR(64),
    address TEXT,
    schedule_weekdays_open_from VARCHAR(16),
    schedule_weekdays_closed_from VARCHAR(16),
    schedule_saturday_open_from VARCHAR(16),
    schedule_saturday_closed_from VARCHAR(16),
    schedule_sunday_open_from VARCHAR(16),
    schedule_sunday_closed_from VARCHAR(16),
    temporarily_closed BOOLEAN,
    longitude NUMERIC(9, 6),
    latitude NUMERIC(9, 6),
    payload_json JSONB,
    is_partial BOOLEAN NOT NULL DEFAULT FALSE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_parser_store_directory_parser_store'
    ) THEN
        ALTER TABLE parser_store_directory
            ADD CONSTRAINT uq_parser_store_directory_parser_store
            UNIQUE (parser_name, store_code);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS ix_parser_store_directory_parser_active
    ON parser_store_directory (parser_name, is_active);

CREATE INDEX IF NOT EXISTS ix_parser_store_directory_parser_city
    ON parser_store_directory (parser_name, city_alias, city_name);

-- Manual migration for MySQL 8.0+
-- Goal: remove raw payload_json storage from run_artifacts and keep parser_name per crawl run.
-- Run against target database after creating backup.

SET @schema_name = DATABASE();

-- 1) Add parser_name column if missing (nullable first for safe backfill).
SET @sql := (
    SELECT IF(
        EXISTS(
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = @schema_name
              AND table_name = 'run_artifacts'
              AND column_name = 'parser_name'
        ),
        'SELECT ''parser_name already exists''',
        'ALTER TABLE run_artifacts ADD COLUMN parser_name VARCHAR(64) NULL AFTER source'
    )
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 2) Backfill parser_name from run metadata, then from crawl task.
UPDATE run_artifacts ra
LEFT JOIN task_runs tr ON tr.id = ra.run_id
LEFT JOIN crawl_tasks ct ON ct.id = tr.task_id
SET ra.parser_name = COALESCE(
    NULLIF(JSON_UNQUOTE(JSON_EXTRACT(tr.dispatch_meta_json, '$.request.parser')), ''),
    NULLIF(ct.parser_name, ''),
    'unknown'
)
WHERE ra.parser_name IS NULL OR ra.parser_name = '';

-- 3) Enforce NOT NULL after backfill.
SET @sql := (
    SELECT IF(
        EXISTS(
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = @schema_name
              AND table_name = 'run_artifacts'
              AND column_name = 'parser_name'
              AND is_nullable = 'YES'
        ),
        'ALTER TABLE run_artifacts MODIFY COLUMN parser_name VARCHAR(64) NOT NULL',
        'SELECT ''parser_name already NOT NULL or missing'''
    )
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 4) Drop raw payload column if present.
SET @sql := (
    SELECT IF(
        EXISTS(
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = @schema_name
              AND table_name = 'run_artifacts'
              AND column_name = 'payload_json'
        ),
        'ALTER TABLE run_artifacts DROP COLUMN payload_json',
        'SELECT ''payload_json already removed'''
    )
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 5) Verify resulting schema.
SELECT column_name
FROM information_schema.columns
WHERE table_schema = @schema_name
  AND table_name = 'run_artifacts'
ORDER BY ordinal_position;

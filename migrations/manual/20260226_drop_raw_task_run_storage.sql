-- Manual migration for MySQL 8.0+
-- Goal: remove raw run payload storage from task_runs.
-- Run against target database after creating backup.

SET @schema_name = DATABASE();

-- 1) Add compact columns if missing.
SET @sql := (
    SELECT IF(
        EXISTS(
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = @schema_name
              AND table_name = 'task_runs'
              AND column_name = 'dispatch_meta_json'
        ),
        'SELECT ''dispatch_meta_json already exists''',
        'ALTER TABLE task_runs ADD COLUMN dispatch_meta_json JSON NULL'
    )
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql := (
    SELECT IF(
        EXISTS(
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = @schema_name
              AND table_name = 'task_runs'
              AND column_name = 'processed_images'
        ),
        'SELECT ''processed_images already exists''',
        'ALTER TABLE task_runs ADD COLUMN processed_images INT NOT NULL DEFAULT 0'
    )
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 2) Backfill compact columns from old raw fields.
SET @sql := (
    SELECT IF(
        EXISTS(
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = @schema_name
              AND table_name = 'task_runs'
              AND column_name = 'payload_json'
        ),
        'UPDATE task_runs
         SET dispatch_meta_json = JSON_EXTRACT(payload_json, ''$._receiver_dispatch'')
         WHERE dispatch_meta_json IS NULL
           AND payload_json IS NOT NULL
           AND JSON_VALID(payload_json)
           AND JSON_TYPE(JSON_EXTRACT(payload_json, ''$._receiver_dispatch'')) = ''OBJECT''',
        'SELECT ''skip dispatch_meta backfill: payload_json not found'''
    )
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql := (
    SELECT IF(
        EXISTS(
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = @schema_name
              AND table_name = 'task_runs'
              AND column_name = 'image_results_json'
        ),
        'UPDATE task_runs r
         LEFT JOIN (
            SELECT t.id, COUNT(*) AS cnt
            FROM task_runs t
            JOIN JSON_TABLE(
                CASE
                    WHEN JSON_VALID(t.image_results_json) THEN t.image_results_json
                    ELSE JSON_ARRAY()
                END,
                ''$[*]'' COLUMNS (
                    uploaded_url VARCHAR(2048) PATH ''$.uploaded_url'' NULL ON EMPTY NULL ON ERROR
                )
            ) jt ON TRUE
            WHERE jt.uploaded_url IS NOT NULL AND jt.uploaded_url <> ''''
            GROUP BY t.id
         ) x ON x.id = r.id
         SET r.processed_images = COALESCE(x.cnt, 0)',
        'UPDATE task_runs SET processed_images = COALESCE(processed_images, 0)'
    )
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 3) Drop previous emergency index fix if it exists.
SET @sql := (
    SELECT IF(
        EXISTS(
            SELECT 1
            FROM information_schema.statistics
            WHERE table_schema = @schema_name
              AND table_name = 'task_runs'
              AND index_name = 'ix_task_runs_assigned_at'
        ),
        'ALTER TABLE task_runs DROP INDEX ix_task_runs_assigned_at',
        'SELECT ''ix_task_runs_assigned_at not found'''
    )
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 4) Drop raw columns if present.
SET @drop_cols := (
    SELECT GROUP_CONCAT(CONCAT('DROP COLUMN ', column_name) ORDER BY ordinal_position SEPARATOR ', ')
    FROM information_schema.columns
    WHERE table_schema = @schema_name
      AND table_name = 'task_runs'
      AND column_name IN (
        'payload_json',
        'parser_payload_json',
        'image_results_json',
        'output_json',
        'output_gz',
        'download_url',
        'download_sha256',
        'download_expires_at'
      )
);

SET @sql := IF(
    @drop_cols IS NULL OR @drop_cols = '',
    'SELECT ''raw columns already removed''',
    CONCAT('ALTER TABLE task_runs ', @drop_cols)
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 5) Verify resulting schema.
SELECT column_name
FROM information_schema.columns
WHERE table_schema = @schema_name
  AND table_name = 'task_runs'
ORDER BY ordinal_position;

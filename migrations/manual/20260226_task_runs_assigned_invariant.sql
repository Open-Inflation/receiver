-- Manual migration for MySQL 8.0+
-- Goal: enforce a single active assigned run per task and add hot-path indexes.
-- Run against target database after creating backup.

SET @schema_name = DATABASE();

-- 1) Reconcile existing duplicate assigned runs: keep newest assigned per task,
--    mark older ones as error.
UPDATE task_runs tr
JOIN (
    SELECT id
    FROM (
        SELECT
            id,
            ROW_NUMBER() OVER (
                PARTITION BY task_id
                ORDER BY assigned_at DESC, id DESC
            ) AS rn
        FROM task_runs
        WHERE status = 'assigned'
    ) ranked
    WHERE ranked.rn > 1
) duplicates ON duplicates.id = tr.id
SET
    tr.status = 'error',
    tr.finished_at = UTC_TIMESTAMP(),
    tr.error_message = COALESCE(NULLIF(tr.error_message, ''), 'Reconciled duplicate assigned run by migration');

-- 2) Add uniqueness invariant for assigned runs using a functional index.
--    This avoids table rebuild issues seen with generated STORED column on some legacy schemas.
SET @sql := (
    SELECT IF(
        EXISTS(
            SELECT 1
            FROM information_schema.statistics
            WHERE table_schema = @schema_name
              AND table_name = 'task_runs'
              AND index_name = 'uq_task_runs_active_assigned_task'
        ),
        'SELECT ''uq_task_runs_active_assigned_task already exists''',
        'CREATE UNIQUE INDEX uq_task_runs_active_assigned_task
            ON task_runs ((CASE WHEN status = ''assigned'' THEN task_id ELSE NULL END))'
    )
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 3) Add supporting indexes for scheduler/dashboard queries.
SET @sql := (
    SELECT IF(
        EXISTS(
            SELECT 1
            FROM information_schema.statistics
            WHERE table_schema = @schema_name
              AND table_name = 'task_runs'
              AND index_name = 'ix_task_runs_status_task_id'
        ),
        'SELECT ''ix_task_runs_status_task_id already exists''',
        'CREATE INDEX ix_task_runs_status_task_id
            ON task_runs (status, task_id)'
    )
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql := (
    SELECT IF(
        EXISTS(
            SELECT 1
            FROM information_schema.statistics
            WHERE table_schema = @schema_name
              AND table_name = 'task_runs'
              AND index_name = 'ix_task_runs_assigned_at'
        ),
        'SELECT ''ix_task_runs_assigned_at already exists''',
        'CREATE INDEX ix_task_runs_assigned_at
            ON task_runs (assigned_at)'
    )
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 4) Verification checks.
SELECT task_id, COUNT(*) AS assigned_count
FROM task_runs
WHERE status = 'assigned'
GROUP BY task_id
HAVING COUNT(*) > 1;

SHOW INDEX FROM task_runs
WHERE Key_name IN (
    'uq_task_runs_active_assigned_task',
    'ix_task_runs_status_task_id',
    'ix_task_runs_assigned_at'
);

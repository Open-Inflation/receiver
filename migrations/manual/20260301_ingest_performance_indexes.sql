-- Add covering indexes for receiver ingestion and bridge polling under high write pressure.
-- MySQL does not support CREATE INDEX IF NOT EXISTS, so we guard via information_schema.

SET @schema_name := DATABASE();

SET @idx_exists := (
    SELECT COUNT(1)
    FROM information_schema.statistics
    WHERE table_schema = @schema_name
      AND table_name = 'task_runs'
      AND index_name = 'ix_task_runs_status_orchestrator_assigned_at'
);
SET @ddl := IF(
    @idx_exists > 0,
    'SELECT ''skip ix_task_runs_status_orchestrator_assigned_at''',
    'CREATE INDEX ix_task_runs_status_orchestrator_assigned_at ON task_runs (status, orchestrator_id, assigned_at)'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @idx_exists := (
    SELECT COUNT(1)
    FROM information_schema.statistics
    WHERE table_schema = @schema_name
      AND table_name = 'run_artifacts'
      AND index_name = 'ix_run_artifacts_parser_ingested_id'
);
SET @ddl := IF(
    @idx_exists > 0,
    'SELECT ''skip ix_run_artifacts_parser_ingested_id''',
    'CREATE INDEX ix_run_artifacts_parser_ingested_id ON run_artifacts (parser_name, ingested_at, id)'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @idx_exists := (
    SELECT COUNT(1)
    FROM information_schema.statistics
    WHERE table_schema = @schema_name
      AND table_name = 'run_artifact_products'
      AND index_name = 'ix_run_artifact_products_artifact_id_id'
);
SET @ddl := IF(
    @idx_exists > 0,
    'SELECT ''skip ix_run_artifact_products_artifact_id_id''',
    'CREATE INDEX ix_run_artifact_products_artifact_id_id ON run_artifact_products (artifact_id, id)'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

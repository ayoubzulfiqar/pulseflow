-- 007_create_heartbeat_expectations.down.sql
DROP INDEX IF EXISTS idx_heartbeat_enabled;
DROP INDEX IF EXISTS idx_heartbeat_dest;
DROP TABLE IF EXISTS heartbeat_expectations;

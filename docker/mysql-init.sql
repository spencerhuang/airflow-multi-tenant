-- MySQL initialization script for Control Plane database
-- This script runs automatically when MySQL container starts for the first time

-- Grant Debezium CDC permissions to control_plane user
-- These permissions are required for Debezium to capture change data
GRANT SELECT ON control_plane.* TO 'control_plane'@'%';
GRANT RELOAD ON *.* TO 'control_plane'@'%';
GRANT SHOW DATABASES ON *.* TO 'control_plane'@'%';
GRANT REPLICATION SLAVE ON *.* TO 'control_plane'@'%';
GRANT REPLICATION CLIENT ON *.* TO 'control_plane'@'%';

-- Grant FLUSH_TABLES for snapshot operations
GRANT FLUSH_TABLES ON *.* TO 'control_plane'@'%';

-- Apply privileges
FLUSH PRIVILEGES;

-- Enable binlog (required for CDC)
-- Note: This must also be set in my.cnf for persistent configuration
SET GLOBAL binlog_format = 'ROW';
SET GLOBAL binlog_row_image = 'FULL';

-- Log that initialization completed
SELECT 'MySQL initialization complete - Debezium CDC permissions granted' AS status;

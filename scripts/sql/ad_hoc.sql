DROP TABLE IF EXISTS staging.decoded_logs;

DROP SCHEMA IF EXISTS RAW CASCADE;


-- Check your current role and user
SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_ACCOUNT();

-- Check your current database and schema
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();

-- See what roles you have
SHOW GRANTS TO USER CURRENT_USER();

-- See what your role can do
SHOW GRANTS TO ROLE CURRENT_ROLE();

-- Check specific object permissions
SHOW GRANTS ON TABLE DB_T21.SC_RAW.LOGS_SAMPLE;

-- Check schema-level permissions
SHOW GRANTS TO ROLE CURRENT_ROLE() ON SCHEMA DB_T21.SC_RAW;

-- Check database permissions
SHOW GRANTS TO ROLE CURRENT_ROLE() ON DATABASE your_database;

-- Check warehouse permissions
SHOW GRANTS TO ROLE CURRENT_ROLE() ON WAREHOUSE your_warehouse_name;


SHOW GRANTS ON TABLE DB_T21.SC_RAW.LOGS_SAMPLE;
-- See your databases (you'll see yours and shared ones)
SHOW DATABASES;
SHOW SCHEMAS IN DATABASE DB_T21;
SHOW TABLES IN SCHEMA DB_T21.RAW

SELECT * FROM DB_T21.RAW.LOGS LIMIT 10;
-----------------------------------------------------------
-- Snowflake Setup Script for Insurance Services
-- This script creates roles, a warehouse, a database, and a table
-----------------------------------------------------------
USE WAREHOUSE COMPUTE_WH;
USE ROLE SYSADMIN;

-- Role Creation and Assignment 
SET MY_USER = CURRENT_USER();
CREATE OR REPLACE ROLE INSURED_ROLE;
GRANT ROLE INSURED_ROLE TO ROLE SYSADMIN;
GRANT ROLE INSURED_ROLE TO USER IDENTIFIER($MY_USER);

-- Granting necessary privileges to the role
GRANT EXECUTE TASK ON ACCOUNT TO ROLE INSURED_ROLE;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE INSURED_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE INSURED_ROLE;

-- Warehouse Creation
CREATE OR REPLACE WAREHOUSE INSURE_WH WITH 
    WAREHOUSE_SIZE = XSMALL
    AUTO_SUSPEND = 300
    AUTO_RESUME= TRUE
    COMMENT = 'Insurance Operations Compute';

GRANT OWNERSHIP ON WAREHOUSE INSURE_WH TO ROLE INSURED_ROLE;

-- Database and Schema Creation
CREATE OR REPLACE DATABASE INSURE_DB 
    COMMENT = 'Insurance Services Management Database';

GRANT OWNERSHIP ON DATABASE INSURE_DB TO ROLE INSURED_ROLE;

DROP SCHEMA IF EXISTS INSURE_DB.PUBLIC;

CREATE OR REPLACE SCHEMA RAW;

-- Switching to the new role, warehouse, and database
USE WAREHOUSE INSURE_WH;
USE ROLE INSURED_ROLE;
USE DATABASE INSURE_DB;

-- Table Creation for Claims Data
CREATE OR REPLACE TABLE INSURE_DB.RAW.CLAIMS_TABLE (
    CLAIM_ID STRING,                -- Keeping as STRING if alphanumeric
    POLICY_ID STRING,               -- Keeping as STRING for potential non-numeric formats
    CUSTOMER_ID STRING,             -- Keeping as STRING for consistency with customer identifiers
    CLAIM_AMOUNT NUMBER(18, 2),     -- Storing as numeric with 2 decimal places
    CLAIM_DATE DATE,                -- Using DATE for proper date operations
    INCIDENT_DATE DATE,             -- Using DATE for proper date operations
    CLAIM_TYPE STRING,              -- Typically categorical text
    STATUS STRING,                  -- Status like 'Approved', 'Pending', etc.
    ADJUSTER_NOTES STRING           -- Free-text notes
);

-- Create CSV file format
create file format insure_db.raw.COMMASEP_DBLQUOT_ONEHEADROW 
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ;

-- Create JSON file format
create file format insure_db.raw.json_file_format
    type = 'JSON'
    compression = 'AUTO'
    enable_octal = false
    allow_duplicate = true 
    strip_outer_array = true
    strip_null_values = true 
    ignore_utf8_errors = false
    ;

    
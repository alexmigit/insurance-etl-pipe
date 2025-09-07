-------------------------------------------------------------------
-- Snowflake Setup Script for Insurance Services (Best Practice)
-- Purpose: Creates roles, warehouse, database, schemas, tables, and file formats
-- Best Practices: Security, modularity, and maintainability
-------------------------------------------------------------------

-- Use admin role and warehouse for setup
USE WAREHOUSE COMPUTE_WH;
USE ROLE SYSADMIN;

-------------------------------------------------------------------
-- 1. Role Creation and Privileges
-------------------------------------------------------------------
-- Create a dedicated role for insurance operations
CREATE OR REPLACE ROLE INSURED_ROLE
  COMMENT = 'Role for Insurance Services Operations';

-- Grant role to admin and current user (for setup)
GRANT ROLE INSURED_ROLE TO ROLE SYSADMIN;
GRANT ROLE INSURED_ROLE TO USER IDENTIFIER(CURRENT_USER());

-- Grant only necessary privileges (least privilege principle)
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE INSURED_ROLE;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE INSURED_ROLE;
GRANT MONITOR EXECUTION ON ACCOUNT TO ROLE INSURED_ROLE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE INSURED_ROLE;

-------------------------------------------------------------------
-- 2. Warehouse Creation
-------------------------------------------------------------------
CREATE OR REPLACE WAREHOUSE INSURE_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 300
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Warehouse for Insurance Operations';

GRANT OPERATE, MONITOR ON WAREHOUSE INSURE_WH TO ROLE INSURED_ROLE;

-------------------------------------------------------------------
-- 3. Database and Schema Creation
-------------------------------------------------------------------
CREATE OR REPLACE DATABASE INSURE_DB
  COMMENT = 'Database for Insurance Services Management';

-- Create schemas for data organization
CREATE SCHEMA IF NOT EXISTS INSURE_DB.RAW
  COMMENT = 'Raw data landing zone';
CREATE SCHEMA IF NOT EXISTS INSURE_DB.STAGING
  COMMENT = 'Staging area for transformed data';
CREATE SCHEMA IF NOT EXISTS INSURE_DB.MART
  COMMENT = 'Data marts for business use';
CREATE SCHEMA IF NOT EXISTS INSURE_DB.ANALYTICS
  COMMENT = 'Analytics and reporting layer';

-- Drop public schema to enforce schema usage
DROP SCHEMA IF EXISTS INSURE_DB.PUBLIC;

-------------------------------------------------------------------
-- 4. Table Creation (Example: Claims Data)
-------------------------------------------------------------------
USE SCHEMA INSURE_DB.RAW;

CREATE OR REPLACE TABLE CLAIM (
    CLAIM_ID VARCHAR(12) NOT NULL COMMENT 'Surrogate key, text identifier',
    POLICY_ID VARCHAR(12) NOT NULL COMMENT 'Foreign key to policy',
    CUSTOMER_ID VARCHAR(12) NOT NULL COMMENT 'Foreign key to customer',
    CLAIM_AMOUNT NUMBER(12,2) COMMENT 'Claim amount, up to 999,999,999,999.99',
    CLAIM_DATE DATE COMMENT 'Claim filing date',
    INCIDENT_DATE DATE COMMENT 'Date incident occurred',
    CLAIM_TYPE VARCHAR(25) COMMENT 'Type of claim: AUTO, HOME, HEALTH, etc.',
    STATUS VARCHAR(25) COMMENT 'Status: OPEN, CLOSED, PENDING, etc.',
    ADJUSTER_NOTES VARCHAR(1000) COMMENT 'Free text notes',
    LOAD_TS TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP() COMMENT 'Ingestion timestamp',
    CONSTRAINT PK_CLAIM PRIMARY KEY (CLAIM_ID, POLICY_ID)
)
COMMENT = 'Table for storing insurance claims data';

-------------------------------------------------------------------
-- 5. File Formats
-------------------------------------------------------------------
-- CSV File Format
CREATE FILE FORMAT INSURE_DB.RAW.CSV_SKIPHEADER_DOUBLEQUOTE
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    EMPTY_FIELD_AS_NULL = TRUE
    NULL_IF = ('NULL', 'null')
    COMMENT = 'CSV format with header skipping and double quotes';

-- JSON File Format
CREATE FILE FORMAT INSURE_DB.RAW.JSON_FORMAT
    TYPE = 'JSON'
    COMPRESSION = 'AUTO'
    ENABLE_OCTAL = FALSE
    ALLOW_DUPLICATE = TRUE
    STRIP_OUTER_ARRAY = TRUE
    STRIP_NULL_VALUES = TRUE
    IGNORE_UTF8_ERRORS = FALSE
    COMMENT = 'JSON format with auto-compression and null stripping';

-------------------------------------------------------------------
-- 6. Data Quality Check (Example: Duplicate Check)
-------------------------------------------------------------------
-- Use a view or stored procedure for regular checks
CREATE VIEW INSURE_DB.ANALYTICS.DUPLICATE_CLAIMS AS
SELECT
    CLAIM_ID,
    POLICY_ID,
    COUNT(*) AS RECORD_COUNT
FROM
    INSURE_DB.RAW.CLAIM
GROUP BY
    CLAIM_ID,
    POLICY_ID
HAVING
    COUNT(*) > 1;

-------------------------------------------------------------------
-- 7. Switch Context for Further Operations
-------------------------------------------------------------------
USE WAREHOUSE INSURE_WH;
USE ROLE INSURED_ROLE;
USE DATABASE INSURE_DB;
USE SCHEMA RAW;

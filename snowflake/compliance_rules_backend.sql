-- ============================================================================
-- ONE ENGINE — Snowflake backend provisioning
-- Target role:      FOODBUY_AXIOM_COMPLIANCE_PROD
-- Target warehouse: COMPLIANCE_PROD_WH
-- Target database:  FOODBUY_MASALA_PROD
-- Target schema:    COMPLIANCE_LAB
--
-- This script is idempotent. It creates the eleven persistence tables required
-- by streamlit_app.py and seeds the bundled local-vendor reference list. The
-- Python application seeds the embedded DAF rule catalog.
-- ============================================================================

USE ROLE FOODBUY_AXIOM_COMPLIANCE_PROD;
USE SECONDARY ROLES ALL;
USE WAREHOUSE COMPLIANCE_PROD_WH;
USE DATABASE FOODBUY_MASALA_PROD;
USE SCHEMA COMPLIANCE_LAB;

-- Confirm the execution context before creating objects.
SELECT
    CURRENT_USER()      AS USER_NAME,
    CURRENT_ROLE()      AS ROLE_NAME,
    CURRENT_WAREHOUSE() AS WAREHOUSE_NAME,
    CURRENT_DATABASE()  AS DATABASE_NAME,
    CURRENT_SCHEMA()    AS SCHEMA_NAME;

-- --------------------------------------------------------------------------
-- 1. Workbook/batch metadata
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FOODBUY_MASALA_PROD.COMPLIANCE_LAB.COMPLIANCE_RULES_BATCHES (
    ID                VARCHAR       NOT NULL,
    NAME              VARCHAR       NOT NULL,
    SOURCE_KIND       VARCHAR,
    REPORTING_DATE    DATE,
    STATUS            VARCHAR       NOT NULL,
    ROW_COUNT         NUMBER(38, 0) NOT NULL,
    SOURCE_FILE_NAME  VARCHAR,
    SOURCE_SHEET_NAME VARCHAR,
    FILE_SHA256       VARCHAR,
    WARNINGS          VARIANT,
    METADATA          VARIANT,
    ARCHIVED          BOOLEAN       DEFAULT FALSE,
    CREATED_AT        TIMESTAMP_TZ  NOT NULL,
    UPDATED_AT        TIMESTAMP_TZ  NOT NULL,
    CONSTRAINT PK_COMPLIANCE_RULES_BATCHES PRIMARY KEY (ID)
)
COMMENT = 'ONE ENGINE workbook and batch metadata';

-- --------------------------------------------------------------------------
-- 2. Normalized workflow rows and current analyst state
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FOODBUY_MASALA_PROD.COMPLIANCE_LAB.COMPLIANCE_RULES_WORKFLOW_ROWS (
    ID                  VARCHAR      NOT NULL,
    BATCH_ID            VARCHAR      NOT NULL,
    SOURCE_ROW_NUMBER   NUMBER(38, 0),
    BUSINESS            VARCHAR,
    REQUEST_TYPE        VARCHAR,
    CASE_NUMBER         VARCHAR,
    VENDOR              VARCHAR,
    DIN                 VARCHAR,
    MIN                 VARCHAR,
    DESCRIPTION         VARCHAR,
    ACTION              VARCHAR,
    IF_IN_STOCK_ACTION  VARCHAR,
    AUDIT_ACTION        VARCHAR,
    BUYSMART_ACTION     VARCHAR,
    RULE_APPLIED        VARCHAR,
    NEEDS_REVIEW        BOOLEAN,
    VALIDATION_STATUS   VARCHAR,
    EXCLUDED            BOOLEAN,
    QUEUE_BUCKET        VARCHAR,
    OUTCOME_REPORTING   VARCHAR,
    STATUS              VARCHAR,
    UPDATED_AT          TIMESTAMP_TZ,
    ROW_JSON            VARIANT      NOT NULL,
    CONSTRAINT PK_COMPLIANCE_RULES_WORKFLOW_ROWS PRIMARY KEY (ID)
)
COMMENT = 'ONE ENGINE normalized workbook rows and current decisions';

-- Upgrade existing deployments created before Audit Action became part of the
-- atomic Product Request outcome.
ALTER TABLE FOODBUY_MASALA_PROD.COMPLIANCE_LAB.COMPLIANCE_RULES_WORKFLOW_ROWS
    ADD COLUMN IF NOT EXISTS AUDIT_ACTION VARCHAR;

-- --------------------------------------------------------------------------
-- 3. Bundled and user-authored rule definitions
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FOODBUY_MASALA_PROD.COMPLIANCE_LAB.COMPLIANCE_RULES_RULES (
    ID                 VARCHAR       NOT NULL,
    RULE_ID            VARCHAR       NOT NULL,
    NAME               VARCHAR       NOT NULL,
    RULE_GROUP         VARCHAR,
    STATUS             VARCHAR,
    AUTOMATION_LEVEL   VARCHAR,
    EXECUTION_PRIORITY NUMBER(38, 0),
    IS_BUNDLED         BOOLEAN,
    UPDATED_AT         TIMESTAMP_TZ,
    RULE_JSON          VARIANT       NOT NULL,
    CONSTRAINT PK_COMPLIANCE_RULES_RULES PRIMARY KEY (ID),
    CONSTRAINT UQ_COMPLIANCE_RULES_RULES_RULE_ID UNIQUE (RULE_ID)
)
COMMENT = 'ONE ENGINE executable rule catalog';

-- --------------------------------------------------------------------------
-- 4. Batch execution headers
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FOODBUY_MASALA_PROD.COMPLIANCE_LAB.COMPLIANCE_RULES_RUNS (
    ID                 VARCHAR       NOT NULL,
    BATCH_ID           VARCHAR       NOT NULL,
    MODE               VARCHAR,
    STATUS             VARCHAR,
    DRY_RUN            BOOLEAN,
    INPUT_ROW_COUNT    NUMBER(38, 0),
    SELECTED_ROW_COUNT NUMBER(38, 0),
    CHANGED_ROW_COUNT  NUMBER(38, 0),
    REVIEW_ROW_COUNT   NUMBER(38, 0),
    STARTED_AT         TIMESTAMP_TZ,
    COMPLETED_AT       TIMESTAMP_TZ,
    CREATED_AT         TIMESTAMP_TZ,
    RUN_JSON           VARIANT       NOT NULL,
    CONSTRAINT PK_COMPLIANCE_RULES_RUNS PRIMARY KEY (ID)
)
COMMENT = 'ONE ENGINE execution history';

-- --------------------------------------------------------------------------
-- 5. Per-row execution evidence and traces
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FOODBUY_MASALA_PROD.COMPLIANCE_LAB.COMPLIANCE_RULES_ROW_RESULTS (
    ID              VARCHAR      NOT NULL,
    RUN_ID          VARCHAR      NOT NULL,
    BATCH_ID        VARCHAR      NOT NULL,
    WORKFLOW_ROW_ID VARCHAR      NOT NULL,
    RULES_APPLIED   VARIANT,
    CREATED_AT      TIMESTAMP_TZ,
    RESULT_JSON     VARIANT      NOT NULL,
    CONSTRAINT PK_COMPLIANCE_RULES_ROW_RESULTS PRIMARY KEY (ID)
)
COMMENT = 'ONE ENGINE row-level results, applied rules, and traces';

-- --------------------------------------------------------------------------
-- 6. Auditable administrative and analyst events
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FOODBUY_MASALA_PROD.COMPLIANCE_LAB.COMPLIANCE_RULES_AUDIT_EVENTS (
    ID          VARCHAR      NOT NULL,
    ENTITY_TYPE VARCHAR      NOT NULL,
    ENTITY_ID   VARCHAR,
    BATCH_ID    VARCHAR,
    ACTION      VARCHAR      NOT NULL,
    USER_NAME   VARCHAR,
    CREATED_AT  TIMESTAMP_TZ NOT NULL,
    BEFORE_JSON VARIANT,
    AFTER_JSON  VARIANT,
    DETAILS     VARIANT,
    CONSTRAINT PK_COMPLIANCE_RULES_AUDIT_EVENTS PRIMARY KEY (ID)
)
COMMENT = 'ONE ENGINE audit trail';

-- --------------------------------------------------------------------------
-- 7. Runtime reference lists used by in_ref/not_in_ref predicates
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FOODBUY_MASALA_PROD.COMPLIANCE_LAB.COMPLIANCE_RULES_REFERENCE_LISTS (
    LIST_NAME  VARCHAR      NOT NULL,
    VALUE      VARCHAR      NOT NULL,
    ACTIVE     BOOLEAN      DEFAULT TRUE,
    NOTES      VARCHAR,
    UPDATED_AT TIMESTAMP_TZ NOT NULL,
    CONSTRAINT UQ_COMPLIANCE_RULES_REFERENCE_LISTS_VALUE UNIQUE (LIST_NAME, VALUE)
)
COMMENT = 'ONE ENGINE runtime reference-list values';

-- --------------------------------------------------------------------------
-- 8. Immutable workflow catalog versions
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FOODBUY_MASALA_PROD.COMPLIANCE_LAB.COMPLIANCE_RULES_CATALOG_VERSIONS (
    ID                  VARCHAR       NOT NULL,
    WORKFLOW_ID         VARCHAR       NOT NULL,
    VERSION_NUMBER      NUMBER(38, 0) NOT NULL,
    STATUS              VARCHAR       NOT NULL,
    DISTILLERY_RUN_ID   VARCHAR,
    PARENT_VERSION_ID   VARCHAR,
    CREATED_BY          VARCHAR,
    CREATED_AT          TIMESTAMP_TZ  NOT NULL,
    ACTIVATED_BY        VARCHAR,
    ACTIVATED_AT        TIMESTAMP_TZ,
    VERSION_JSON        VARIANT       NOT NULL,
    CONSTRAINT PK_COMPLIANCE_RULES_CATALOG_VERSIONS PRIMARY KEY (ID),
    CONSTRAINT UQ_COMPLIANCE_RULES_CATALOG_VERSION UNIQUE (WORKFLOW_ID, VERSION_NUMBER)
)
COMMENT = 'ONE ENGINE immutable workflow catalog version headers';

-- --------------------------------------------------------------------------
-- 9. Versioned rule definitions retained for activation and rollback
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FOODBUY_MASALA_PROD.COMPLIANCE_LAB.COMPLIANCE_RULES_CATALOG_VERSION_RULES (
    ID                  VARCHAR      NOT NULL,
    CATALOG_VERSION_ID  VARCHAR      NOT NULL,
    WORKFLOW_ID         VARCHAR      NOT NULL,
    RULE_ID             VARCHAR      NOT NULL,
    RULE_JSON           VARIANT      NOT NULL,
    CREATED_AT          TIMESTAMP_TZ NOT NULL,
    CONSTRAINT PK_COMPLIANCE_RULES_CATALOG_VERSION_RULES PRIMARY KEY (ID),
    CONSTRAINT UQ_COMPLIANCE_RULES_CATALOG_VERSION_RULE UNIQUE (CATALOG_VERSION_ID, RULE_ID)
)
COMMENT = 'ONE ENGINE immutable rules belonging to catalog versions';

-- --------------------------------------------------------------------------
-- 10. Persistent evidence rows not covered by governed literal filters
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FOODBUY_MASALA_PROD.COMPLIANCE_LAB.COMPLIANCE_RULES_DISTILLERY_GAPS (
    ID                  VARCHAR      NOT NULL,
    CATALOG_VERSION_ID  VARCHAR      NOT NULL,
    WORKFLOW_ID         VARCHAR      NOT NULL,
    SOURCE_GROUP        VARCHAR,
    PAIR_ID             VARCHAR,
    STATUS              VARCHAR      NOT NULL,
    RESOLUTION          VARCHAR,
    GAP_JSON            VARIANT      NOT NULL,
    CREATED_AT          TIMESTAMP_TZ NOT NULL,
    UPDATED_AT          TIMESTAMP_TZ NOT NULL,
    CONSTRAINT PK_COMPLIANCE_RULES_DISTILLERY_GAPS PRIMARY KEY (ID)
)
COMMENT = 'ONE ENGINE unresolved Distillery gaps and their evidence';

-- --------------------------------------------------------------------------
-- 11. Workflow-aware canonical outcome aliases
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS FOODBUY_MASALA_PROD.COMPLIANCE_LAB.COMPLIANCE_RULES_OUTCOME_ALIASES (
    WORKFLOW_ID         VARCHAR      NOT NULL,
    FIELD_NAME          VARCHAR      NOT NULL,
    RAW_VALUE_KEY       VARCHAR      NOT NULL,
    RAW_VALUE           VARCHAR,
    CANONICAL_VALUE     VARCHAR,
    STATUS              VARCHAR      NOT NULL,
    UPDATED_BY          VARCHAR,
    UPDATED_AT          TIMESTAMP_TZ NOT NULL,
    CONSTRAINT UQ_COMPLIANCE_RULES_OUTCOME_ALIAS UNIQUE (
        WORKFLOW_ID,
        FIELD_NAME,
        RAW_VALUE_KEY
    )
)
COMMENT = 'ONE ENGINE reviewed raw-to-canonical outcome mappings';

-- --------------------------------------------------------------------------
-- Seed the default local-vendor reference list without duplicating values.
-- --------------------------------------------------------------------------
MERGE INTO FOODBUY_MASALA_PROD.COMPLIANCE_LAB.COMPLIANCE_RULES_REFERENCE_LISTS AS TARGET
USING (
    SELECT
        COLUMN1::VARCHAR AS LIST_NAME,
        COLUMN2::VARCHAR AS VALUE,
        COLUMN3::VARCHAR AS NOTES
    FROM VALUES
        ('local_vendors', 'Baldor',              'Bundled default reference value'),
        ('local_vendors', 'Network',             'Bundled default reference value'),
        ('local_vendors', 'UNFI',                'Bundled default reference value'),
        ('local_vendors', 'Vesta',               'Bundled default reference value'),
        ('local_vendors', 'Vistar Vending',      'Bundled default reference value'),
        ('local_vendors', 'The Chefs Warehouse', 'Bundled default reference value'),
        ('local_vendors', 'Gourmet',             'Bundled default reference value')
) AS SOURCE
    ON  TARGET.LIST_NAME = SOURCE.LIST_NAME
    AND TARGET.VALUE     = SOURCE.VALUE
WHEN MATCHED THEN UPDATE SET
    ACTIVE     = TRUE,
    NOTES      = SOURCE.NOTES,
    UPDATED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    LIST_NAME,
    VALUE,
    ACTIVE,
    NOTES,
    UPDATED_AT
) VALUES (
    SOURCE.LIST_NAME,
    SOURCE.VALUE,
    TRUE,
    SOURCE.NOTES,
    CURRENT_TIMESTAMP()
);

-- --------------------------------------------------------------------------
-- Verification: eleven rows should be returned, all in COMPLIANCE_LAB.
-- --------------------------------------------------------------------------
SELECT
    TABLE_CATALOG,
    TABLE_SCHEMA,
    TABLE_NAME,
    TABLE_TYPE,
    ROW_COUNT,
    CREATED,
    LAST_ALTERED
FROM FOODBUY_MASALA_PROD.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'COMPLIANCE_LAB'
  AND TABLE_NAME IN (
      'COMPLIANCE_RULES_BATCHES',
      'COMPLIANCE_RULES_WORKFLOW_ROWS',
      'COMPLIANCE_RULES_RULES',
      'COMPLIANCE_RULES_RUNS',
      'COMPLIANCE_RULES_ROW_RESULTS',
      'COMPLIANCE_RULES_AUDIT_EVENTS',
      'COMPLIANCE_RULES_REFERENCE_LISTS',
      'COMPLIANCE_RULES_CATALOG_VERSIONS',
      'COMPLIANCE_RULES_CATALOG_VERSION_RULES',
      'COMPLIANCE_RULES_DISTILLERY_GAPS',
      'COMPLIANCE_RULES_OUTCOME_ALIASES'
  )
ORDER BY TABLE_NAME;

-- Readiness check. Every statement should return a count without error.
SELECT 'COMPLIANCE_RULES_BATCHES'        AS TABLE_NAME, COUNT(*) AS ROW_COUNT
FROM FOODBUY_MASALA_PROD.COMPLIANCE_LAB.COMPLIANCE_RULES_BATCHES
UNION ALL
SELECT 'COMPLIANCE_RULES_WORKFLOW_ROWS', COUNT(*)
FROM FOODBUY_MASALA_PROD.COMPLIANCE_LAB.COMPLIANCE_RULES_WORKFLOW_ROWS
UNION ALL
SELECT 'COMPLIANCE_RULES_RULES',         COUNT(*)
FROM FOODBUY_MASALA_PROD.COMPLIANCE_LAB.COMPLIANCE_RULES_RULES
UNION ALL
SELECT 'COMPLIANCE_RULES_RUNS',          COUNT(*)
FROM FOODBUY_MASALA_PROD.COMPLIANCE_LAB.COMPLIANCE_RULES_RUNS
UNION ALL
SELECT 'COMPLIANCE_RULES_ROW_RESULTS',   COUNT(*)
FROM FOODBUY_MASALA_PROD.COMPLIANCE_LAB.COMPLIANCE_RULES_ROW_RESULTS
UNION ALL
SELECT 'COMPLIANCE_RULES_AUDIT_EVENTS',  COUNT(*)
FROM FOODBUY_MASALA_PROD.COMPLIANCE_LAB.COMPLIANCE_RULES_AUDIT_EVENTS
UNION ALL
SELECT 'COMPLIANCE_RULES_REFERENCE_LISTS', COUNT(*)
FROM FOODBUY_MASALA_PROD.COMPLIANCE_LAB.COMPLIANCE_RULES_REFERENCE_LISTS
UNION ALL
SELECT 'COMPLIANCE_RULES_CATALOG_VERSIONS', COUNT(*)
FROM FOODBUY_MASALA_PROD.COMPLIANCE_LAB.COMPLIANCE_RULES_CATALOG_VERSIONS
UNION ALL
SELECT 'COMPLIANCE_RULES_CATALOG_VERSION_RULES', COUNT(*)
FROM FOODBUY_MASALA_PROD.COMPLIANCE_LAB.COMPLIANCE_RULES_CATALOG_VERSION_RULES
UNION ALL
SELECT 'COMPLIANCE_RULES_DISTILLERY_GAPS', COUNT(*)
FROM FOODBUY_MASALA_PROD.COMPLIANCE_LAB.COMPLIANCE_RULES_DISTILLERY_GAPS
UNION ALL
SELECT 'COMPLIANCE_RULES_OUTCOME_ALIASES', COUNT(*)
FROM FOODBUY_MASALA_PROD.COMPLIANCE_LAB.COMPLIANCE_RULES_OUTCOME_ALIASES
ORDER BY TABLE_NAME;

-- ============================================================================
-- Optional administrator grants
--
-- Do not run this block when FOODBUY_AXIOM_COMPLIANCE_PROD executed the CREATE
-- TABLE statements above; that role owns the new objects already. If another
-- administrative role creates the objects, run the following grants under a
-- role that owns the warehouse/database/schema/tables or has MANAGE GRANTS.
-- The READ SESSION grant requires ACCOUNTADMIN and is needed by warehouse-
-- runtime Streamlit apps that use Snowflake context functions such as
-- CURRENT_USER():
--
-- USE ROLE ACCOUNTADMIN;
-- GRANT READ SESSION ON ACCOUNT
--   TO ROLE FOODBUY_AXIOM_COMPLIANCE_PROD;
--
-- USE ROLE SECURITYADMIN;
-- GRANT USAGE ON WAREHOUSE COMPLIANCE_PROD_WH
--   TO ROLE FOODBUY_AXIOM_COMPLIANCE_PROD;
-- GRANT USAGE ON DATABASE FOODBUY_MASALA_PROD
--   TO ROLE FOODBUY_AXIOM_COMPLIANCE_PROD;
-- GRANT USAGE ON SCHEMA FOODBUY_MASALA_PROD.COMPLIANCE_LAB
--   TO ROLE FOODBUY_AXIOM_COMPLIANCE_PROD;
-- GRANT SELECT, INSERT, UPDATE, DELETE
--   ON ALL TABLES IN SCHEMA FOODBUY_MASALA_PROD.COMPLIANCE_LAB
--   TO ROLE FOODBUY_AXIOM_COMPLIANCE_PROD;
-- ============================================================================

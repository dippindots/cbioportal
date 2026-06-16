-- ============================================================
-- Setup: resource_data table + resource_data_unified view
-- for testing the resource-table feature WITHOUT a full data
-- migration (no pipelines team involvement needed).
--
-- Run this against your ClickHouse Cloud database ONCE.
-- Existing data in resource_sample / resource_patient / resource_study
-- remains untouched and will be visible through the view immediately.
--
-- Usage:
--   clickhouse-client \
--     --host  <host>.clickhouse.cloud \
--     --port  9440 --secure \
--     --user  default \
--     --password <password> \
--     --database cbioportal \
--     --multiquery < setup_resource_data_testing.sql
--
-- Future migration path (when pipelines team is ready):
--   Run migrate_resource_data.sql — it backfills all legacy rows into
--   resource_data and replaces this view with a simple SELECT FROM
--   resource_data (no more UNION ALL).
-- ============================================================

-- Step 1: Create the unified resource_data table (starts empty).
--         New data imported via the importer goes here directly.
CREATE TABLE IF NOT EXISTS resource_data
(
    `RESOURCE_DATA_ID` Int32,
    `RESOURCE_ID`      String,
    `CANCER_STUDY_ID`  Int32,
    `ENTITY_TYPE`      String,           -- 'SAMPLE' | 'PATIENT' | 'STUDY'
    `PATIENT_ID`       Nullable(String),
    `SAMPLE_ID`        Nullable(String),
    `URL`              String,
    `DISPLAY_NAME`     Nullable(String),
    `TYPE`             Nullable(String),
    `METADATA`         Nullable(String), -- JSON object string
    `PRIORITY`         Int32 DEFAULT 0
) ENGINE = MergeTree()
  ORDER BY (CANCER_STUDY_ID, RESOURCE_ID, RESOURCE_DATA_ID);

-- Step 2: Create the unified view that exposes ALL resource data:
--   - New rows written by the importer → from resource_data
--   - Legacy rows that haven't been migrated yet → joined from the three legacy tables
--
-- The backend (ResourceDataMapper.xml) queries resource_data_unified so that
-- it works correctly regardless of which migration phase you are in.
CREATE OR REPLACE VIEW resource_data_unified AS
    -- New data written by the importer
    SELECT
        RESOURCE_DATA_ID,
        RESOURCE_ID,
        CANCER_STUDY_ID,
        ENTITY_TYPE,
        PATIENT_ID,
        SAMPLE_ID,
        URL,
        DISPLAY_NAME,
        TYPE,
        METADATA,
        PRIORITY
    FROM resource_data

    UNION ALL

    -- Legacy SAMPLE-level resources
    SELECT
        toInt32(rowNumberInAllBlocks()) + 1000000 AS RESOURCE_DATA_ID,
        rs.resource_id  AS RESOURCE_ID,
        toInt32(cs.cancer_study_id) AS CANCER_STUDY_ID,
        'SAMPLE'        AS ENTITY_TYPE,
        p.stable_id     AS PATIENT_ID,
        s.stable_id     AS SAMPLE_ID,
        rs.url          AS URL,
        NULL            AS DISPLAY_NAME,
        NULL            AS TYPE,
        NULL            AS METADATA,
        0               AS PRIORITY
    FROM resource_sample rs
    INNER JOIN sample       s  ON rs.internal_id   = s.internal_id
    INNER JOIN patient      p  ON s.patient_id     = p.internal_id
    INNER JOIN cancer_study cs ON p.cancer_study_id = cs.cancer_study_id

    UNION ALL

    -- Legacy PATIENT-level resources
    SELECT
        toInt32(rowNumberInAllBlocks()) + 2000000 AS RESOURCE_DATA_ID,
        rp.resource_id  AS RESOURCE_ID,
        toInt32(cs.cancer_study_id) AS CANCER_STUDY_ID,
        'PATIENT'       AS ENTITY_TYPE,
        pt.stable_id    AS PATIENT_ID,
        NULL            AS SAMPLE_ID,
        rp.url          AS URL,
        NULL            AS DISPLAY_NAME,
        NULL            AS TYPE,
        NULL            AS METADATA,
        0               AS PRIORITY
    FROM resource_patient rp
    INNER JOIN patient      pt ON rp.internal_id     = pt.internal_id
    INNER JOIN cancer_study cs ON pt.cancer_study_id = cs.cancer_study_id

    UNION ALL

    -- Legacy STUDY-level resources (internal_id IS the cancer_study_id in resource_study)
    SELECT
        toInt32(rowNumberInAllBlocks()) + 3000000 AS RESOURCE_DATA_ID,
        rst.resource_id      AS RESOURCE_ID,
        toInt32(rst.internal_id) AS CANCER_STUDY_ID,
        'STUDY'              AS ENTITY_TYPE,
        NULL                 AS PATIENT_ID,
        NULL                 AS SAMPLE_ID,
        rst.url              AS URL,
        NULL                 AS DISPLAY_NAME,
        NULL                 AS TYPE,
        NULL                 AS METADATA,
        0                    AS PRIORITY
    FROM resource_study rst;

-- Step 3: Sanity check — should show counts from all legacy tables
SELECT
    'resource_sample'        AS source, count() AS rows FROM resource_sample
UNION ALL SELECT 'resource_patient',   count() FROM resource_patient
UNION ALL SELECT 'resource_study',     count() FROM resource_study
UNION ALL SELECT 'resource_data',      count() FROM resource_data
UNION ALL SELECT 'resource_data_unified (total)', count() FROM resource_data_unified;

-- Creating initial staging delta table
CONVERT TO DELTA parquet.`s3://fenz-datalake`;

CREATE TABLE RESULTS_BRONZE
USING DELTA
LOCATION 's3://fenz-datalake';

-- start creating the fact tables
CREATE OR REPLACE TABLE COMPETITIONS_SILVER
USING DELTA
PARTITIONED BY (comp_year, category)
AS
WITH parsed_comp_info AS (
  SELECT
    CAST(cmp.cmp_id as int)       as comp_id
    ,cmp.name                     as comp_name
    ,CAST(cmp.comp_start as date) as start_date
    ,CAST(cmp.comp_end as date)   as end_date
    ,cmp.host                     as host
    ,cmp.category                 as category
    -- first_event will be the key to join to RESULT
    ,CAST(firstevent as smallint) as first_event
    -- leave as a string as it is merely for display dates. For analysis, use start/end_date
    ,cmp.date                     as comp_display_date
    ,YEAR(CAST(cmp.comp_start as date)) as comp_year
  FROM results_bronze
)
-- deduplicate primary key
SELECT
  comp_id
  ,comp_name
  ,start_date
  ,end_date
  ,host
  ,category
  ,first_event
  ,comp_display_date
  ,comp_year
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY comp_id ORDER BY comp_id) AS rn
  FROM parsed_comp_info
)
WHERE rn = 1
;
OPTIMIZE COMPETITIONS_SILVER ZORDER BY (comp_id);

ALTER TABLE COMPETITIONS_SILVER ALTER COLUMN comp_id SET NOT NULL;
ALTER TABLE COMPETITIONS_SILVER ADD PRIMARY KEY (comp_id);



CONVERT TO DELTA parquet.`s3://fenz-datalake`;

CREATE TABLE RESULTS_BRONZE
USING DELTA
LOCATION 's3://fenz-datalake';

CREATE OR REPLACE TABLE COMPETITION_SILVER AS
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
FROM results_bronze;
OPTIMIZE COMPETITION_SILVER ZORDER BY (comp_id, start_date, end_date);

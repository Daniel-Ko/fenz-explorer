REATE OR REPLACE TABLE events_silver
USING DELTA
PARTITIONED BY (short_desc, category)
AS
SELECT
   CONCAT(cmp_id, '_', event_id, '_', short_desc) AS event_key
  ,CAST(event_id AS INT)                          AS event_id
  ,CAST(cmp_id AS INT)                            AS comp_id
  ,CAST(event AS INT)                             AS event_num 
  ,category                                       AS category 
  ,short_desc                                     AS short_desc 
  ,long_desc                                      AS long_desc
FROM (
  SELECT
    cmp.cmp_id,
    event_struct.*,
    -- Remove duplicates from primary key 'event_key'
    ROW_NUMBER() OVER (
      PARTITION BY CONCAT(event_struct.event_id, '_', event_struct.short_desc)
      ORDER BY event_struct.event_id
    ) AS rn
  FROM results_bronze
  LATERAL VIEW explode(availEvents) AS event_struct
  )
  WHERE rn = 1
;
OPTIMIZE EVENTS_SILVER ZORDER BY (event_id);

ALTER TABLE EVENTS_SILVER ALTER COLUMN event_key SET NOT NULL;
ALTER TABLE EVENTS_SILVER ADD PRIMARY KEY (event_key);

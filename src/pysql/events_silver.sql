CREATE OR REPLACE TABLE events_silver
USING DELTA
PARTITIONED BY (short_desc, category)
AS
SELECT 
   event_key
  ,event_id
  ,event_num
  ,category
  ,short_desc
  ,long_desc
FROM (
  SELECT
    -- create composite key as event_id is denormalised thanks to the explode()
     CONCAT(event_struct.event_id, '_', event_struct.short_desc) AS event_key
    ,event_struct.event_id   as event_id
    ,event_struct.event      as event_num 
    ,event_struct.category   as category 
    ,event_struct.short_desc as short_desc 
    ,event_struct.long_desc  as long_desc
    -- ensure no duplicates for our primary key
    ,ROW_NUMBER() OVER (PARTITION BY CONCAT(event_struct.event_id, '_', event_struct.short_desc) ORDER BY event_struct.event_id) AS rn
  FROM (
    -- explode flattens the nested structure so we can identify each event
    -- separately
    SELECT explode(availEvents) as event_struct
    FROM results_bronze
  )
)
WHERE rn = 1
;
OPTIMIZE EVENTS_SILVER ZORDER BY (event_id);

ALTER TABLE EVENTS_SILVER ALTER COLUMN event_key SET NOT NULL;
ALTER TABLE EVENTS_SILVER ADD PRIMARY KEY (event_key);

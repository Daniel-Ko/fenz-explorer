CREATE OR REPLACE TABLE RESULTS_SILVER 
USING DELTA
AS
WITH json_friendly_events AS (
  SELECT 
    cmp.cmp_id,
    regexp_replace(
      regexp_replace(
        regexp_replace(events, 
          '\'detailedResults\': ?True', '\'detailedResults\': true'),
        '\'detailedResults\': ?False', '\'detailedResults\': false'),
      'None', 'null'
    ) AS events_cleaned
  FROM results_bronze
),
exploded_results AS (
  SELECT
    cmp_id,
    event_number,
    event_details
  FROM (
    SELECT
      cmp_id,
      explode(
        from_json(
          events_cleaned,
          'MAP<STRING, STRUCT<
            detailedResults:BOOLEAN,
            drawPouleRound: STRUCT<
              poules: MAP<STRING, MAP<STRING, STRUCT<
                matches: MAP<STRING, STRING>,
                name: STRING
              >>>
            >,
            drawPouleResults: MAP<STRING, STRUCT<
              name: STRING,
              club: STRING,
              poule: STRING,
              poule_pos: STRING,
              initial: STRING,
              eligable: STRING
            >>,
            drawTableaux: MAP<STRING, STRUCT<
              tableau_data: ARRAY<STRUCT<
                name: STRING,
                points_for: STRING,
                points_against: STRING,
                code: STRING,
                pos: INT
              >>,
              tab_size: INT
            >>,
            drawTournResults:ARRAY<STRUCT<
              place:STRING,
              uid:STRING,
              name:STRING,
              region:STRING,
              club:STRING,
              country:STRING
            >>
          >>'
        )
      ) AS (event_number, event_details)
    FROM json_friendly_events
  )
)

SELECT
  e.event_key
  ,e.event_id
  ,cast(r.cmp_id as int) AS comp_id
  ,cast(r.event_number as int)
  ,r.event_details.detailedResults AS detailedResults
  ,r.event_details.drawPouleRound AS drawPouleRound
  ,r.event_details.drawPouleResults AS drawPouleResults
  ,r.event_details.drawTableaux AS drawTableaux
  ,r.event_details.drawTournResults AS drawTournResults
FROM exploded_results r
INNER JOIN events_silver e
  ON r.event_number = e.event_num
  AND cast(r.cmp_id as int) = e.comp_id
;
OPTIMIZE RESULTS_SILVER ZORDER BY (event_key);

ALTER TABLE RESULTS_SILVER ALTER COLUMN event_key SET NOT NULL;
ALTER TABLE RESULTS_SILVER ADD PRIMARY KEY (event_key);

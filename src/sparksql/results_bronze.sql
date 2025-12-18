 /* Creating initial staging master table. This is the base for
  * COMPETITIONS_SILVER
  * RESULTS_SILVER
  * EVENTS_SILVER
  */
CONVERT TO DELTA parquet.`s3://fenz-datalake`;

CREATE TABLE RESULTS_BRONZE
USING DELTA
LOCATION 's3://fenz-datalake';

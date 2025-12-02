CREATE OR REPLACE PIPE LOGISTICS_SNOWPIPE
  AUTO_INGEST = TRUE
AS
COPY INTO CURATED_LOGISTICS_TABLE
FROM (
  SELECT
    $1:truck_id::STRING,
    $1:timestamp::STRING,
    $1:location::VARIANT,
    $1:fuel_level::FLOAT,
    $1:temperature::FLOAT,
    $1:delivery_status::STRING,
    TO_TIMESTAMP_NTZ($1:processed_at::STRING) AS processed_at,
    $1:ge_validation_status::STRING,
    $1:data_quality_flag::STRING
    FROM @GREENTECH_LOGISTICS_STAGE_PIPE/curated/
        (FILE_FORMAT => parquet_format,
         PATTERN => '.*\\.parquet')
)
ON_ERROR = 'CONTINUE';
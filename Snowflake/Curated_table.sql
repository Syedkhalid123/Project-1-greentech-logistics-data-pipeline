
CREATE OR REPLACE TABLE CURATED_LOGISTICS_TABLE (
  truck_id STRING,
  timestamp STRING,
  location VARIANT,
  fuel_level FLOAT,
  temperature FLOAT,
  delivery_status STRING,
  processed_at TIMESTAMP_NTZ,
  ge_validation_status STRING,
  data_quality_flag STRING
);
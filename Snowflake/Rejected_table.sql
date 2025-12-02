CREATE OR REPLACE TABLE REJECTED_LOGISTICS_TABLE (
  truck_id STRING,
  timestamp STRING,
  location VARIANT,
  fuel_level FLOAT,
  temperature FLOAT,
  delivery_status STRING,
  processed_at TIMESTAMP_NTZ,
  rejection_reasons ARRAY
);
CREATE OR REPLACE STAGE FIRST_DB.FIRST_SCHEMA.GREENTECH_LOGISTICS_STAGE_PIPE
URL = 's3://first-project-greentech-logistics-datalake/'
STORAGE_INTEGRATION = logistics_s3_storage_integration
file_format = parquet_format
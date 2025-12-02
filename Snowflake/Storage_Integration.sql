CREATE OR REPLACE STORAGE INTEGRATION logistics_s3_storage_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::521818210110:role/Data_loading_logistics'
STORAGE_ALLOWED_LOCATIONS = ('s3://first-project-greentech-logistics-datalake/');

desc integration logistics_s3_storage_integration;
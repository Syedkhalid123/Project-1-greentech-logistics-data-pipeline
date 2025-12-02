"""
GreenTech Logistics Data Lake Pipeline (with Great Expectations & Governance)
Author: [Your Name]
Phases: Raw → Validated → Great Expectations → Rejected → Curated → Governance → Analytics
Tech: AWS Glue + PySpark + Delta Lake + Great Expectations
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_timestamp, from_json,
    explode, from_utc_timestamp, when, isnan, expr
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, ArrayType
)

# ------------------------- Spark Session -------------------------
spark = SparkSession.builder \
    .appName("GreenTech_Logistics_DataLake_Pipeline") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# ------------------------- S3 Paths -------------------------
RAW_PATH = "s3://first-project-greentech-logistics-datalake/raw/"
CURATED_PATH = "s3://first-project-greentech-logistics-datalake/curated/"
REJECTED_PATH = "s3://first-project-greentech-logistics-datalake/rejected/"
VALIDATED_PATH = "s3://first-project-greentech-logistics-datalake/validated/"

# ------------------------- Phase 1: Read RAW JSON -------------------------
raw_df = spark.read.text(RAW_PATH)
array_df = raw_df.select(from_json(col("value"), ArrayType(StringType())).alias("json_array"))
exploded_df = array_df.select(explode(col("json_array")).alias("json_string"))

# ------------------------- Schema -------------------------
inner_schema = StructType([
    StructField("truck_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("location", StructType([
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True)
    ]), True),
    StructField("fuel_level", DoubleType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("delivery_status", StringType(), True)
])

df_parsed = exploded_df.withColumn("parsed", from_json(col("json_string"), inner_schema)).select("parsed.*")

# ------------------------- Phase 2: Clean & Validate -------------------------
df_clean = df_parsed \
    .withColumn("fuel_level", when(isnan(col("fuel_level")) | (col("fuel_level").isNull()), None).otherwise(col("fuel_level"))) \
    .withColumn("temperature", when(isnan(col("temperature")) | (col("temperature").isNull()), None).otherwise(col("temperature")))

# Validation rules
validated = df_clean.withColumn("truck_id_present", col("truck_id").isNotNull()) \
    .withColumn("fuel_level_range", col("fuel_level").between(0, 100)) \
    .withColumn("temperature_range", col("temperature").between(-10, 60)) \
    .withColumn("delivery_status_ok", col("delivery_status").isin("in_transit", "delivered", "delayed")) \
    .withColumn("lat_present", col("location").isNotNull() & col("location.lat").isNotNull()) \
    .withColumn("lon_present", col("location").isNotNull() & col("location.lon").isNotNull())

validated = validated.withColumn(
    "is_valid",
    col("truck_id_present") & col("fuel_level_range") & col("temperature_range") &
    col("delivery_status_ok") & col("lat_present") & col("lon_present")
)

validated = validated.withColumn(
    "rejection_reasons",
    expr(
        "array_remove(array("
        "case when not truck_id_present then 'missing_truck_id' else null end, "
        "case when not fuel_level_range then 'fuel_level_out_of_range' else null end, "
        "case when not temperature_range then 'temperature_out_of_range' else null end, "
        "case when not delivery_status_ok then 'invalid_delivery_status' else null end, "
        "case when not lat_present then 'missing_lat' else null end, "
        "case when not lon_present then 'missing_lon' else null end"
        "), null)"
    )
).withColumn("processed_at", from_utc_timestamp(current_timestamp(), "Asia/Kolkata"))

# ------------------------- Phase 3: Split Curated & Rejected -------------------------
curated_df = validated.filter(col("is_valid") == True) \
    .drop("truck_id_present", "fuel_level_range", "temperature_range", "delivery_status_ok",
          "lat_present", "lon_present", "is_valid", "rejection_reasons") \
    .withColumn("data_quality_flag", lit("VALID")) \
    .withColumn("ge_validation_status", lit("PENDING"))

rejected_df = validated.filter(col("is_valid") == False) \
    .withColumn("data_quality_flag", lit("REJECTED")) \
    .select("truck_id", "timestamp", "location", "fuel_level", "temperature", "delivery_status",
            "processed_at", "rejection_reasons")

print(f"✅ Curated rows: {curated_df.count()} | ❌ Rejected rows: {rejected_df.count()}")

# ------------------------- Phase 4: Great Expectations -------------------------
try:
    import great_expectations as ge
    from great_expectations.dataset import PandasDataset

    sample_df = curated_df.limit(500).toPandas()
    ge_df = PandasDataset(sample_df)

    ge_df.expect_column_values_to_not_be_null("truck_id")
    ge_df.expect_column_values_to_be_between("fuel_level", 0, 100)
    ge_df.expect_column_values_to_be_between("temperature", -10, 60)
    ge_df.expect_column_values_to_be_in_set("delivery_status", ["in_transit", "delivered", "delayed"])

    ge_status = "PASSED" if ge_df.validate()["success"] else "FAILED"
    curated_df = curated_df.withColumn("ge_validation_status", lit(ge_status))

    if ge_status != "PASSED":
        failed_from_ge = curated_df.withColumn("rejection_reasons", expr("array('ge_failed')")) \
            .withColumn("data_quality_flag", lit("REJECTED_GE"))
        rejected_df = rejected_df.unionByName(failed_from_ge.select(rejected_df.columns), allowMissingColumns=True)
        curated_df = curated_df.limit(0)

except Exception as e:
    print(f"⚠️ Great Expectations skipped or failed: {e}")
    curated_df = curated_df.withColumn("ge_validation_status", lit("SKIPPED"))

# ------------------------- Phase 5: Write Curated & Rejected -------------------------
if curated_df.count() > 0:
    curated_df.write.format("delta").option("mergeSchema", "true").mode("overwrite").save(CURATED_PATH)

if rejected_df.count() > 0:
    rejected_df.write.format("delta").option("mergeSchema", "true").mode("append").save(REJECTED_PATH)

# ------------------------- Phase 6: Data Lineage / Governance -------------------------
lineage_data = [
    ("raw", exploded_df.count()),
    ("validated_total", validated.count()),
    ("curated", curated_df.count()),
    ("rejected", rejected_df.count())
]

df_lineage = spark.createDataFrame(lineage_data, ["layer", "record_count"]) \
    .withColumn("pipeline_name", lit("green_tech_raw_to_curated_job")) \
    .withColumn("run_timestamp", from_utc_timestamp(current_timestamp(), "Asia/Kolkata"))

df_lineage.write.format("delta").option("mergeSchema", "true").mode("append").save(VALIDATED_PATH)

print("✅ Production-ready Glue job with Governance & GE completed successfully")

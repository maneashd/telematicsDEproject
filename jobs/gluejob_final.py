import sys
from pyspark.sql.types import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, input_file_name
from awsglue.context import GlueContext
from awsglue.job import Job

# AWS Glue Job arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Set paths and catalog
warehouse_path = 's3://spark-straming-data02/iceberg_data/'
catalog_nm = "awsdatacatalog"
database_op = "telematics_db"
table_op = "vehicle_schema_iceb"

# Initialize Spark session with Iceberg configurations
spark = SparkSession.builder \
    .config(f"spark.sql.catalog.{catalog_nm}", "org.apache.iceberg.spark.SparkCatalog") \
    .config(f"spark.sql.catalog.{catalog_nm}.warehouse", warehouse_path) \
    .config(f"spark.sql.catalog.{catalog_nm}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config(f"spark.sql.catalog.{catalog_nm}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.iceberg.handle-timestamp-without-timezone", "true") \
    .getOrCreate()


# Read all files in vehicle_data folder efficiently
vehicle_raw_df = spark.read \
    .option("mode", "PERMISSIVE") \
    .option("inferSchema", "true") \
    .json("s3://spark-straming-data02/data/vehicle_data/part-*.json") \
    .withColumn("file_name", input_file_name())

# Debug: Check data before transformation
print("Vehicle DataFrame Schema Before Transformation:")
vehicle_raw_df.printSchema()
print(f"Vehicle DataFrame count before transformation: {vehicle_raw_df.count()}")
vehicle_raw_df.show(5, truncate=False)

# Transform columns to desired types
vehicle_raw_df = vehicle_raw_df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")) \
                              .withColumn("year", col("year").cast(IntegerType()))

# Debug: Check data after transformation
print("Vehicle DataFrame Schema After Transformation:")
vehicle_raw_df.printSchema()
print(f"Vehicle DataFrame count after transformation: {vehicle_raw_df.count()}")
vehicle_raw_df.show(5, truncate=False)

# Create database if not exists
spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_nm}.{database_op}")

# Write to Iceberg table with partitioning
vehicle_raw_df.write \
    .format("iceberg") \
    .mode("overwrite") \
    .option("write.format.default", "parquet") \
    .saveAsTable(f"{catalog_nm}.{database_op}.{table_op}")

# Verify Iceberg table
print("Verifying Iceberg table contents:")
spark.sql(f"SELECT * FROM {catalog_nm}.{database_op}.{table_op}").show(5, truncate=False)

job.commit()




{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::123273454166:user/ld5z0000-s"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "<your_external_id>"
        }
      }
    }
  ]
}
import os
import logging
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def get_spark_session(app_name="App"):
    logging.info(f"starting spark session (Delta Lake): {app_name}")

    MINIO_URL = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
    ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "admin")
    SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "password123")

    builder = SparkSession.builder.appName(app_name) \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.530") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_URL) \
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.memory", "1g")

    # extra config for Delta
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    
    return spark
import logging
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pyspark.sql.functions import col, to_timestamp, udf
from pyspark.sql.types import StringType
from scripts.spark_utils import get_spark_session 

logging.basicConfig(level=logging.INFO)

def clean_html(text):
    if not text: return ""
    import re
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

clean_html_udf = udf(clean_html, StringType())

def run_bronze_to_silver():
    spark = get_spark_session("BronzeToSilver_ETL")

    try:
        # Extract
        bronze_path = "s3a://bronze/*/*/*.json"
        logging.info(f"reading: {bronze_path}")
        
        try:
            df_raw = spark.read.option("multiline", "true").json(bronze_path)
        except Exception:
            logging.warning("didnt read any files (empty bucket?)")
            return

        if df_raw.rdd.isEmpty():
            logging.warning("Dataframe empty")
            return

        # Transform
        logging.info("transforming data")
        if "_id" not in df_raw.columns:
            logging.error("JSON without '_id'")
            raise ValueError("wrong data schema Bronze")

        df_silver = df_raw.select(
            col("_id").alias("article_id"),
            col("title"),
            col("description"),
            clean_html_udf(col("content")).alias("content"),
            col("source"),
            col("url"),
            to_timestamp(col("published_at")).alias("published_at"),
            col("collected_at")
        ).dropDuplicates(["article_id"])

        # Load (DELTA LAKE)
        # Delta handles transactions and avoid FileAlreadyExists error.
        silver_path = "s3a://silver/articles_delta"
        logging.info(f"saving in silver (Delta Lake): {silver_path}")
        
        df_silver.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("source") \
            .save(silver_path)
            
        logging.info(f"ETL completed, articles processed: {df_silver.count()}")

    except Exception as e:
        logging.error(f"spark ETL error: {e}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    run_bronze_to_silver()
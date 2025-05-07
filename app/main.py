import os
import joblib
from dotenv import load_dotenv
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, current_timestamp, struct
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import IntegerType, BooleanType
from pyspark.sql.functions import pandas_udf
from typing import Iterator
from confluent_kafka.schema_registry import SchemaRegistryClient

load_dotenv()

# === Config ===
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
GCS_BUCKET = os.getenv("GCS_BUCKET")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE")
GCS_BUCKET_CHECKPOINT = f"gs://{GCS_BUCKET}/checkpoints/bigquery"
BQ_TABLE_REF = f"{GOOGLE_CLOUD_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
SCHEMA_URL = os.getenv("SCHEMA_URL")
SCHEMA_SUBJECT = f"{KAFKA_TOPIC}-value"
MODEL_PATH = "model.pkl"

COLUMNS = [
    "trans_date_trans_time", "cc_num", "merchant", "category", "amt", "first", "last",
    "gender", "street", "city", "state", "zip", "lat", "long", "city_pop", "job",
    "dob", "trans_num", "unix_time", "merch_lat", "merch_long"
]

# === Spark Session ===
spark = SparkSession.builder \
    .appName("PySpark-Kafka-BQ-Optimized") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.fs.gs.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GOOGLE_APPLICATION_CREDENTIALS) \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.executor.instances", "4") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# === Load Schema & Model ===
schema_str = SchemaRegistryClient({'url': SCHEMA_URL}) \
    .get_latest_version(SCHEMA_SUBJECT).schema.schema_str
model = spark.sparkContext.broadcast(joblib.load(MODEL_PATH))

# === Read Kafka Stream ===
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("minPartitions", 4) \
    .option("maxOffsetsPerTrigger", 500) \
    .load()

# === Decode Avro Payload ===
decoded_df = kafka_df \
    .select(substring("value", 6, 1000000).alias("avro_value")) \
    .select(from_avro("avro_value", schema_str).alias("data")) \
    .select("data.*").select(*COLUMNS, "produced_timestamp")

# === Optimized Iterator-based UDF ===


@pandas_udf(IntegerType())
def predict_udf_iter(batch_iter: Iterator[pd.DataFrame]) -> Iterator[pd.Series]:
    for df in batch_iter:
        df.columns = COLUMNS
        yield pd.Series(model.value.predict(df))


# === Apply Model ===
predicted_df = decoded_df.withColumn(
    "prediction", predict_udf_iter(struct(*[col(c) for c in COLUMNS]))
)

# === Final Output Schema ===
final_df = predicted_df.select(
    col("trans_num").cast("string").alias("trans_id"),
    (col("prediction") == 1).cast(BooleanType()).alias("is_fraud"),
    col("produced_timestamp"),
    (current_timestamp().cast("long") * 1000).alias("processed_timestamp"),
    ((current_timestamp().cast("long") * 1000 -
     col("produced_timestamp")).cast("long")).alias("latency_ms")
)

# === BigQuery Write Function ===


# === Streaming Query ===
try:
    final_df.writeStream \
        .format("bigquery") \
        .option("table", BQ_TABLE_REF) \
        .option("checkpointLocation", GCS_BUCKET_CHECKPOINT) \
        .trigger(processingTime="5 seconds") \
        .start()

    spark.streams.awaitAnyTermination()
except Exception as e:
    print("Streaming error:", e)

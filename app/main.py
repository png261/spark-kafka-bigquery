import os
import joblib
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, udf, current_timestamp
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import IntegerType, BooleanType
from confluent_kafka.schema_registry import SchemaRegistryClient

# === Config ===
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service-account-key.json"
GCS_BUCKET, PROJECT, DATASET, TABLE = "spark-bigquery-68686", "inbound-respect-455808-r4", "financial_transactions", "fraud_prediction_2"
BQ_TABLE_REF = f"{PROJECT}.{DATASET}.{TABLE}"
KAFKA_BOOTSTRAP, TOPIC = "35.184.44.247:9092", "transactions_input_with_timestamp"
SCHEMA_URL = "http://34.170.91.127:8081"
SCHEMA_SUBJECT = f"{TOPIC}-value"
MODEL_PATH = "model.pkl"

COLUMNS = [
    "trans_date_trans_time", "cc_num", "merchant", "category", "amt", "first", "last",
    "gender", "street", "city", "state", "zip", "lat", "long", "city_pop", "job",
    "dob", "trans_num", "unix_time", "merch_lat", "merch_long"
]

# === Spark Session ===
spark = SparkSession.builder \
    .appName("PySpark-Kafka-BQ") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.fs.gs.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", os.environ["GOOGLE_APPLICATION_CREDENTIALS"]) \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# === Schema Registry & Model ===
schema_str = SchemaRegistryClient({'url': SCHEMA_URL}).get_latest_version(
    SCHEMA_SUBJECT).schema.schema_str
model = spark.sparkContext.broadcast(joblib.load(MODEL_PATH))

# === Kafka Stream & Avro Decode ===
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", TOPIC).option("startingOffsets", "latest").load()

decoded_df = kafka_df.select(substring("value", 6, 1000000).alias("avro_value")) \
    .select(from_avro("avro_value", schema_str).alias("data")).select("data.*")

# === Prediction UDF ===


@udf(IntegerType())
def predict(*args):
    return int(model.value.predict(pd.DataFrame([args], columns=COLUMNS))[0])


# === Apply Model & Prepare Output ===
predicted_df = decoded_df.withColumn(
    "prediction", predict(*[col(c) for c in COLUMNS]))
final_df = predicted_df.select(
    col("trans_num").cast("string").alias("trans_id"),
    (col("prediction") == 1).cast(BooleanType()).alias("is_fraud"),
    col("produced_timestamp"),
    (current_timestamp().cast("long") * 1000).alias("processed_timestamp"),
    ((current_timestamp().cast("long") * 1000 -
     col("produced_timestamp")).cast("long")).alias("latency_ms")
)

# === Write Function ===


def write_to_bq(df, _):
    df.write.format("bigquery") \
        .option("table", BQ_TABLE_REF) \
        .option("temporaryGcsBucket", GCS_BUCKET) \
        .option("project", PROJECT) \
        .mode("append").save()


# === Streaming ===
try:
    final_df.writeStream.foreachBatch(write_to_bq) \
        .option("checkpointLocation", f"gs://{GCS_BUCKET}/checkpoints/bigquery").start()
    final_df.writeStream.outputMode("append").format("console") \
        .option("truncate", "false").start().awaitTermination()
except Exception as e:
    print("Streaming error:", e)


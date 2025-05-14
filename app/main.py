import os
import uuid
import joblib
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
import h2o
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, current_timestamp, struct, monotonically_increasing_id, datediff, current_date
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import IntegerType, BooleanType, DoubleType
from pyspark.sql.functions import pandas_udf
from typing import Iterator
from confluent_kafka.schema_registry import SchemaRegistryClient
from pysparkling import H2OContext

load_dotenv()

# === Config ===
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
GCS_BUCKET = os.getenv("GCS_BUCKET")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
BIGQUERY_TABLE = os.getenv("BIGQUERY_TABLE")
GCS_BUCKET_CHECKPOINT = f"gs://{GCS_BUCKET}/checkpoints/bigquery/{uuid.uuid4()}"
BQ_TABLE_REF = f"{GOOGLE_CLOUD_PROJECT}.{BIGQUERY_DATASET}.{BIGQUERY_TABLE}"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
SCHEMA_URL = os.getenv("SCHEMA_URL")
SCHEMA_SUBJECT = f"{KAFKA_TOPIC}-value"
# Path to H2O MOJO or binary model (e.g., in GCS)
MODEL_PATH = "model_binary"

# === Spark Session ===
spark = SparkSession.builder \
    .appName("PySpark-Kafka-BQ-H2O") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.fs.gs.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GOOGLE_APPLICATION_CREDENTIALS) \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.streaming.kafka.consumer.poll.ms", "512") \
    .config("spark.executor.instances", "4") \
    .config("spark.executor.cores", "2") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .config("spark.streaming.backpressure.enabled", "true") \
    .config("spark.streaming.kafka.maxRatePerPartition", "1000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# === Initialize H2O ===
h2o.init(strict_version_check=False, enable_web=False)  # Initialize H2O cluster
hc = H2OContext.getOrCreate()  # Initialize H2OContext

# === Load Schema ===
try:
    schema_registry_client = SchemaRegistryClient({'url': SCHEMA_URL})
    schema_str = schema_registry_client.get_latest_version(
        SCHEMA_SUBJECT).schema.schema_str
except Exception as e:
    print(f"Error loading schema: {e}")
    raise

# === Load H2O Model ===
try:
    if MODEL_PATH.endswith(".zip"):  # MOJO model
        model = h2o.import_mojo(MODEL_PATH)
    else:  # H2O binary model
        model = h2o.load_model(MODEL_PATH)
except Exception as e:
    print(f"Error loading H2O model: {e}")
    raise

# === Read Kafka Stream ===
kafka_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("minPartitions", 4) \
    .option("maxOffsetsPerTrigger", 500) \
    .option("kafka.session.timeout.ms", "10000") \
    .option("kafka.max.poll.interval.ms", "300000") \
    .option("kafka.max.poll.records", "500") \
    .load()

# === Decode Avro Payload ===
decoded_df = kafka_df \
    .select(substring(col("value"), 6, 1000000).alias("avro_value")) \
    .select(from_avro(col("avro_value"), schema_str).alias("data")) \
    .select("data.*")

# === Prediction with H2O ===


def prepare_batch_for_prediction(batch_df):
    if 'id' in batch_df.columns:
        batch_df = batch_df.withColumnRenamed('id', 'Unnamed: 0')
    # Get feature names and domains from model
    # exclude response col
    feature_names = model._model_json['output']['names'][:-1]
    # exclude response col
    domains = model._model_json['output']['domains'][:-1]

    # Split into numeric and categorical
    categorical_features = [feature for feature, domain in zip(
        feature_names, domains) if domain is not None]
    numeric_features = [
        feature for feature in feature_names if feature not in categorical_features]

    # Cast numeric features
    for colname in numeric_features:
        batch_df = batch_df.withColumn(colname, col(colname).cast("double"))

    # Cast categorical features to string
    for colname in categorical_features:
        batch_df = batch_df.withColumn(colname, col(colname).cast("string"))

    return batch_df, feature_names, categorical_features


def predict_with_h2o(batch_df, batch_id):
    try:
        batch_df = batch_df.withColumn(
            "_temp_id", monotonically_increasing_id())

        # Prepare batch with correct column types
        batch_df, feature_names, categorical_features = prepare_batch_for_prediction(
            batch_df)

        h2o_frame = hc.asH2OFrame(batch_df)

        # Convert categorical features in H2OFrame to factors
        for feature in categorical_features:
            h2o_frame[feature] = h2o_frame[feature].asfactor()

        predictions = model.predict(h2o_frame)
        pred_df = hc.asSparkFrame(predictions).withColumn(
            "_temp_id", monotonically_increasing_id())
        joined_df = batch_df.join(pred_df, "_temp_id", "inner")

        result_df = joined_df.select(
            col("trans_num").cast("string").alias("trans_id"),
            (col("predict") == "1").cast(BooleanType()).alias("is_fraud"),
            col("produced_timestamp"),
            (current_timestamp().cast("long") * 1000).alias("processed_timestamp"),
            ((current_timestamp().cast("long") * 1000 -
             col("produced_timestamp")).cast("long")).alias("latency_ms")
        )

        result_df.write \
            .format("bigquery") \
            .option("table", BQ_TABLE_REF) \
            .option("temporaryGcsBucket", GCS_BUCKET) \
            .mode("append") \
            .save()

        print(f"Batch {batch_id} written to BigQuery table {BQ_TABLE_REF}")

    except Exception as e:
        print(f"Error in batch {batch_id}: {e}")
        raise


# === Streaming Query ===
try:
    query = decoded_df.writeStream \
        .foreachBatch(predict_with_h2o) \
        .option("checkpointLocation", GCS_BUCKET_CHECKPOINT) \
        .trigger(processingTime="5 seconds") \
        .start()

    query.awaitTermination()
except Exception as e:
    print(f"Streaming query failed: {e}")
    raise
finally:
    hc.stop()  # Stop H2OContext
    h2o.cluster().shutdown()  # Shutdown H2O cluster
    spark.stop()  # Stop Spark session

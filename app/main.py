import os
import requests
import numpy as np
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import struct, col, to_timestamp, to_date, udf
from pyspark.sql.types import IntegerType, BooleanType
from pyspark.sql.column import Column, _to_java_column

# List of required environment variables
required_envs = [
    "GCS_BUCKET_ID",
    "BQ_PROJECT_ID",
    "BQ_DATASET_ID",
    "BG_TABLE_ID",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "PREDICTION_SERVICE_URL",
    "KAFKA_BOOTSTRAP_SERVERS",
    "KAFKA_SUBSCRIBE",
    "SCHEMA_REGISTRY_URL"
]

for env_var in required_envs:
    if env_var not in os.environ:
        raise ValueError(f"Missing required environment variable: {env_var}")

# Read environment variables
GCS_BUCKET_ID = os.environ["GCS_BUCKET_ID"]
BQ_PROJECT_ID = os.environ["BQ_PROJECT_ID"]
BQ_DATASET_ID = os.environ["BQ_DATASET_ID"]
BG_TABLE_ID = os.environ["BG_TABLE_ID"]
PREDICTION_SERVICE_URL = os.environ["PREDICTION_SERVICE_URL"]
KAFKA_BOOTSTRAP_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_SUBSCRIBE = os.environ["KAFKA_SUBSCRIBE"]
SCHEMA_REGISTRY_URL = os.environ["SCHEMA_REGISTRY_URL"]
GOOGLE_APPLICATION_CREDENTIALS = os.environ["GOOGLE_APPLICATION_CREDENTIALS"]

BG_TABLE_REF = f"{BQ_PROJECT_ID}.{BQ_DATASET_ID}.{BG_TABLE_ID}"

# Start SparkSession
spark = SparkSession.builder \
    .appName("PySpark-Kafka-BigQuery") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.fs.gs.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GOOGLE_APPLICATION_CREDENTIALS) \
    .getOrCreate()

# Kafka Streaming Source
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_SUBSCRIBE) \
    .load()

# ABRiS helpers


def from_avro_abris_config(config_map, topic, is_key=False):
    jvm_gateway = spark._jvm
    scala_map = jvm_gateway.PythonUtils.toScalaMap(config_map)
    return (jvm_gateway.za.co.absa.abris.config.AbrisConfig
            .fromConfluentAvro()
            .downloadReaderSchemaByLatestVersion()
            .andTopicNameStrategy(topic, is_key)
            .usingSchemaRegistry(scala_map))


def from_avro(col, abris_config):
    jvm_gateway = spark._jvm
    abris_avro = jvm_gateway.za.co.absa.abris.avro
    return Column(abris_avro.functions.from_avro(_to_java_column(col), abris_config))


# ABRiS config
schema_registry_config = {
    "schema.registry.url": SCHEMA_REGISTRY_URL
}

abris_config = from_avro_abris_config(schema_registry_config, KAFKA_SUBSCRIBE)

# Decode Avro from Kafka
df_decoded = kafka_df.select(
    from_avro(col("value"), abris_config).alias("data")
).select("data.*")

# Predict UDF


def predict_func(*cols):
    try:
        input_data = np.array(cols).reshape(1, -1).tolist()
        response = requests.post(
            PREDICTION_SERVICE_URL,
            json=input_data,
            timeout=5
        )
        response.raise_for_status()
        prediction = response.json().get("prediction")
        return int(prediction)
    except Exception as e:
        print("Prediction service error:", e)
        return 0


predict_udf = udf(predict_func, IntegerType())

# Transform Data
df_transformed = df_decoded \
    .withColumn("trans_date_trans_time", to_timestamp("trans_date_trans_time")) \
    .withColumn("dob", to_date("dob"))

predicted_df = df_transformed.withColumn(
    "prediction", predict_udf(struct(*df_transformed.columns))
)

final_df = predicted_df.select(
    col("trans_num").cast("string").alias("trans_id"),
    (col("prediction") == 1).cast(BooleanType()).alias("is_fraud")
)

# Write to BigQuery


def write_to_bigquery(batch_df, batch_id):
    batch_df.write \
        .format("bigquery") \
        .option("table", BG_TABLE_REF) \
        .option("temporaryGcsBucket", GCS_BUCKET_ID) \
        .option("project", BQ_PROJECT_ID) \
        .mode("append") \
        .save()


# Start query
try:
    query = final_df.writeStream \
        .foreachBatch(write_to_bigquery) \
        .option("checkpointLocation", "/tmp/spark_checkpoints") \
        .start()

    query.awaitTermination()

except Exception as e:
    print("Streaming query failed!")
    traceback.print_exc()
    if hasattr(e, "__cause__") and e.__cause__:
        print("\nRoot cause (Java):")
        print(e.__cause__)

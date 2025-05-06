# Spark-Kafka-Bigquery
A Dockerized pipeline using Apache Spark to consume data from Kafka, process it, and write results to BigQuery.

## How to run
### 1. Pull the Docker Image
```
docker pull ghcr.io/png261/spark-kafka-bigquery:main
```
### 2. Create a `.env` file:
```
GOOGLE_CLOUD_PROJECT=
GCS_BUCKET=
BIGQUERY_DATASET=
BIGQUERY_TABLE=
KAFKA_BOOTSTRAP=
KAFKA_TOPIC=
SCHEMA_URL=
```

### 3. Run Spark Master Container
Bind your `service-account-key.json` file when launching the container:
```
sudo docker run \
  --network host \
  --env-file <path-to-your-local-.env>: \
  -v <path-to-your-local-service-account-key.json>:/app/service-account-key.json \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/service-account-key.json \
  -e SPARK_MODE=master \
  -d --name spark-master ghcr.io/png261/spark-kafka-bigquery
```
### 4. Get the Spark Master URL
After the master container is running, get the **SPARK_MASTER_URL**:

```
sudo docker logs spark-master 2>&1 | grep -oP 'spark://\S+:\d+' | head -1
```

### 5. Add workers
Replace <YOUR_SPARK_MASTER_URL> with the URL from the previous step:
```
sudo docker run \
  --network host \
  --env-file <path-to-your-local-.env>: \
  -v <path-to-your-local-service-account-key.json>:/app/service-account-key.json \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/service-account-key.json \
  -e SPARK_MODE=worker \
  -e SPARK_MASTER_URL=<YOUR_SPARK_MASTER_URL> \
  -d --name spark-worker ghcr.io/png261/spark-kafka-bigquery
```

### 6. Submit the PySpark Job
Make sure the job script `app/main.py` is available inside the container:
```
sudo docker exec -u root spark-master /bin/bash -c "spark-submit --master <YOUR_SPARK_MASTER_URL> app/main.py"
```

## Environment Variables Descriptions:
| Variable                         | Description                                                                 |
|----------------------------------|-----------------------------------------------------------------------------|
| `GOOGLE_APPLICATION_CREDENTIALS` | Path to your Google Cloud service account key file inside the container.   |
| `GOOGLE_CLOUD_PROJECT`           | Your Google Cloud project ID.                                               |
| `GCS_BUCKET`                     | Google Cloud Storage bucket name.                                           |
| `BIGQUERY_DATASET`               | BigQuery dataset where data will be written.                                |
| `BIGQUERY_TABLE`                 | BigQuery table for storing the results.                                     |
| `KAFKA_BOOTSTRAP`                | Address of your Kafka broker.                                               |
| `KAFKA_TOPIC`                    | Kafka topic to consume messages from.                                       |
| `SCHEMA_URL`                     | URL to fetch the Avro schema for Kafka messages.                            |

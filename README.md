# Spark-Kafka-Bigquery
## How to run
### 1. Pull the Docker Image
```
docker pull ghcr.io/png261/fraud-dection-model-microservice:main

```
### 2. Run the Docker Container
Environment Variables:
- GOOGLE_APPLICATION_CREDENTIALS: Path to your Google Cloud service account key file.
- GOOGLE_CLOUD_PROJECT: Your Google Cloud project ID.
- GCS_BUCKET: Google Cloud Storage bucket for storing BigQuery data.
- DATASET: BigQuery dataset where results will be stored.
- TABLE: BigQuery table to store fraud prediction results.
- KAFKA_BOOTSTRAP: The Kafka broker address.
- TOPIC: Kafka topic to consume data from.
- SCHEMA_URL: URL for schema registry to fetch Avro schema.
- PREDICTOR_API: The URL of the FastAPI service to interact with.
 
```
docker run -d \
    -v $(pwd)/service-account-key.json:/app/service-account-key.json \
    -e GOOGLE_APPLICATION_CREDENTIALS=/app/service-account-key.json \
    -e GOOGLE_CLOUD_PROJECT=<your-gcp-project-id> \
    -e GCS_BUCKET=<your-gcs-bucket> \
    -e PROJECT=<your-gcp-project-id> \
    -e DATASET=<your-bigquery-dataset> \
    -e TABLE=<your-bigquery-table> \
    -e KAFKA_BOOTSTRAP=<your-kafka-broker-address> \
    -e TOPIC=<your-kafka-topic> \
    -e SCHEMA_URL=<your-schema-registry-url> \
    -e PREDICTOR_API=<your-predictor-api-url> \
    <your-docker-image-name>

```
Forexample:
```
docker run -d \
    -v $(pwd)/service-account-key.json:/app/service-account-key.json \
    -e GOOGLE_APPLICATION_CREDENTIALS=/app/service-account-key.json \
    -e GOOGLE_CLOUD_PROJECT=inbound-respect-455808-r4 \
    -e GCS_BUCKET=spark-bigquery-68686 \
    -e PROJECT=inbound-respect-455808-r4 \
    -e DATASET=financial_transactions \
    -e TABLE=fraud_prediction_2 \
    -e KAFKA_BOOTSTRAP=34.135.150.218:9092 \
    -e TOPIC=fulll3 \
    -e SCHEMA_URL=http://34.170.91.127:8081 \
    -e PREDICTOR_API=http://127.0.0.1:8000 \
    spark-kafka-bigquery-app
```

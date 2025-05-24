# Spark-Kafka-BigQuery Pipeline

A containerized data processing pipeline that uses Apache Spark to consume streaming data from Apache Kafka, process it in real-time, and store the results in Google BigQuery.

## 🏗️ Architecture

This pipeline provides:
- **Real-time data ingestion** from Kafka topics
- **Distributed processing** using Apache Spark
- **Cloud-native storage** with Google BigQuery integration
- **Containerized deployment** for easy scaling and management

## 📋 Prerequisites

Before getting started, ensure you have:

- Docker installed and running
- Google Cloud Platform account with a project
- Kafka cluster accessible from your Docker environment
- Google Cloud service account with appropriate permissions

### Required Google Cloud Permissions

Your service account needs the following IAM roles:
- `BigQuery Data Editor`
- `BigQuery Data Owner` 
- `BigQuery Job User`

## 🚀 Quick Start

### Step 1: Pull the Docker Image

```bash
docker pull ghcr.io/png261/spark-kafka-bigquery:main
```

### Step 2: Configure Environment Variables

Create a `.env` file in your working directory with the following configuration:

```env
# Google Cloud Configuration
GOOGLE_CLOUD_PROJECT=your-gcp-project-id
GCS_BUCKET=your-gcs-bucket-name
BIGQUERY_DATASET=your-bigquery-dataset
BIGQUERY_TABLE=your-bigquery-table

# Kafka Configuration
KAFKA_BOOTSTRAP=your-kafka-broker:9092
KAFKA_TOPIC=your-kafka-topic

# Schema Configuration
SCHEMA_URL=https://your-schema-registry/schemas/your-schema
```

### Step 3: Start Spark Master

Launch the Spark master container with your service account credentials:

```bash
docker run \
  --network host \
  --env-file .env \
  -v /path/to/your/service-account-key.json:/app/service-account-key.json \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/service-account-key.json \
  -e SPARK_MODE=master \
  -d --name spark-master \
  ghcr.io/png261/spark-kafka-bigquery:main
```

### Step 4: Get Spark Master URL

Retrieve the Spark master URL for connecting workers:

```bash
docker logs spark-master 2>&1 | grep -oP 'spark://\S+:\d+' | head -1
```

Save this URL as you'll need it for the next steps.

### Step 5: Add Spark Workers

Start one or more worker containers (replace `<SPARK_MASTER_URL>` with the URL from Step 4):

```bash
docker run \
  --network host \
  --env-file .env \
  -v /path/to/your/service-account-key.json:/app/service-account-key.json \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/service-account-key.json \
  -e SPARK_MODE=worker \
  -e SPARK_MASTER_URL=<SPARK_MASTER_URL> \
  -d --name spark-worker-1 \
  ghcr.io/png261/spark-kafka-bigquery:main
```

**Tip:** You can scale by running multiple worker containers with different names (e.g., `spark-worker-2`, `spark-worker-3`).

### Step 6: Submit the Processing Job

Execute the data processing pipeline:

```bash
docker exec -u root spark-master spark-submit \
  --master <SPARK_MASTER_URL> \
  app/main.py
```

## 🔧 Advanced Usage

### Scaling Workers

To add more processing power, start additional worker containers:

```bash
for i in {2..4}; do
  docker run \
    --network host \
    --env-file .env \
    -v /path/to/your/service-account-key.json:/app/service-account-key.json \
    -e GOOGLE_APPLICATION_CREDENTIALS=/app/service-account-key.json \
    -e SPARK_MODE=worker \
    -e SPARK_MASTER_URL=<SPARK_MASTER_URL> \
    -d --name spark-worker-$i \
    ghcr.io/png261/spark-kafka-bigquery:main
done
```

### Monitoring
View container logs:
```bash
# Master logs
docker logs -f spark-master

# Worker logs  
docker logs -f spark-worker-1

# Job execution logs
docker exec spark-master spark-submit --help
```

### Cleanup

Stop and remove all containers:
```bash
docker stop spark-master spark-worker-1
docker rm spark-master spark-worker-1
```

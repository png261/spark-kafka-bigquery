# Use official Spark image
FROM bitnami/spark:3.5.5

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/
COPY jars/ /opt/spark/jars/

# Set environment variables for Spark and application configurations
ENV GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS}
ENV GCS_BUCKET=${GCS_BUCKET}
ENV PROJECT=${PROJECT}
ENV DATASET=${DATASET}
ENV TABLE=${TABLE}
ENV KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP}
ENV TOPIC=${TOPIC}
ENV SCHEMA_URL=${SCHEMA_URL}
ENV PREDICTOR_API=${PREDICTOR_API}

# Set entrypoint to spark-submit with all required packages and jars
CMD ["spark-submit", "--repositories", "https://packages.confluent.io/maven", \
    "--jars", "/opt/spark/jars/*", \
    "/app/app/main.py"]


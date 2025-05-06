# Use official Spark image
FROM bitnami/spark:3.5.5

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN /opt/bitnami/python/bin/pip install -r requirements.txt

# Copy application code
COPY app/ ./app/
COPY model.pkl ./model.pkl
COPY jars/ /opt/bitnami/spark/jars/

# Create writable Ivy cache directory
RUN mkdir -p /tmp/.ivy2 && chmod -R 777 /tmp/.ivy2
# Set spark.jars.ivy config to use the writable directory
RUN echo "spark.jars.ivy /tmp/.ivy2" >> /opt/bitnami/spark/conf/spark-defaults.conf

ENV GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS}
ENV GCS_BUCKET=${GCS_BUCKET}
ENV PROJECT=${PROJECT}
ENV DATASET=${DATASET}
ENV TABLE=${TABLE}
ENV KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP}
ENV TOPIC=${TOPIC}
ENV SCHEMA_URL=${SCHEMA_URL}

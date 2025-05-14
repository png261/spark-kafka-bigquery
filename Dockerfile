# Use official Spark image
FROM bitnami/spark:3.5.5

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN /opt/bitnami/python/bin/pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /tmp/.ivy2 && chmod -R 777 /tmp/.ivy2 && \
    echo "spark.jars.ivy /tmp/.ivy2" >> /opt/bitnami/spark/conf/spark-defaults.conf

# Copy application code
COPY app/ ./app/
COPY model_binary ./model_binary
COPY jars/ /opt/bitnami/spark/jars/


ENV GOOGLE_APPLICATION_CREDENTIALS=${GOOGLE_APPLICATION_CREDENTIALS} \

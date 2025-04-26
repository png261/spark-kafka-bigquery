# Use official Spark image
FROM bitnami/spark:3.5.5

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ ./app/

# Download necessary JARs
RUN wget https://packages.confluent.io/maven/za/co/absa/abris_2.12/6.3.0/abris_2.12-6.3.0.jar -P /opt/spark/jars
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.5/spark-sql-kafka-0-10_2.12-3.5.5.jar -P /opt/spark/jars
RUN wget https://repo1.maven.org/maven2/org/apache/spark/spark-avro_2.12/3.5.1/spark-avro_2.12-3.5.1.jar -P /opt/spark/jars
RUN wget https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar -P /opt/spark/jars
RUN wget https://repo1.maven.org/maven2/io/confluent/common-utils/6.2.1/common-utils-6.2.1.jar -P /opt/spark/jars
RUN wget https://repo1.maven.org/maven2/io/confluent/kafka-schema-registry-client/6.2.1/kafka-schema-registry-client-6.2.1.jar -P /opt/spark/jars
RUN wget https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.42.1.jar -P /opt/spark/jars
RUN wget https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar -P /opt/spark/jars

# Set entrypoint to spark-submit with all required packages and jars
CMD ["spark-submit", "--repositories", "https://packages.confluent.io/maven", \
    "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,za.co.absa:abris_2.12:6.3.0", \
    "--jars", "/opt/spark/jars/abris_2.12-6.3.0.jar,/opt/spark/jars/spark-avro_2.12-3.5.1.jar,/opt/spark/jars/kafka-clients-3.5.1.jar,/opt/spark/jars/common-utils-6.2.1.jar,/opt/spark/jars/kafka-schema-registry-client-6.2.1.jar,/opt/spark/jars/spark-bigquery-with-dependencies_2.12-0.42.1.jar,/opt/spark/jars/common-utils-6.2.1.jar", \
    "/app/app/main.py"]


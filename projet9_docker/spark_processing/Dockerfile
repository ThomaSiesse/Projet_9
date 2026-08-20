FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jdk-headless wget && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="$JAVA_HOME/bin:$PATH"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /opt/spark-jars && cd /opt/spark-jars && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.2.0/spark-sql-kafka-0-10_2.13-4.2.0.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.2.0/spark-token-provider-kafka-0-10_2.13-4.2.0.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar && \
    wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar

ENV SPARK_LOCAL_IP=127.0.0.1

COPY read_client_tickets.py .

RUN mkdir -p /app/data /app/checkpoints

CMD ["python", "read_client_tickets.py"]

FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH

RUN curl -o spark.tgz https://dlcdn.apache.org/spark/spark-4.0.0-preview2/spark-4.0.0-preview2-bin-hadoop3.tgz && \
    tar -xzf spark.tgz && \
    mv spark-4.0.0-preview2-bin-hadoop3 /opt/spark && \
    rm spark.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

EXPOSE 8000
CMD ["python", "run.py"]
FROM python:3.10-slim

# Install Java 17
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PATH="$JAVA_HOME/bin:$PATH"

# Download and install Spark 4.0.0-preview2
RUN curl -o spark.tgz https://dlcdn.apache.org/spark/spark-4.0.0-preview2/spark-4.0.0-preview2-bin-hadoop3.tgz && \
    tar -xzf spark.tgz && \
    mv spark-4.0.0-preview2-bin-hadoop3 /opt/spark && \
    rm spark.tgz

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Expose port
EXPOSE 8000

# Command to run the application
CMD ["python", "run.py"]
# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Install necessary system packages
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME environment variable
ENV JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
ENV PATH="$JAVA_HOME/bin:$PATH"

# Download and install Spark 4.0.0-preview2
RUN curl -o spark-4.0.0-preview2-bin-hadoop3.tgz \
    https://downloads.apache.org/spark/spark-4.0.0-preview2/spark-4.0.0-preview2-bin-hadoop3.tgz && \
    tar -xvzf spark-4.0.0-preview2-bin-hadoop3.tgz && \
    mv spark-4.0.0-preview2-bin-hadoop3 /opt/spark

# Set Spark environment variables
ENV SPARK_HOME="/opt/spark"
ENV PATH="$SPARK_HOME/bin:$PATH"

# Set PYTHONPATH to include PySpark and Py4J
# Dynamically find the Py4J zip file
RUN ln -s $(ls $SPARK_HOME/python/lib/py4j-*-src.zip) $SPARK_HOME/python/lib/py4j-src.zip
ENV PYTHONPATH="$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-src.zip:$PYTHONPATH"

# Copy the requirements file into the container at /app
COPY requirements.txt .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Install Flask development server
RUN pip install --no-cache-dir flask[async]
RUN curl -L -o /app/postgresql-42.6.0.jar https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Copy the rest of the working directory contents into the container at /app
COPY . /app

# Expose port 3000 for the Flask app
EXPOSE 3000

# Run app.py when the container launches
CMD ["python", "app.py"]
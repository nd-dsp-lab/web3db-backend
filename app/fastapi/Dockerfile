# Use a minimal Python image
FROM python:3.9-slim

# Install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jdk \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Verify Java installation
RUN java -version

# Install Python dependencies
RUN pip install --no-cache-dir fastapi uvicorn[standard] pyspark==3.3.2 pandas pyarrow requests python-multipart

# Copy the FastAPI app
COPY app.py /app.py

# Expose FastAPI port
EXPOSE 8000

# Run FastAPI with Uvicorn
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
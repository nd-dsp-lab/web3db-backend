# Use a minimal Python image
FROM python:3.9-slim

# Install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    default-jdk \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set Java environment variables dynamically based on actual installation
RUN echo "Setting up Java environment..." && \
    JAVA_HOME=$(readlink -f /usr/bin/java | sed "s:/bin/java::") && \
    if [ -z "$JAVA_HOME" ]; then \
        JAVA_HOME=$(find /usr/lib/jvm -name "java-*-openjdk-*" -type d | head -n 1); \
    fi && \
    echo "JAVA_HOME detected as $JAVA_HOME" && \
    echo "export JAVA_HOME=$JAVA_HOME" > /etc/profile.d/java.sh && \
    echo "export PATH=\$PATH:\$JAVA_HOME/bin" >> /etc/profile.d/java.sh && \
    echo "JAVA_HOME=$JAVA_HOME" >> /etc/environment

# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
# For local testing
# ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
# ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Verify Java installation and show debug info
RUN echo "Verifying Java installation..." && \
    java -version && \
    echo "Java home is: $JAVA_HOME" && \
    ls -la $JAVA_HOME 2>/dev/null || echo "Warning: Cannot list JAVA_HOME directory"

# Install Python dependencies
RUN pip install --no-cache-dir fastapi uvicorn[standard] pyspark==3.3.2 pandas pyarrow requests python-multipart pygtrie python-dotenv

# Copy the FastAPI app
COPY app.py /app.py
COPY bplustree.py /bplustree.py
COPY cidindex.py /cidindex.py
COPY trie.py /trie.py

# Set environment variable for PySpark to find Java
ENV PYSPARK_SUBMIT_ARGS="--driver-java-options=-Dlog4j.logLevel=info pyspark-shell"

# Add a healthcheck for Java
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
  CMD java -version || exit 1

# Expose FastAPI port
EXPOSE 8000

# Run FastAPI with Uvicorn
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
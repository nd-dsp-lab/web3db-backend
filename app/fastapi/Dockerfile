# Use a minimal Python image
FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies including cryptography for AES encryption
RUN pip install --no-cache-dir \
    fastapi \
    uvicorn[standard] \
    pandas \
    pyarrow \
    requests \
    python-multipart \
    pygtrie \
    python-dotenv \
    duckdb \
    cryptography

# Copy the FastAPI app and related modules
COPY app.py /app.py
COPY bplustree.py /bplustree.py
COPY cidindex.py /cidindex.py
COPY trie.py /trie.py

# Create directory for temporary parquet files
RUN mkdir -p /tmp/ipfs_parquet

# Add a healthcheck
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
  CMD python -c "import duckdb; import cryptography; print('Health check passed')" || exit 1

# Expose FastAPI port
EXPOSE 8000

# Run FastAPI with Uvicorn
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]

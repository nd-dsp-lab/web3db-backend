# Get Gramine version to check for compatibility issues
FROM ubuntu:22.04 as gramine-check
RUN apt-get update && apt-get install -y --no-install-recommends \
    software-properties-common \
    curl \
    ca-certificates \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSLo /usr/share/keyrings/gramine-keyring.gpg https://packages.gramineproject.io/gramine-keyring.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/gramine-keyring.gpg] https://packages.gramineproject.io/ $(lsb_release -sc) main" > /etc/apt/sources.list.d/gramine.list && \
    apt-get update && apt-get install -y gramine && \
    gramine-sgx --version > /gramine-version.txt

# Main stage
FROM ubuntu:22.04

ARG DEBIAN_FRONTEND=noninteractive

# Install prerequisites
RUN apt-get update && apt-get install -y --no-install-recommends \
    software-properties-common \
    wget \
    gnupg \
    curl \
    ca-certificates \
    python3 \
    python3-pip \
    openjdk-11-jdk-headless \
    procps \
    libyaml-0-2 \
    openssl \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Gramine
RUN curl -fsSLo /usr/share/keyrings/gramine-keyring.gpg https://packages.gramineproject.io/gramine-keyring.gpg && \
    echo "deb [arch=amd64 signed-by=/usr/share/keyrings/gramine-keyring.gpg] https://packages.gramineproject.io/ $(lsb_release -sc) main" > /etc/apt/sources.list.d/gramine.list && \
    apt-get update && apt-get install -y gramine && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA env
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Validate Java
RUN java -version

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt

# Copy application code
COPY app.py .
COPY bplustree.py .
COPY cidindex.py .
COPY trie.py .

# Copy manifest template
COPY app.py.manifest.template .

# Generate SGX key and manifest
RUN gramine-sgx-gen-private-key && \
    gramine-manifest \
    -Dlog_level=debug \
    -Darch_libdir=/lib/x86_64-linux-gnu \
    app.py.manifest.template app.py.manifest && \
    gramine-sgx-sign \
    --manifest app.py.manifest \
    --output app.py.manifest.sgx

# Set environment for Python
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Expose FastAPI and Spark UI
EXPOSE 8000
EXPOSE 4040

# Use gramine-direct for debugging
# CMD ["gramine-direct", "app.py"]
CMD ["gramine-sgx", "app.py"]
FROM alpine:latest

# Install autossh, openssh-client, procps (for pgrep), and yq (for YAML parsing)
RUN apk add --no-cache autossh openssh-client procps yq

# Create directory for SSH keys and logs
RUN mkdir -p /app/ssh /app/logs

# Build argument for SSH key filename (key should be copied to build context)
# Default: ec2.pem (expects file to be in build context root)
ARG SSH_KEY_FILE=ec2.pem

# Copy SSH key into image
# The key file should be placed in the build context directory before building
COPY ${SSH_KEY_FILE} /app/ssh/ec2.pem
RUN chmod 600 /app/ssh/ec2.pem

# Copy entrypoint script, tunnel configuration, and nginx template
COPY entrypoint.sh /app/entrypoint.sh
COPY tunnel.yml /app/tunnel.yml
COPY nginx.example /app/nginx.example
RUN chmod +x /app/entrypoint.sh

WORKDIR /app

# Set environment variables with defaults
ENV REMOTE_PORT=5000
ENV LOCAL_PORT=5000
ENV REMOTE_USER=ubuntu
ENV REMOTE_HOST=""
ENV SSH_KEY_PATH=/app/ssh/ec2.pem
ENV TUNNEL_CONFIG=/app/tunnel.yml
ENV LOG_FILE=/app/logs/tunnel.log
ENV TARGET_HOST=localhost

# Expose the local port (for documentation, actual forwarding is via SSH)
EXPOSE ${LOCAL_PORT}

ENTRYPOINT ["/app/entrypoint.sh"]


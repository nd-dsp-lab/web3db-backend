#!/bin/sh

set -e

# Configuration file path
TUNNEL_CONFIG=${TUNNEL_CONFIG:-/app/tunnel.yml}
SSH_KEY_PATH=${SSH_KEY_PATH:-/app/ssh/ec2.pem}
TARGET_HOST=${TARGET_HOST:-localhost}
LOG_DIR=${LOG_DIR:-/app/logs}

# Check if tunnel config file exists
if [ ! -f "$TUNNEL_CONFIG" ]; then
    echo "Error: Tunnel configuration file not found at $TUNNEL_CONFIG"
    exit 1
fi

# Check if SSH key exists
if [ ! -f "$SSH_KEY_PATH" ]; then
    echo "Error: SSH key not found at $SSH_KEY_PATH"
    echo "The SSH key should have been copied into the image during build"
    exit 1
fi

# Verify SSH key permissions
if [ -f "$SSH_KEY_PATH" ]; then
    PERMS=$(stat -c "%a" "$SSH_KEY_PATH" 2>/dev/null || stat -f "%OLp" "$SSH_KEY_PATH" 2>/dev/null || echo "unknown")
    if [ "$PERMS" != "600" ] && [ "$PERMS" != "unknown" ]; then
        echo "Warning: SSH key permissions are $PERMS, should be 600"
        chmod 600 "$SSH_KEY_PATH" 2>/dev/null || echo "Could not fix permissions"
    fi
fi

# Create logs directory
mkdir -p "$LOG_DIR"

# Read server configuration from YAML
REMOTE_HOST=$(yq eval '.server.REMOTE_HOST' "$TUNNEL_CONFIG")
REMOTE_USER=$(yq eval '.server.REMOTE_USER' "$TUNNEL_CONFIG")
NGINX_SITE_CONFIG=$(yq eval '.nginx.site_config_file' "$TUNNEL_CONFIG")
NGINX_TEMPLATE="/app/nginx.example"

if [ -z "$REMOTE_HOST" ] || [ "$REMOTE_HOST" = "null" ]; then
    echo "Error: REMOTE_HOST not found in tunnel configuration"
    exit 1
fi

if [ -z "$REMOTE_USER" ] || [ "$REMOTE_USER" = "null" ]; then
    REMOTE_USER=${REMOTE_USER:-ubuntu}
fi

if [ -z "$NGINX_SITE_CONFIG" ] || [ "$NGINX_SITE_CONFIG" = "null" ]; then
    echo "Warning: nginx.site_config_file not found in tunnel configuration, skipping nginx update"
    NGINX_SITE_CONFIG=""
fi

# Read nginx server_name, default to REMOTE_HOST if not specified
NGINX_SERVER_NAME=$(yq eval '.nginx.server_name' "$TUNNEL_CONFIG")
if [ -z "$NGINX_SERVER_NAME" ] || [ "$NGINX_SERVER_NAME" = "null" ]; then
    NGINX_SERVER_NAME="$REMOTE_HOST"
fi

# Display configuration
echo "=========================================="
echo "Reverse SSH Tunnel Configuration"
echo "=========================================="
echo "Remote Host: $REMOTE_HOST"
echo "Remote User: $REMOTE_USER"
echo "SSH Key: $SSH_KEY_PATH"
echo "Target Host: $TARGET_HOST"
echo "Config File: $TUNNEL_CONFIG"
if [ -n "$NGINX_SITE_CONFIG" ]; then
    echo "Nginx Config: $NGINX_SITE_CONFIG"
fi
echo "=========================================="
echo ""

# Get list of tunnel names
TUNNEL_NAMES=$(yq eval '.tunnels | keys | .[]' "$TUNNEL_CONFIG")

if [ -z "$TUNNEL_NAMES" ]; then
    echo "Error: No tunnels defined in configuration"
    exit 1
fi

# Update nginx configuration on remote server if configured
if [ -n "$NGINX_SITE_CONFIG" ] && [ -f "$NGINX_TEMPLATE" ]; then
    echo "=========================================="
    echo "Updating Nginx Configuration"
    echo "=========================================="
    
    # Generate nginx config from template
    NGINX_CONFIG=$(mktemp)
    
    # Use server_name (already defaulted to REMOTE_HOST if not specified)
    SERVER_NAME="$NGINX_SERVER_NAME"
    
    # Start building nginx config
    {
        echo "server {"
        echo "    listen 80;"
        echo "    server_name $SERVER_NAME;"
        echo ""
        
        # Add location blocks for each tunnel
        for TUNNEL_NAME in $TUNNEL_NAMES; do
            REMOTE_PORT=$(yq eval ".tunnels.$TUNNEL_NAME.remote_port" "$TUNNEL_CONFIG")
            if [ -n "$REMOTE_PORT" ] && [ "$REMOTE_PORT" != "null" ]; then
                # Use tunnel name as path prefix (e.g., /web3db/)
                LOCATION_PATH="/${TUNNEL_NAME}/"
                echo "    location ${LOCATION_PATH} {"
                echo "        proxy_pass http://127.0.0.1:${REMOTE_PORT}/;"
                echo "    }"
            fi
        done
        
        echo "}"
    } > "$NGINX_CONFIG"
    
    echo "Generated nginx configuration:"
    cat "$NGINX_CONFIG"
    echo ""
    
    # Copy config to remote server (disable set -e for this section)
    set +e
    echo "Copying nginx config to $REMOTE_USER@$REMOTE_HOST:$NGINX_SITE_CONFIG"
    scp -i "$SSH_KEY_PATH" \
        -o StrictHostKeyChecking=no \
        -o UserKnownHostsFile=/dev/null \
        "$NGINX_CONFIG" \
        ${REMOTE_USER}@${REMOTE_HOST}:/tmp/nginx_config_temp > /dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        # Move the file to the final location and reload nginx
        echo "Installing nginx config and reloading service..."
        ssh -i "$SSH_KEY_PATH" \
            -o StrictHostKeyChecking=no \
            -o UserKnownHostsFile=/dev/null \
            ${REMOTE_USER}@${REMOTE_HOST} \
            "sudo mv /tmp/nginx_config_temp $NGINX_SITE_CONFIG && sudo nginx -t && sudo systemctl reload nginx" 2>&1
        
        if [ $? -eq 0 ]; then
            echo "✓ Nginx configuration updated and reloaded successfully"
        else
            echo "✗ Failed to update nginx configuration"
            echo "Warning: Continuing with tunnel setup despite nginx update failure"
        fi
    else
        echo "✗ Failed to copy nginx config to remote server"
        echo "Warning: Continuing with tunnel setup despite nginx update failure"
    fi
    
    # Re-enable set -e
    set -e
    
    # Clean up temp file
    rm -f "$NGINX_CONFIG"
    
    echo "=========================================="
    echo ""
fi

# Start tunnels
TUNNEL_COUNT=0
FAILED_TUNNELS=""

for TUNNEL_NAME in $TUNNEL_NAMES; do
    LOCAL_PORT=$(yq eval ".tunnels.$TUNNEL_NAME.local_port" "$TUNNEL_CONFIG")
    REMOTE_PORT=$(yq eval ".tunnels.$TUNNEL_NAME.remote_port" "$TUNNEL_CONFIG")
    
    if [ -z "$LOCAL_PORT" ] || [ "$LOCAL_PORT" = "null" ] || [ -z "$REMOTE_PORT" ] || [ "$REMOTE_PORT" = "null" ]; then
        echo "Warning: Skipping tunnel '$TUNNEL_NAME' - missing local_port or remote_port"
        continue
    fi
    
    TUNNEL_LOG="$LOG_DIR/tunnel_${TUNNEL_NAME}.log"
    
    echo "Starting tunnel '$TUNNEL_NAME': $REMOTE_HOST:$REMOTE_PORT -> $TARGET_HOST:$LOCAL_PORT"
    
    # Start autossh tunnel for this port pair
    autossh -M 0 \
        -o ServerAliveInterval=30 \
        -o ServerAliveCountMax=3 \
        -o ExitOnForwardFailure=yes \
        -o StrictHostKeyChecking=no \
        -o UserKnownHostsFile=/dev/null \
        -o IdentitiesOnly=yes \
        -R ${REMOTE_PORT}:${TARGET_HOST}:${LOCAL_PORT} \
        -i "$SSH_KEY_PATH" \
        -N \
        -f \
        -E "$TUNNEL_LOG" \
        ${REMOTE_USER}@${REMOTE_HOST}
    
    # Wait a moment for autossh to start
    sleep 1
    
    # Check if tunnel started successfully
    if pgrep -f "autossh.*-R.*${REMOTE_PORT}:${TARGET_HOST}:${LOCAL_PORT}.*${REMOTE_HOST}" > /dev/null; then
        echo "  ✓ Tunnel '$TUNNEL_NAME' established successfully"
        TUNNEL_COUNT=$((TUNNEL_COUNT + 1))
    else
        echo "  ✗ Failed to start tunnel '$TUNNEL_NAME'"
        FAILED_TUNNELS="$FAILED_TUNNELS $TUNNEL_NAME"
        echo "  Check log: $TUNNEL_LOG"
    fi
done

echo ""
echo "=========================================="
echo "Started $TUNNEL_COUNT tunnel(s)"
if [ -n "$FAILED_TUNNELS" ]; then
    echo "Failed tunnels:$FAILED_TUNNELS"
    echo "=========================================="
    exit 1
fi
echo "=========================================="
echo ""

# Monitor tunnel health
echo "Monitoring tunnel health..."
echo "Logs directory: $LOG_DIR"
echo "Individual tunnel logs:"
for TUNNEL_NAME in $TUNNEL_NAMES; do
    echo "  - tunnel_${TUNNEL_NAME}.log"
done
echo ""
echo "Use 'docker compose logs -f' or check logs in $LOG_DIR"
echo ""

# Keep container running and monitor tunnel health
while true; do
    sleep 30
    RUNNING_TUNNELS=$(pgrep -f "autossh.*${REMOTE_HOST}" | wc -l)
    if [ "$RUNNING_TUNNELS" -lt "$TUNNEL_COUNT" ]; then
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] Warning: Only $RUNNING_TUNNELS out of $TUNNEL_COUNT tunnels are running"
    fi
    # Exit if all tunnels die
    if [ "$RUNNING_TUNNELS" -eq 0 ]; then
        echo "[$(date +'%Y-%m-%d %H:%M:%S')] Error: No autossh processes found for $REMOTE_HOST"
        echo "All tunnels have stopped. Exiting..."
        exit 1
    fi
done

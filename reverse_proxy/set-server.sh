#!/bin/bash

set -e

# Initialize tput colors (check if terminal supports colors)
if command -v tput >/dev/null 2>&1 && [ -t 1 ]; then
    RED=$(tput setaf 1)
    GREEN=$(tput setaf 2)
    YELLOW=$(tput setaf 3)
    BOLD=$(tput bold)
    RESET=$(tput sgr0)
else
    RED=""
    GREEN=""
    YELLOW=""
    BOLD=""
    RESET=""
fi

# Function to print colored output
print_success() {
    echo "${GREEN}✓${RESET} $1"
}

print_error() {
    echo "${RED}✗${RESET} $1"
}

print_warning() {
    echo "${YELLOW}⚠${RESET} $1"
}

print_info() {
    echo "${BOLD}ℹ${RESET} $1"
}

# Parse command line arguments
SKIP_NGINX=false
for arg in "$@"; do
    case $arg in
        --no-nginx)
            SKIP_NGINX=true
            shift
            ;;
        *)
            print_warning "Unknown option: $arg"
            ;;
    esac
done

# Check if running as root
if [ "$EUID" -ne 0 ]; then 
    print_error "Please run as root (use sudo)"
    exit 1
fi

if command -v tput >/dev/null 2>&1 && [ -t 1 ]; then
    echo "${BOLD}==========================================${RESET}"
    echo "${BOLD}Server Setup for Reverse Proxy${RESET}"
    echo "${BOLD}==========================================${RESET}"
else
    echo "=========================================="
    echo "Server Setup for Reverse Proxy"
    echo "=========================================="
fi
if [ "$SKIP_NGINX" = true ]; then
    print_info "Mode: SSH only (nginx skipped)"
fi
echo ""

# Detect Linux distribution
if [ -f /etc/os-release ]; then
    . /etc/os-release
    OS=$ID
    VER=$VERSION_ID
else
    print_error "Cannot detect Linux distribution"
    exit 1
fi

print_info "Detected OS: $OS $VER"
echo ""

# Step 1: Configure SSH GatewayPorts
if command -v tput >/dev/null 2>&1 && [ -t 1 ]; then
    echo "${BOLD}Step 1:${RESET} Configuring SSH GatewayPorts..."
else
    echo "Step 1: Configuring SSH GatewayPorts..."
fi
SSHD_CONFIG="/etc/ssh/sshd_config"

if [ ! -f "$SSHD_CONFIG" ]; then
    print_error "SSH config file not found at $SSHD_CONFIG"
    exit 1
fi

# Backup original config
if [ ! -f "${SSHD_CONFIG}.backup" ]; then
    cp "$SSHD_CONFIG" "${SSHD_CONFIG}.backup"
    print_success "Created backup of SSH config"
fi

# Set GatewayPorts value based on mode
if [ "$SKIP_NGINX" = true ]; then
    GATEWAY_PORTS_VALUE="yes"
else
    GATEWAY_PORTS_VALUE="clientspecified"
fi

# Check if GatewayPorts is already set
if grep -q "^GatewayPorts" "$SSHD_CONFIG"; then
    # Update existing GatewayPorts setting
    sed -i "s/^GatewayPorts.*/GatewayPorts $GATEWAY_PORTS_VALUE/" "$SSHD_CONFIG"
    print_success "Updated GatewayPorts setting to '$GATEWAY_PORTS_VALUE'"
elif grep -q "^#GatewayPorts" "$SSHD_CONFIG"; then
    # Uncomment and set GatewayPorts
    sed -i "s/^#GatewayPorts.*/GatewayPorts $GATEWAY_PORTS_VALUE/" "$SSHD_CONFIG"
    print_success "Enabled GatewayPorts setting to '$GATEWAY_PORTS_VALUE'"
else
    # Add GatewayPorts setting
    echo "GatewayPorts $GATEWAY_PORTS_VALUE" >> "$SSHD_CONFIG"
    print_success "Added GatewayPorts setting to '$GATEWAY_PORTS_VALUE'"
fi

# Restart SSH service
print_info "Restarting SSH service..."
if systemctl restart sshd 2>/dev/null || systemctl restart ssh 2>/dev/null; then
    print_success "SSH service restarted successfully"
else
    print_error "Failed to restart SSH service"
    exit 1
fi

echo ""

# Step 2: Install nginx (skip if --no-nginx flag is set)
if [ "$SKIP_NGINX" = false ]; then
if command -v tput >/dev/null 2>&1 && [ -t 1 ]; then
    echo "${BOLD}Step 2:${RESET} Installing nginx..."
else
    echo "Step 2: Installing nginx..."
fi

# Check if nginx is already installed
if command -v nginx &> /dev/null; then
    print_warning "Nginx is already installed"
    NGINX_VERSION=$(nginx -v 2>&1 | cut -d'/' -f2)
    print_info "Current version: $NGINX_VERSION"
else
    print_info "Installing nginx..."
    
    case $OS in
        ubuntu|debian)
            apt-get update
            apt-get install -y nginx
            ;;
        centos|rhel|fedora|rocky|almalinux)
            if command -v dnf &> /dev/null; then
                dnf install -y nginx
            else
                yum install -y nginx
            fi
            ;;
        *)
            print_error "Unsupported OS: $OS"
            exit 1
            ;;
    esac
    
    if [ $? -eq 0 ]; then
        print_success "Nginx installed successfully"
    else
        print_error "Failed to install nginx"
        exit 1
    fi
fi

# Enable nginx to start on boot
systemctl enable nginx 2>/dev/null || true
print_success "Nginx enabled to start on boot"

echo ""

# Step 2.5: Configure server_names_hash_bucket_size
if command -v tput >/dev/null 2>&1 && [ -t 1 ]; then
    echo "${BOLD}Step 2.5:${RESET} Configuring server_names_hash_bucket_size..."
else
    echo "Step 2.5: Configuring server_names_hash_bucket_size..."
fi
NGINX_CONF="/etc/nginx/nginx.conf"

if [ ! -f "$NGINX_CONF" ]; then
    print_error "Nginx config file not found at $NGINX_CONF"
    exit 1
fi

# Backup nginx.conf if not already backed up
if [ ! -f "${NGINX_CONF}.backup" ]; then
    cp "$NGINX_CONF" "${NGINX_CONF}.backup"
    print_success "Created backup of nginx.conf"
fi

# Check if server_names_hash_bucket_size is already set in http block
if grep -q "^[[:space:]]*#.*server_names_hash_bucket_size" "$NGINX_CONF"; then
    # Uncomment and set the value
    sed -i 's/^[[:space:]]*#.*server_names_hash_bucket_size.*/    server_names_hash_bucket_size 256;/' "$NGINX_CONF"
    print_success "Uncommented and set server_names_hash_bucket_size to 256"
elif grep -q "^[[:space:]]*server_names_hash_bucket_size" "$NGINX_CONF"; then
    # Update existing uncommented setting
    sed -i 's/^[[:space:]]*server_names_hash_bucket_size.*/    server_names_hash_bucket_size 256;/' "$NGINX_CONF"
    print_success "Updated server_names_hash_bucket_size to 256"
else
    # Add the setting to the http block
    # Find the http block and add the setting after the opening brace
    if grep -q "^http {" "$NGINX_CONF" || grep -q "^http{" "$NGINX_CONF"; then
        # Add after http { line
        sed -i '/^http[[:space:]]*{/a\    server_names_hash_bucket_size 256;' "$NGINX_CONF"
        print_success "Added server_names_hash_bucket_size 256 to http block"
    else
        # Try to find http block with different formatting
        if grep -q "http {" "$NGINX_CONF" || grep -q "http{" "$NGINX_CONF"; then
            # Use awk to add after http block opening
            awk '/http[[:space:]]*{/ {print; print "    server_names_hash_bucket_size 256;"; next}1' "$NGINX_CONF" > "${NGINX_CONF}.tmp" && mv "${NGINX_CONF}.tmp" "$NGINX_CONF"
            print_success "Added server_names_hash_bucket_size 256 to http block"
        else
            print_warning "Could not find http block in nginx.conf, skipping server_names_hash_bucket_size setting"
        fi
    fi
fi

echo ""

# Step 3: Start/Reload nginx
if command -v tput >/dev/null 2>&1 && [ -t 1 ]; then
    echo "${BOLD}Step 3:${RESET} Starting nginx service..."
else
    echo "Step 3: Starting nginx service..."
fi
if systemctl is-active --quiet nginx; then
    systemctl reload nginx
    print_success "Nginx reloaded successfully"
else
    systemctl start nginx
    print_success "Nginx started successfully"
fi

echo ""
fi  # End of nginx section

if command -v tput >/dev/null 2>&1 && [ -t 1 ]; then
    echo "${BOLD}==========================================${RESET}"
    echo "${GREEN}${BOLD}Setup completed successfully!${RESET}"
    echo "${BOLD}==========================================${RESET}"
else
    echo "=========================================="
    echo "Setup completed successfully!"
    echo "=========================================="
fi
echo ""
echo "Summary:"
if [ "$SKIP_NGINX" = true ]; then
    print_success "SSH GatewayPorts configured to 'yes'"
    print_success "SSH service restarted"
else
    print_success "SSH GatewayPorts configured to 'clientspecified'"
    print_success "SSH service restarted"
    print_success "Nginx installed"
    print_success "server_names_hash_bucket_size set to 256"
    print_success "Nginx service started"
fi
echo ""

#!/bin/bash

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_color() {
    printf "${1}${2}${NC}\n"
}

# Function to check if a command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Function to check if a port is in use
port_in_use() {
    lsof -i ":$1" >/dev/null 2>&1
}

# Function to check system resources
check_resources() {
    print_color $BLUE "Checking system resources..."
    
    # Check available disk space
    available_space=$(df -h / | awk 'NR==2 {print $4}')
    print_color $YELLOW "Available disk space: $available_space"
    
    # Check available memory
    available_memory=$(free -h | awk '/^Mem:/ {print $7}')
    print_color $YELLOW "Available memory: $available_memory"
    
    # Check CPU load
    cpu_load=$(uptime | awk -F'load average:' '{ print $2 }' | cut -d, -f1 | sed 's/^ *//g')
    print_color $YELLOW "Current CPU load: $cpu_load"
}

# Function to deploy a component
deploy_component() {
    component=$1
    directory=$2
    
    print_color $BLUE "Deploying $component components..."
    cd "$directory" || { print_color $RED "Failed to navigate to $directory. Exiting."; exit 1; }
    
    if [ ! -f "docker-compose.yml" ]; then
        print_color $RED "docker-compose.yml not found in $directory. Exiting."
        exit 1
    fi
    
    docker-compose up -d
    if [ $? -ne 0 ]; then
        print_color $RED "Failed to deploy $component components. Exiting."
        exit 1
    fi
    
    print_color $GREEN "$component components deployed successfully."
    cd - > /dev/null
}

# Main script execution starts here
print_color $BLUE "Starting Web3DB Deployment Process"

# Check if script is run with sudo
if [ "$EUID" -ne 0 ]; then
    print_color $RED "Please run this script with sudo or as root."
    exit 1
fi

# Check for required commands
for cmd in docker docker-compose lsof df free uptime; do
    if ! command_exists $cmd; then
        print_color $RED "$cmd could not be found. Please install it and try again."
        exit 1
    fi
done

# Check system resources
check_resources

# Check if required ports are available
required_ports=(80 443 4001 8080 8083 5003 9870 9000 9864 10000 9083 8089 3001)
for port in "${required_ports[@]}"; do
    if port_in_use $port; then
        print_color $RED "Port $port is already in use. Please free this port before deploying."
        exit 1
    fi
done

# Deploy DB components
deploy_component "Database" "DB"

# Deploy API components
deploy_component "API" "QueryAPI/QueryAPIKernel"

# Final checks and information
print_color $BLUE "Performing final checks..."

# Wait for services to be fully up (adjust sleep time as needed)
sleep 10

# Check if key services are running
key_services=("ipfs" "namenode" "datanode" "hive-server" "web3db-api")
for service in "${key_services[@]}"; do
    if ! docker ps | grep -q $service; then
        print_color $YELLOW "Warning: $service doesn't seem to be running. Please check logs for more information."
    fi
done

# Print deployment information
print_color $GREEN "\nDeployment completed successfully!"
print_color $YELLOW "Database services are running as defined in DB/docker-compose.yml"
print_color $YELLOW "API should be accessible at: https://api.web3db.org"
print_color $YELLOW "IPFS WebUI should be accessible at: http://localhost:5003/webui"
print_color $YELLOW "Hadoop NameNode UI should be accessible at: http://localhost:9870"
print_color $YELLOW "Hive server is running on port 10000"

print_color $BLUE "\nTo view logs of a specific service, use: docker-compose logs [service_name]"
print_color $BLUE "To stop all services, navigate to each directory and run: docker-compose down"

print_color $GREEN "\nThank you for using Web3DB!"
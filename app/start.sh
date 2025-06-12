#!/bin/bash

# Build and start driver containers
echo "Building and starting Spark driver containers..."
sudo docker compose -f driver.yml build --no-cache
sudo docker compose -f driver.yml up -d

echo "All containers have been built and started successfully."
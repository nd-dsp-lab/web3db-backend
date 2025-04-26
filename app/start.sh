#!/bin/bash

# Build and start spark-master containers
echo "Building and starting Spark master containers..."
sudo docker compose -f spark-master.yml build
sudo docker compose -f spark-master.yml up -d

# Build and start driver containers
echo "Building and starting Spark driver containers..."
sudo docker compose -f driver.yml build
sudo docker compose -f driver.yml up -d

# Build and start worker containers
echo "Building and starting Spark worker containers..."
sudo docker compose -f worker.yml build
sudo docker compose -f worker.yml up -d

echo "All containers have been built and started successfully."
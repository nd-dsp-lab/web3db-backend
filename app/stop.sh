#!/bin/bash

# Stop and remove containers defined in spark-master.yml
echo "Stopping Spark master containers..."
sudo docker compose -f spark-master.yml down

# Stop and remove containers defined in worker.yml
echo "Stopping Spark worker containers..."
sudo docker compose -f worker.yml down

# Stop and remove containers defined in driver.yml
echo "Stopping Spark driver containers..."
sudo docker compose -f driver.yml down

echo "All containers have been stopped successfully."
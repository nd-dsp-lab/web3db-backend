#!/bin/bash

# Stop and remove containers defined in spark-master.yml
echo "Stopping Spark master containers..."
sudo docker compose -f spark-master.yml down

# Stop and remove containers defined in driver.yml
echo "Stopping Spark driver containers..."
sudo docker compose -f driver.yml down
sudo rm -rf data
echo "All containers have been stopped successfully."
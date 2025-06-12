#!/bin/bash

# Build and start FastAPI driver container
echo "Building and starting FastAPI container..."
# sudo docker-compose -f docker-compose.fastapi.yml build --no-cache
sudo docker-compose -f docker-compose.fastapi.yml build
sudo docker-compose -f docker-compose.fastapi.yml up -d


echo "FastAPI container started successfully."
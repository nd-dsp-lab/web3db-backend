#!/bin/bash

# Build and start FastAPI driver container
echo "Stopping FastAPI container..."
sudo docker-compose -f docker-compose.fastapi.yml down


echo "FastAPI container stopped successfully."
#!/bin/bash

# Build and start IPFS container
echo "Building and starting IPFS container..."
# sudo docker-compose -f docker-compose.ipfs.yml build --no-cache
sudo docker-compose -f docker-compose.ipfs.yml build
sudo docker-compose -f docker-compose.ipfs.yml up -d


echo "IPFS container started successfully."
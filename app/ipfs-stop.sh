#!/bin/bash

# Stop and remove IPFS container defined in docker-compose.ipfs.yml
echo "Stopping IPFS container..."
sudo docker-compose -f docker-compose.ipfs.yml down
sudo rm -rf data

echo "IPFS container stopped successfully."
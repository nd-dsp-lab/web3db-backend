#!/bin/bash

echo "Stopping FastAPI/Spark-local (SGX) and IPFS containers..."
sudo docker compose -f driver.yml down --remove-orphans

echo "All relevant containers have been stopped and removed."
# Optional: Clean up volumes if desired, but be careful with data.
# echo "To remove IPFS data volume, run: sudo docker volume rm <projectname>_ipfs_data"
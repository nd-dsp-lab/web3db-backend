# start.sh
#!/bin/bash

# Load environment variables from .env file if they are used by docker-compose
if [ -f .env ]; then
  export $(echo $(cat .env | sed 's/#.*//g'| xargs) | envsubst)
fi

echo "Ensuring IPFS data directory exists..."
mkdir -p ./ipfs_data

echo "Building and starting FastAPI/Spark-local (SGX) and IPFS containers..."
# The build context is defined in driver.yml (./fastapi)
sudo docker compose -f driver.yml build
sudo docker compose -f driver.yml up -d

echo "FastAPI/Spark (SGX) app should be running. Check logs:"
echo "FastAPI/Spark logs: sudo docker logs fastapi-spark-local-sgx -f"
echo "IPFS logs: sudo docker logs ipfs-node -f"
echo "FastAPI accessible at http://<your_host_ip>:8000 (if network_mode:host)"
echo "Spark UI typically at http://<your_host_ip>:${SPARK_UI_PORT:-4040} (if network_mode:host)"
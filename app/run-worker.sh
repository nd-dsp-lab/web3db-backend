HOST_PUBLIC_DNS=$(curl -s ifconfig.me)
sudo docker-compose -f worker.yml up --build -d

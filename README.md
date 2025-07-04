sudo pip3 install -r requirements.txt --break-system-packages

make clean

make SGX=1

sudo gramine-sgx ./python scripts/app.py
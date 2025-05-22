sudo pip3 install sqlalchemy==1.4.46

sudo pip3 install pandasql

make clean

make SGX=1

sudo gramine-sgx ./python scripts/app.py
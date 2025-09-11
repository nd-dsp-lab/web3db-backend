# MtDB Backend Setup Instructions

## Running the System Without SGX

Follow these steps to set up and run the MtDB backend application in a standard (non-SGX) environment:

### Prerequisites
- Python 3.x installed on your system
- Administrative privileges for package installation

### Installation and Setup

1. **Clone the repo and navigate to the application directory:**
    ```bash
    git clone https://github.com/nd-dsp-lab/web3db-backend
    cd web3db-backend/app
    ```

2. **Install Python dependencies:**
   ```bash
   sudo pip3 install -r requirements.txt --break-system-packages
   ```

3. **Navigate to the scripts directory:**
   ```bash
   cd scripts
   ```

4. **Start the application:**
   ```bash
   python3 app.py
   ```

### Verification

Once the application starts successfully, you should see output similar to the following:

```console
shossain@tjws-06:~/web3db-backend/app/scripts$ python3 app.py
INFURA_API_KEY: Present
PRIVATE_KEY: Present
CONTRACT_ADDRESS: Present
Connected to Sepolia network: True
Connected with address: 0x68ef100cC9dAdE0bb67a0aE99A02CDd1eaE54A2f
2025-09-11 15:06:39 [INFO] Smart contract connection initialized successfully
2025-09-11 15:06:39 [INFO] Generated AES-256 encryption key
2025-09-11 15:06:39 [INFO] Initializing DuckDB Connection
2025-09-11 15:06:39 [INFO] DuckDB Connection created
/home/shossain/web3db-backend/app/scripts/app.py:1324: DeprecationWarning: 
        on_event is deprecated, use lifespan event handlers instead.

        Read more about it in the
        [FastAPI docs for Lifespan Events](https://fastapi.tiangolo.com/advanced/events/).
        
  @app.on_event("shutdown")
2025-09-11 15:06:39 [INFO] Starting FastAPI server...
INFO:     Started server process [716386]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
INFO:     Uvicorn running on http://0.0.0.0:8001 (Press CTRL+C to quit)
```
### Accessing the API Documentation

The application provides an interactive API documentation interface via Swagger UI. You can access it at:

**URL:** http://host-ip:8000/docs#

![Swagger UI Interface](images/swagger_ui.png)

### Notes
- Ensure all dependencies are properly installed before running the application
- The application will be accessible on the specified port once started
- Use the Swagger UI to explore and test the available API endpoints

### Additional Resource
**[SGX Setup Instructions](README.md)** - Detailed setup guide for running MtDB node inside gramine/sgx
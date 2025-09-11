### Start Server in Background
```bash
screen -S sgx-app
sudo gramine-sgx ./python scripts/app.py
# Detach: Ctrl+A, then D
```

### Shutdown Server
```bash
# Method 1: Graceful shutdown (Recommended)
screen -r sgx-app    # Attach to session
# Press Ctrl+C       # Stop server
# exit               # Close screen session

# Method 2: Force kill screen session
screen -S sgx-app -X quit

# Method 3: Kill process directly
sudo pkill -f "gramine-sgx"
```

### Monitor Server
```bash
# Check if running
screen -list
ps aux | grep gramine-sgx
sudo netstat -tlnp | grep :8000
```


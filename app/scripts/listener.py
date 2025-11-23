import time
from web3db_contract_index import Web3dbContract


from fastapi import FastAPI
from contextlib import asynccontextmanager
from threading import Lock
import time
from typing import Any, Dict

# 1. Define the Shared State and Lock
# We'll use a simple dictionary to hold the state.
# We wrap it in a class to keep the state and the lock together.
class AppState:
  def __init__(self):
    self.data: Dict[str, Any] = {"status": "initial", "last_update": "never"}
    self.index_storage = Web3dbContract(
      contract_address="0x5FbDB2315678afecb367f032d93F642f64180aa3",
      infura_api_key="eb1d43f1429e49fba50e18fbf5ebd4ab",
      private_key="0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
    )
    # The Lock is the key to thread-safety.
    self.lock = Lock()


  
  def get_state(self):
    # Acquire the lock for reading to ensure consistency
    with self.lock:
      return self.data.copy() # Return a copy to prevent external modification

  def update_state(self):
    print("--- Updating app state in background thread ---")
    print("Listener starting...")
    attributes = ["PatientID", "HospitalID", "Age"]
    with self.lock:
        print(self.index_storage.batch_get_indices(attributes))
    # Acquire the lock for writing to ensure atomicity
    with self.lock:
        
      # Simulate a time-consuming update operation
      # NOTE: If this was an async I/O operation (like a DB query),
      # you'd use a different scheduler/executor. APScheduler's BackgroundScheduler
      # is appropriate for the default sync task execution.
      current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
      self.data["status"] = "updated"
      self.data["last_update"] = current_time
      self.data["counter"] = self.data.get("counter", 0) + 1
      print(f"State updated to: {self.data}")
      print("--------------------------------------------------")

# Create the state object
app_state = AppState()



# 3. Create the FastAPI application with the lifespan event
app = FastAPI()

# 4. Expose the State via an API endpoint (demonstrating access)
@app.get("/state")
def get_current_state():
    return app_state.get_state()
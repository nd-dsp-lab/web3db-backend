#!/usr/bin/env python3
"""
Test script to demonstrate smart contract integration for index CID storage.

This script shows how to:
1. Enable/disable smart contract integration
2. Test both in-memory and smart contract storage modes
3. Verify the integration works correctly
"""

import os
import requests
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# FastAPI server URL
BASE_URL = "http://localhost:8000"

def test_smart_contract_status():
    """Test the smart contract status endpoint."""
    print("=== Testing Smart Contract Status ===")
    try:
        response = requests.get(f"{BASE_URL}/smart-contract-status")
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Smart Contract Enabled: {data.get('smart_contract_enabled')}")
            print(f"Connection Status: {data.get('connection_status')}")
            print(f"Contract Address: {data.get('contract_address')}")
            print(f"Message: {data.get('message')}")
            if 'account_address' in data:
                print(f"Account Address: {data.get('account_address')}")
        else:
            print(f"Error: {response.text}")
    except Exception as e:
        print(f"Error testing smart contract status: {e}")
    print()

def test_get_index_cids():
    """Test getting index CIDs."""
    print("=== Testing Get Index CIDs ===")
    try:
        response = requests.get(f"{BASE_URL}/index-cids")
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Status: {data.get('status')}")
            print(f"Smart Contract Enabled: {data.get('smart_contract_enabled')}")
            print(f"Index CIDs: {data.get('index_cids')}")
            print(f"Index Sizes: {data.get('index_sizes')}")
            print(f"Timestamp: {data.get('timestamp')}")
        else:
            print(f"Error: {response.text}")
    except Exception as e:
        print(f"Error testing get index CIDs: {e}")
    print()

def test_update_index_cids():
    """Test updating index CIDs."""
    print("=== Testing Update Index CIDs ===")
    
    # Test data
    test_cids = {
        "PatientID": "QmTestPatientID123456789abcdefghijklmnopqrstuvwxyz",
        "HospitalID": "QmTestHospitalID123456789abcdefghijklmnopqrstuvw",
        "Age": "QmTestAge123456789abcdefghijklmnopqrstuvwxyzabcdef"
    }
    
    try:
        response = requests.put(
            f"{BASE_URL}/index-cids",
            json={"index_cids": test_cids},
            headers={"Content-Type": "application/json"}
        )
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Status: {data.get('status')}")
            print(f"Message: {data.get('message')}")
            print(f"Smart Contract Enabled: {data.get('smart_contract_enabled')}")
            print(f"Updated CIDs: {data.get('updated_cids')}")
            print(f"Current CIDs: {data.get('current_cids')}")
        else:
            print(f"Error: {response.text}")
    except Exception as e:
        print(f"Error testing update index CIDs: {e}")
    print()

def test_health_check():
    """Test the health check endpoint."""
    print("=== Testing Health Check ===")
    try:
        response = requests.get(f"{BASE_URL}/health")
        print(f"Status Code: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"Status: {data.get('status')}")
        else:
            print(f"Error: {response.text}")
    except Exception as e:
        print(f"Error testing health check: {e}")
    print()

def main():
    """Run all tests."""
    print("Smart Contract Integration Test Suite")
    print("=====================================")
    print()
    
    # Check if server is running
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        if response.status_code != 200:
            print("ERROR: FastAPI server is not running or not healthy!")
            print("Please start the server with: python app.py")
            return
    except Exception as e:
        print("ERROR: Cannot connect to FastAPI server!")
        print(f"Make sure the server is running on {BASE_URL}")
        print(f"Error: {e}")
        return
    
    print("✓ FastAPI server is running")
    print()
    
    # Display current configuration
    print("=== Current Configuration ===")
    print(f"USE_SMART_CONTRACT: {os.getenv('USE_SMART_CONTRACT', 'not set')}")
    print(f"CONTRACT_ADDRESS: {os.getenv('CONTRACT_ADDRESS', 'not set')}")
    print(f"INFURA_API_KEY: {'present' if os.getenv('INFURA_API_KEY') else 'not set'}")
    print(f"PRIVATE_KEY: {'present' if os.getenv('PRIVATE_KEY') else 'not set'}")
    print()
    
    # Run tests
    test_health_check()
    test_smart_contract_status()
    test_get_index_cids()
    test_update_index_cids()
    
    # Get final state
    print("=== Final State ===")
    test_get_index_cids()
    
    print("Test suite completed!")
    print()
    print("To enable smart contract integration:")
    print("1. Set USE_SMART_CONTRACT=true in the .env file")
    print("2. Ensure CONTRACT_ADDRESS, INFURA_API_KEY, and PRIVATE_KEY are set")
    print("3. Restart the FastAPI server")
    print()
    print("To disable smart contract integration:")
    print("1. Set USE_SMART_CONTRACT=false in the .env file")
    print("2. Restart the FastAPI server")

if __name__ == "__main__":
    main()

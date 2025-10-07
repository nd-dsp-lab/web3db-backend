#!/usr/bin/env python3
"""
Debug script for DELETE endpoint
Tests each component step by step
"""

import requests
import json
import time

# Configuration
API_BASE_URL = "http://129.74.154.215:8001"  # Updated to match the actual server
TEST_WALLET = "0x1A28b19f6d2ea1A05F9eFFbcCcbF7E9571877981"

def debug_step1_check_access_policies():
    """Check access policies for the test wallet"""
    print("=== Step 1: Checking Access Policies ===")
    
    try:
        url = f"{API_BASE_URL}/access-policies/{TEST_WALLET}"
        response = requests.get(url)
        print(f"Access policies response status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"Policy count: {result.get('policy_count', 0)}")
            print("Policies:", json.dumps(result.get('policies', []), indent=2))
            return len(result.get('policies', [])) > 0
        else:
            print(f"Failed to get policies: {response.text}")
            return False
    except Exception as e:
        print(f"Error checking policies: {e}")
        return False

def debug_step2_check_existing_data():
    """Check what data exists for PatientID = '1'"""
    print("\n=== Step 2: Checking Existing Data ===")
    
    query_payload = {
        "wallet_address": TEST_WALLET,
        "index_attribute": "PatientID", 
        "query": "SELECT * FROM patient_data WHERE PatientID = '1'"
    }
    
    try:
        response = requests.post(f"{API_BASE_URL}/query", json=query_payload)
        print(f"Query response status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print(f"Records found: {result.get('records', 0)}")
            print(f"CIDs processed: {result.get('cids', 0)}")
            
            if result.get('results'):
                print("Sample records:", json.dumps(result['results'][:2], indent=2))
                return True
            else:
                print("No records found - this explains why DELETE found nothing to delete")
                return False
        else:
            print(f"Query failed: {response.text}")
            return False
    except Exception as e:
        print(f"Query error: {e}")
        return False

def debug_step3_check_indexes():
    """Check the current index status"""
    print("\n=== Step 3: Checking Index Status ===")
    
    try:
        response = requests.get(f"{API_BASE_URL}/index-cids")
        print(f"Index status response: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print("Index CIDs:", json.dumps(result.get('index_cids', {}), indent=2))
            print("Index sizes:", json.dumps(result.get('index_sizes', {}), indent=2))
            
            # Check if indexes are populated
            index_cids = result.get('index_cids', {})
            has_indexes = any(cid is not None for cid in index_cids.values())
            print(f"Has populated indexes: {has_indexes}")
            return has_indexes
        else:
            print(f"Failed to get index status: {response.text}")
            return False
    except Exception as e:
        print(f"Error checking indexes: {e}")
        return False

def debug_step4_simple_delete():
    """Try a simple DELETE operation"""
    print("\n=== Step 4: Simple DELETE Test ===")
    
    delete_payload = {
        "delete_query": "DELETE FROM patient_data WHERE PatientID = '1'",
        "wallet_address": TEST_WALLET
    }
    
    try:
        print("Sending DELETE request...")
        response = requests.post(f"{API_BASE_URL}/delete", json=delete_payload)
        print(f"DELETE response status: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print("DELETE Response:")
            print(json.dumps(result, indent=2))
            
            # Analyze the response
            if result.get('deleted_count', 0) > 0:
                print("✅ DELETE operation was successful!")
                return True
            else:
                print("⚠️ DELETE operation completed but no records were deleted")
                print("Possible reasons:")
                print("- No records match the criteria")
                print("- Access control prevented deletion")
                print("- Records don't exist in accessible data")
                return False
        else:
            print(f"DELETE failed: {response.text}")
            return False
    except Exception as e:
        print(f"DELETE error: {e}")
        return False

def debug_step5_broader_query():
    """Try broader queries to see what data is available"""
    print("\n=== Step 5: Broader Data Check ===")
    
    queries = [
        "SELECT COUNT(*) as total_records FROM patient_data",
        "SELECT DISTINCT PatientID FROM patient_data LIMIT 10",
        "SELECT * FROM patient_data LIMIT 5"
    ]
    
    for query in queries:
        print(f"\nTesting query: {query}")
        query_payload = {
            "wallet_address": TEST_WALLET,
            "index_attribute": "PatientID",
            "query": query
        }
        
        try:
            response = requests.post(f"{API_BASE_URL}/query", json=query_payload)
            if response.status_code == 200:
                result = response.json()
                print(f"Records: {result.get('records', 0)}")
                if result.get('results'):
                    print("Results:", json.dumps(result['results'][:3], indent=2))
            else:
                print(f"Query failed: {response.status_code}")
        except Exception as e:
            print(f"Query error: {e}")

def check_server_health():
    """Check if server is healthy"""
    try:
        response = requests.get(f"{API_BASE_URL}/health")
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Server is healthy: {result.get('message', 'Unknown')}")
            return True
        else:
            print(f"❌ Server health check failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Cannot connect to server: {e}")
        return False

def add_sample_access_policy():
    """Add a sample access policy for testing"""
    print("\n=== Adding Sample Access Policy ===")
    
    policy_payload = {
        "subject_address": TEST_WALLET,
        "object_address": TEST_WALLET,
        "table_name": "patient_data",
        "policy_sql": "SELECT * FROM patient_data"  # Allow access to all data
    }
    
    try:
        response = requests.post(f"{API_BASE_URL}/access-policies", json=policy_payload)
        print(f"Add policy response: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print("Policy added successfully:", json.dumps(result, indent=2))
            return True
        else:
            print(f"Failed to add policy: {response.text}")
            return False
    except Exception as e:
        print(f"Error adding policy: {e}")
        return False

if __name__ == "__main__":
    print("🔍 Debugging DELETE Endpoint")
    print(f"API Base URL: {API_BASE_URL}")
    print(f"Test Wallet: {TEST_WALLET}")
    
    # Check server health
    if not check_server_health():
        print("\n❌ Server is not accessible. Please check if it's running.")
        exit(1)
    
    # Run debug steps
    print("\n" + "="*50)
    
    # Step 1: Check access policies
    has_policies = debug_step1_check_access_policies()
    
    # If no policies, try to add one
    if not has_policies:
        print("\n⚠️ No access policies found. Attempting to add a test policy...")
        add_sample_access_policy()
        time.sleep(1)  # Give it a moment
        has_policies = debug_step1_check_access_policies()
    
    # Step 2: Check existing data  
    has_data = debug_step2_check_existing_data()
    
    # Step 3: Check indexes
    has_indexes = debug_step3_check_indexes()
    
    # Step 4: Try DELETE
    delete_success = debug_step4_simple_delete()
    
    # Step 5: Broader data exploration
    debug_step5_broader_query()
    
    # Summary
    print("\n" + "="*50)
    print("🔍 DEBUG SUMMARY:")
    print(f"✅ Server Health: ✓")
    print(f"✅ Access Policies: {'✓' if has_policies else '❌'}")
    print(f"✅ Data Available: {'✓' if has_data else '❌'}")  
    print(f"✅ Indexes Populated: {'✓' if has_indexes else '❌'}")
    print(f"✅ DELETE Success: {'✓' if delete_success else '❌'}")
    
    if not has_data:
        print("\n💡 RECOMMENDATION:")
        print("The issue appears to be that PatientID = '1' doesn't exist in your dataset.")
        print("Try uploading some data first, or use a PatientID that exists in your data.")
        print("Run: POST /upload/patient-data to upload some test data first.")
    elif not has_policies:
        print("\n💡 RECOMMENDATION:")
        print("No access policies found. Add access policies for your wallet address first.")
    elif not has_indexes:
        print("\n💡 RECOMMENDATION:")
        print("Indexes are not populated. Upload some data to populate the indexes.")
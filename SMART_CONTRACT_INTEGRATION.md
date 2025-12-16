# Smart Contract Integration for Index CID Storage

This document explains how to use the smart contract integration feature for storing and retrieving index CIDs.

## Overview

The application supports two modes for storing index CIDs:

1. **In-Memory Storage** (default): Index CIDs are stored in the application's memory
2. **Smart Contract Storage**: Index CIDs are stored on the Ethereum Sepolia testnet using a smart contract

## Configuration

### Environment Variables

Add these variables to your `.env` file:

```bash
# Smart contract integration (set to "true" to enable, "false" to disable)
USE_SMART_CONTRACT=false

# Web3 configuration (required when USE_SMART_CONTRACT=true)
INFURA_API_KEY=your_infura_api_key_here
PRIVATE_KEY=your_private_key_here
ETHERSCAN_API_KEY=your_etherscan_api_key_here
CONTRACT_ADDRESS=0xe4B4B17AA1Fe9f90fA1521ed87FfcC0f85452F91
```

### Enabling Smart Contract Integration

1. Set `USE_SMART_CONTRACT=true` in your `.env` file
2. Ensure all Web3 configuration variables are properly set
3. Restart the FastAPI application
4. Check the smart contract status using the `/smart-contract-status` endpoint

### Disabling Smart Contract Integration

1. Set `USE_SMART_CONTRACT=false` in your `.env` file
2. Restart the FastAPI application
3. The system will fall back to in-memory storage

## API Endpoints

### Check Smart Contract Status

```bash
GET /smart-contract-status
```

Returns the current status of the smart contract integration:

```json
{
  "smart_contract_enabled": true,
  "connection_status": "connected",
  "contract_address": "0xe4B4B17AA1Fe9f90fA1521ed87FfcC0f85452F91",
  "account_address": "0x...",
  "network": "Sepolia",
  "message": "Smart contract connection is active"
}
```

### Get Index CIDs

```bash
GET /index-cids
```

Returns index CIDs from either smart contract or in-memory storage:

```json
{
  "status": "success",
  "index_cids": {
    "PatientID": "QmXxxxxx...",
    "HospitalID": "QmYyyyy...",
    "Age": "QmZzzzz..."
  },
  "index_sizes": {
    "PatientID": 12345,
    "HospitalID": 23456,
    "Age": 34567
  },
  "smart_contract_enabled": true,
  "timestamp": "2025-08-12 10:30:45"
}
```

### Update Index CIDs

```bash
PUT /index-cids
```

Updates index CIDs in either smart contract or in-memory storage:

```json
{
  "index_cids": {
    "PatientID": "QmNewPatientID...",
    "HospitalID": "QmNewHospitalID...",
    "Age": "QmNewAge..."
  }
}
```

Response:

```json
{
  "status": "success",
  "message": "Index CIDs updated successfully in smart contract",
  "updated_cids": {
    "PatientID": "QmNewPatientID...",
    "HospitalID": "QmNewHospitalID...",
    "Age": "QmNewAge..."
  },
  "current_cids": {
    "PatientID": "QmNewPatientID...",
    "HospitalID": "QmNewHospitalID...",
    "Age": "QmNewAge..."
  },
  "smart_contract_enabled": true
}
```

## Automatic Integration in Upload and Query Operations

### Upload API (`POST /upload/patient-data`)

When smart contract integration is enabled:
1. The index CIDs are automatically stored in the smart contract after successful index creation
2. The response includes the updated index CIDs from the smart contract
3. If smart contract storage fails, the system falls back to in-memory storage

### Query API (`POST /query`)

When smart contract integration is enabled:
1. Index CIDs are automatically retrieved from the smart contract
2. If smart contract retrieval fails, the system falls back to in-memory storage
3. The query proceeds normally with the retrieved CIDs

## Error Handling and Fallback

The system is designed with robust error handling:

1. **Initialization Failure**: If smart contract connection fails during startup, the system automatically falls back to in-memory storage
2. **Runtime Failures**: If smart contract operations fail during runtime, the system falls back to in-memory storage and logs the error
3. **Network Issues**: If network connectivity is lost, the system continues to work with cached in-memory data

## Testing

Use the provided test script to verify the integration:

```bash
python test_smart_contract_integration.py
```

The test script will:
- Check server connectivity
- Test smart contract status
- Test getting and updating index CIDs
- Verify the integration works correctly

## Smart Contract Functions Used

The integration uses these smart contract functions:

- `updateIndexCID(string attribute, string newCID)`: Update a single index CID
- `batchUpdateIndexCIDs(string[] attributes, string[] newCIDs)`: Update multiple index CIDs
- `getIndexCID(string attribute)`: Get a single index CID
- `batchGetIndexCIDs(string[] attributes)`: Get multiple index CIDs

## Security Considerations

1. **Private Key**: Store your private key securely and never commit it to version control
2. **Network**: The integration uses Sepolia testnet for development/testing
3. **Gas Costs**: Smart contract operations require ETH for gas fees
4. **Backup**: In-memory storage serves as a backup when smart contract operations fail

## Troubleshooting

### Common Issues

1. **"Smart contract connection failed"**
   - Check your INFURA_API_KEY is valid
   - Ensure CONTRACT_ADDRESS is correct
   - Verify PRIVATE_KEY has sufficient ETH for gas

2. **"Failed to update index CID in smart contract"**
   - Check network connectivity
   - Ensure your account has sufficient ETH
   - Verify the contract address is correct

3. **"Connection status: disconnected"**
   - Check Infura service status
   - Verify network connectivity
   - Ensure API key limits aren't exceeded

### Debug Information

Check the application logs for detailed error messages:
- Smart contract initialization status
- Individual transaction hashes
- Error details for failed operations

The system logs all smart contract operations for debugging and monitoring purposes.

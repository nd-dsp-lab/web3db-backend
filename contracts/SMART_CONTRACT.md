# Smart Contract Documentation

## Overview

This smart contract (Web3dbContract) manages index CIDs and access policies for the Web3DB system. It provides functions to store, retrieve, update, and batch-manage index CIDs, table schemas, and access policies on the Ethereum Sepolia testnet.

## Contract Features

- **Single Index Management**: Store and retrieve individual index CIDs
- **Batch Operations**: Update multiple index CIDs in a single transaction
- **Event Logging**: Emit events for all index updates
- **Gas Optimization**: Batch operations reduce gas costs compared to individual updates

## Setup and Installation

### Prerequisites

- Node.js (v18 or higher recommended, v16.20.2+ minimum)
- npm or yarn
- Metamask or similar wallet
- Sepolia testnet ETH for gas fees

**Note**: Hardhat recommends Node.js v18+. If using v16, you may see warnings but functionality should work.

### Install Dependencies

```bash
cd contracts
npm install --save-dev hardhat @nomicfoundation/hardhat-toolbox dotenv
```

### Environment Configuration

Create a `.env` file in the contracts directory:

```bash
# Sepolia network configuration
INFURA_API_KEY=your_infura_api_key_here
PRIVATE_KEY=your_private_key_here
ETHERSCAN_API_KEY=your_etherscan_api_key_here

# Contract deployment
CONTRACT_ADDRESS=0xe4B4B17AA1Fe9f90fA1521ed87FfcC0f85452F91
```

## Deployment

### Deploy to Sepolia Testnet

```bash
cd contracts
npx hardhat run scripts/deploy.js --network sepolia
```

### Verify Contract on Etherscan

```bash
npx hardhat verify --network sepolia <CONTRACT_ADDRESS>
```

Replace `<CONTRACT_ADDRESS>` with the actual deployed contract address.

## Contract Functions

### Read Functions (View)

#### `getIndexCID(string attribute)`
- **Description**: Get the CID for a specific attribute
- **Parameters**: `attribute` - The attribute name (e.g., "PatientID")
- **Returns**: The CID string for the attribute
- **Gas**: No gas cost (view function)

#### `batchGetIndexCIDs(string[] attributes)`
- **Description**: Get CIDs for multiple attributes in a single call
- **Parameters**: `attributes` - Array of attribute names
- **Returns**: Array of CID strings corresponding to the attributes
- **Gas**: No gas cost (view function)

### Write Functions (State-changing)

#### `updateIndexCID(string attribute, string newCID)`
- **Description**: Update the CID for a single attribute
- **Parameters**: 
  - `attribute` - The attribute name
  - `newCID` - The new CID value
- **Events**: Emits `IndexUpdated(attribute, oldCID, newCID)`
- **Gas**: ~50,000-70,000 gas

#### `batchUpdateIndexCIDs(string[] attributes, string[] newCIDs)`
- **Description**: Update CIDs for multiple attributes in a single transaction
- **Parameters**: 
  - `attributes` - Array of attribute names
  - `newCIDs` - Array of corresponding new CID values
- **Events**: 
  - Emits `BatchIndexUpdated(attributes, newCIDs)`
  - Emits individual `IndexUpdated` events for each attribute
- **Gas**: ~150,000-250,000 gas (more efficient than multiple single updates)

#### `removeIndex(string attribute)`
- **Description**: Remove an index (sets CID to empty string)
- **Parameters**: `attribute` - The attribute name to remove
- **Events**: Emits `IndexUpdated(attribute, oldCID, "")`
- **Gas**: ~40,000-60,000 gas

## Events

### `IndexUpdated`
```solidity
event IndexUpdated(string attribute, string oldCID, string newCID);
```
- Emitted when a single index is updated or removed
- Contains the attribute name, old CID, and new CID

### `BatchIndexUpdated`
```solidity
event BatchIndexUpdated(string[] attributes, string[] newCIDs);
```
- Emitted when multiple indices are updated in a batch operation
- Contains arrays of all attributes and their new CIDs

## Integration with Web3DB

### Application Configuration

The Web3DB application integrates with this contract through environment variables:

```bash
# Smart contract integration (set to "true" to enable, "false" to disable)
USE_SMART_CONTRACT=true

# Web3 configuration
INFURA_API_KEY=your_infura_api_key_here
PRIVATE_KEY=your_private_key_here
CONTRACT_ADDRESS=0xe4B4B17AA1Fe9f90fA1521ed87FfcC0f85452F91
```

### Usage in Upload Process

When uploading patient data:
1. Indices are built and uploaded to IPFS
2. **Single batch call** updates all index CIDs in the smart contract
3. Gas-efficient: 1 transaction instead of 3 individual transactions

### Usage in Query Process

When querying data:
1. Index CIDs are retrieved from the smart contract
2. Fallback to in-memory storage if smart contract fails
3. Queries proceed with retrieved CIDs

## Gas Optimization

### Individual vs Batch Updates

- **Individual Updates**: 3 separate transactions
  - PatientID: ~50,000 gas
  - HospitalID: ~50,000 gas  
  - Age: ~50,000 gas
  - **Total**: ~150,000 gas + 3× base transaction cost

- **Batch Update**: 1 transaction
  - All indices: ~200,000 gas
  - **Total**: ~200,000 gas + 1× base transaction cost
  - **Savings**: ~30-40% gas reduction

### Best Practices

1. **Use Batch Operations**: Always prefer `batchUpdateIndexCIDs` over multiple `updateIndexCID` calls
2. **Check Gas Prices**: Monitor Sepolia gas prices before deployment
3. **Error Handling**: Implement proper fallback mechanisms
4. **Event Monitoring**: Listen to events for transaction confirmation

## Testing

### Local Testing

Run the comprehensive test suite:

```bash
cd contracts
npx hardhat test
```

### Network Testing

#### Check Account Balance and Network Status

```bash
npx hardhat run scripts/check-balance.js --network sepolia
```

This script will:
- Display your account address and balance
- Check if you're connected to Sepolia testnet
- Show current gas prices
- Estimate transaction costs
- Warn if balance is too low

#### Test Contract Functions on Sepolia

```bash
npx hardhat run scripts/test-contract.js --network sepolia
```

This script will:
- Test single index updates
- Test batch index updates
- Compare gas usage between single vs batch operations
- Verify event emissions
- Show gas savings from batch operations

### Available Test Scripts

1. **`test/IndexCIDContract.test.js`** - Comprehensive unit tests
2. **`scripts/check-balance.js`** - Account and network status check
3. **`scripts/test-contract.js`** - Live contract testing on Sepolia
4. **`scripts/deploy.js`** - Contract deployment script

## Troubleshooting

### Common Issues

1. **"Transaction failed"**
   - Check account has sufficient ETH for gas
   - Verify contract address is correct
   - Ensure network connectivity

2. **"Contract not found"**
   - Verify CONTRACT_ADDRESS in .env
   - Check you're connected to Sepolia network
   - Confirm contract is deployed and verified

3. **"Gas estimation failed"**
   - Increase gas limit in transaction
   - Check for contract function errors
   - Verify input parameters are valid

4. **Node.js version warnings**
   - Hardhat recommends Node.js v18+
   - Warnings with v16 are generally safe to ignore
   - Consider upgrading to Node.js v18+ for best experience

### Debug Commands

#### Check Contract Deployment Status
```bash
npx hardhat verify --network sepolia <CONTRACT_ADDRESS>
```

#### Check Account Balance and Network
```bash
npx hardhat run scripts/check-balance.js --network sepolia
```

#### Test Contract Functions
```bash
npx hardhat run scripts/test-contract.js --network sepolia
```

#### Run Local Tests
```bash
npx hardhat test
```

## Security Considerations

1. **Private Key Management**: Never commit private keys to version control
2. **Access Control**: Contract currently has no access control (public functions)
3. **Input Validation**: Validate CID formats in application layer
4. **Gas Limits**: Set appropriate gas limits to prevent DoS attacks

## Contract Address

**Sepolia Testnet**: `0x041da68BD3F1bf13C5d75E3bA80ab6bB8B136BFd`

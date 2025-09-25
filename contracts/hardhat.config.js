require("@nomicfoundation/hardhat-toolbox");
require('dotenv').config();

// Add debugging
console.log('Environment variables loaded:');
console.log('INFURA_API_KEY:', process.env.INFURA_API_KEY ? 'Present' : 'Missing');
console.log('PRIVATE_KEY:', process.env.PRIVATE_KEY ? 'Present' : 'Missing');
console.log('ETHERSCAN_API_KEY:', process.env.ETHERSCAN_API_KEY ? 'Present' : 'Missing');

// Validate private key format
function validatePrivateKey(privateKey) {
  if (!privateKey) return false;
  // Remove 0x prefix if present
  const cleanKey = privateKey.startsWith('0x') ? privateKey.slice(2) : privateKey;
  // Check if it's 64 characters (32 bytes in hex)
  return cleanKey.length === 64 && /^[0-9a-fA-F]+$/.test(cleanKey);
}

// Build networks configuration
const networks = {
  hardhat: {
    // This is important for running local tests
    chainId: 31337
  },
  localhost: {
    url: "http://localhost:8545",
    chainId: 31337,
    // Use the same accounts as hardhat for consistency
    accounts: [
      "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",
      "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d",
      "0x5de4111daa5ba4e5b4a4d238ff944bacb478cbed5efcae784d7bf4f2ff80ac97"
    ]
  }
};

// Only add sepolia network if we have valid credentials
if (process.env.INFURA_API_KEY && process.env.PRIVATE_KEY && validatePrivateKey(process.env.PRIVATE_KEY)) {
  networks.sepolia = {
    url: `https://sepolia.infura.io/v3/${process.env.INFURA_API_KEY}`,
    accounts: [process.env.PRIVATE_KEY.startsWith('0x') ? process.env.PRIVATE_KEY : `0x${process.env.PRIVATE_KEY}`],
    chainId: 11155111
  };
  console.log('✅ Sepolia network configured');
} else {
  console.log('⚠️  Sepolia network not configured - missing or invalid credentials');
  if (process.env.PRIVATE_KEY && !validatePrivateKey(process.env.PRIVATE_KEY)) {
    console.log('❌ Private key format is invalid. Expected 64 hex characters (32 bytes)');
  }
}

/** @type import('hardhat/config').HardhatUserConfig */
module.exports = {
  solidity: {
    version: "0.8.28",
    settings: {
      optimizer: {
        enabled: true,
        runs: 200
      }
    }
  },
  networks,
  etherscan: {
    apiKey: process.env.ETHERSCAN_API_KEY
  },
  paths: {
    sources: "./contracts",
    tests: "./test",
    cache: "./cache",
    artifacts: "./artifacts"
  }
};
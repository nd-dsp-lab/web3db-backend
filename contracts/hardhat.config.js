require("@nomicfoundation/hardhat-toolbox");
require('dotenv').config();

// Add debugging
console.log('Environment variables loaded:');
console.log('INFURA_API_KEY:', process.env.INFURA_API_KEY ? 'Present' : 'Missing');
console.log('PRIVATE_KEY:', process.env.PRIVATE_KEY ? 'Present' : 'Missing');
console.log('ETHERSCAN_API_KEY:', process.env.ETHERSCAN_API_KEY ? 'Present' : 'Missing');

/** @type import('hardhat/config').HardhatUserConfig */
module.exports = {
  solidity: {
    version: "0.8.28",
    settings: {
      optimizer: { enabled: true, runs: 200 },
    },
  },
  networks: {
    hardhat: {},
    ...(process.env.INFURA_API_KEY && process.env.PRIVATE_KEY ? {
      sepolia: {
        url: `https://sepolia.infura.io/v3/${process.env.INFURA_API_KEY}`,
        accounts: [`0x${process.env.PRIVATE_KEY}`]
      }
    } : {})
  },
  etherscan: {
    apiKey: process.env.ETHERSCAN_API_KEY
  }
};
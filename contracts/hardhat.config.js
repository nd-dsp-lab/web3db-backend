require("@nomicfoundation/hardhat-toolbox");
require('dotenv').config();

// Add debugging
console.log('Environment variables loaded:');
console.log('INFURA_API_KEY:', process.env.INFURA_API_KEY ? 'Present' : 'Missing');
console.log('PRIVATE_KEY:', process.env.PRIVATE_KEY ? 'Present' : 'Missing');
console.log('ETHERSCAN_API_KEY:', process.env.ETHERSCAN_API_KEY ? 'Present' : 'Missing');

/** @type import('hardhat/config').HardhatUserConfig */
module.exports = {
  solidity: "0.8.28",
  networks: {
    hardhat: {},  // This is important for running local tests
    sepolia: {
      url: `https://sepolia.infura.io/v3/${process.env.INFURA_API_KEY}`,
      accounts: [`0x${process.env.PRIVATE_KEY}`]  // Note the '0x' prefix
    }
  },
  etherscan: {
    apiKey: process.env.ETHERSCAN_API_KEY
  }
};
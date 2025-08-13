Install hardhat dependencies
    npm install --save-dev hardhat @nomicfoundation/hardhat-toolbox dotenv


Deploy
cd contracts
    npx hardhat run scripts/deploy.js --network sepolia

Verify
   npx hardhat verify --network sepolia <CONTRACT_ADDRESS>
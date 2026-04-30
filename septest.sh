cd ~/develop/Github/web3db-backend/contracts
NODE=/home/shady/.nvm/versions/node/v24.14.0/bin/node
$NODE node_modules/.bin/hardhat test test/Web3dbContract.sepolia.test.js --network sepolia

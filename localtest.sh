cd ~/develop/Github/web3db-backend/contracts                                                                                                                
NODE=/home/shady/.nvm/versions/node/v24.14.0/bin/node                                                                                                       
PRIVATE_KEY=0000000000000000000000000000000000000000000000000000000000000001  
$NODE node_modules/.bin/hardhat test test/Web3dbContract.test.js --network hardhat 

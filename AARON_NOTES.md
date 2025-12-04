# Start Testing
### Open 5 Terminals
1. Start HardHat
```
cd contracts
npx hardhat clean
npx hardhat compile
npx hardhat node
```
2. Start Local Host hardhat
```
cd contracts
npx hardhat ignition deploy ignition/modules/Web3dbModule.js --network localhost
```
3. Start IPFS container
```
cd app/ipfs
docker-compose up
```
4. Start Server
```
cd app/scripts
python3 app.py
```
5. Run Commands


### Important edits

- delta_index.py: main code
- web3db_contract_index.py: testing contracts api
- Web3dbContract.sol: updating contracts api

### Index Updating

Requirements: Needs to have background task that updates the index so queries can happen quickly and efficiently

Run a thread that has a polling loop (maybe per minute?) which checks smart contract. locks index, updates index, unlocks index sleep...

Tasks

1. Find Thread + Lock Library
2. Integrate method to spin wait? whats a good way


### Implemented

1. used threading to create background thread that updates index every `INDEX_UPDATE_INTERVAL` seconds
2. lock upon update and lock upon query
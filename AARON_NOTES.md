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


### Notes on what to implement

#### Delta file shema
```
prev delta
change 1
change 2
change 3
...
```

#### Pseudocode
```
DELTA_LAST = <CID>

def uploadData(data):
  # do i need to set lock from here
  getIndex()
  age_set = set(data['age])
  CID = IPFS_upload(data)
  prev_delta = smart_contract_get_age_index()
  with open("delta_file", "w") as tmp:
    tmp.write(prev_delta)
    for age in age_set:
      age_index[age].add(CID)
      tmp.write((age, CID))

  delta_CID = IPFS_upload("delta_file")
  smart_contract_update_age_index()
  # to here ???
  
def getIndex():
  tail_delta = smart_contract_get_age_index()
  delta = tail_delta
  while delta != DELTA_LAST:
    delta_file = IPFS_download(delta)
    prev_delta, changes = delta_file.parsed()
    for age, CID in changes:
      age_index[age].add(CID)
    delta = prev_delta
  DELTA_LAST = tail_delta


```

### Important edits

- delta_index.py: main code
- web3db_contract_index.py: testing contracts api
- Web3dbContract.sol: updating contracts api
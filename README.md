# Web3DB Backend

FastAPI server for Web3DB.

## Run

Prereqs: Python 3.12, Docker, `Web3dbContract` deployed on a Hardhat node or Sepolia Testnet.

Start the IPFS daemon (in a separate terminal):

```bash
cd app/ipfs
docker compose up -d                                   # Linux (host network)
docker compose -f docker-compose.mac.yml up -d         # macOS (port mapping)
```

Run the server:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp app/scripts/.env.example app/scripts/.env   # fill RPC_URL, PRIVATE_KEY, CONTRACT_ADDRESS, ENCRYPTION_KEY
./start_server.sh                              # tmux session; logs in logs/
# or directly:
cd app/scripts && python3 -u app.py            # http://localhost:8000
```

To deploy / redeploy the smart contract, see `contracts/SMART_CONTRACT.md`.

## Structure

```
app/scripts/
  app.py                  FastAPI app, /upload, /query, /schemas, /tables/{t}/owner-count, ...
  web3db_contract.py      Web3 client + ABI for Web3dbContract
  web3db_controller.py    sub-router
  cidindex.py, bplustree.py, trie.py   index data structures
  crypto_utils.py         AES-256 packaging for IPFS payloads
  ipfs_utils.py           IPFS add/get helpers
  audit_*.py              request audit logging
contracts/                Hardhat project (Web3dbContract.sol, deploy scripts)
reverse_proxy/, ngrok/    deployment helpers
sample_data/              test CSVs
logs/                     server logs
start_server.sh           tmux launcher (per-branch session)
view_logs.sh              tail current branch's log
```

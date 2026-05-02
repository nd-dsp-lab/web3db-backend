# Web3dbContract — Deploy Guide

Deploy the `Web3dbContract` to a local Hardhat node or to Sepolia.

## Prereqs

- Node.js v18+
- `cd contracts && npm install`
- `.env` in `contracts/` with:
  ```
  INFURA_API_KEY=...        # sepolia only
  PRIVATE_KEY=...           # no 0x prefix
  ETHERSCAN_API_KEY=...     # sepolia verify only
  ```

## Localhost

In one terminal:
```bash
cd contracts
npx hardhat node
```

In another:
```bash
npx hardhat run scripts/deploy.js --network localhost
```

Copy the printed address into the backend `.env`:
```
CONTRACT_ADDRESS=<deployed_address>
RPC_URL=http://127.0.0.1:8545
```

The Hardhat node funds account #0 automatically; use its private key for `PRIVATE_KEY` in the backend `.env`.

## Sepolia

```bash
cd contracts
npx hardhat run scripts/deploy.js --network sepolia
```

Verify on Etherscan:
```bash
npx hardhat verify --network sepolia <CONTRACT_ADDRESS>
```

Backend `.env`:
```
CONTRACT_ADDRESS=<deployed_address>
RPC_URL=                            # leave blank to use Infura
INFURA_API_KEY=<your_key>
PRIVATE_KEY=<deployer_key>
```

## After redeploy

Redeploying yields a new address; previous on-chain state (schemas, indexes, policies, owners) is unreachable. Update `CONTRACT_ADDRESS` in the backend `.env` and restart the backend. Re-upload tables to repopulate.

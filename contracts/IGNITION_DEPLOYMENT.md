# Web3DB Contract - Ignition Deployment Guide

This guide explains how to deploy and test the Web3DB contract using Hardhat Ignition on the local Hardhat network.

## Prerequisites

Make sure you have the following installed:
- Node.js (v16 or higher)
- npm or yarn
- hardhat

## Start the local hardhat node for testing

- starting the node and do not stop the terminal 
```
npx hardhat node
```

- from a new terminal, deploy the contract using ignition module to the localhost network

```
npx hardhat ignition deploy ignition/modules/Web3dbModule.js --network localhost
```

Information will be prompted to the hardhat node window


The app.py should be good to run after the ipfs container is started.

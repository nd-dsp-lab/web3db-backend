const { buildModule } = require("@nomicfoundation/hardhat-ignition/modules");

module.exports = buildModule("Web3dbModule", (m) => {
  // Deploy the Web3dbContract
  const web3dbContract = m.contract("Web3dbContract");

  return { web3dbContract };
});

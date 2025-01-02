async function main() {
  console.log("Deploying Web3DBHashManagement...");

  const Contract = await ethers.getContractFactory("Web3DBHashManagement");
  const contract = await Contract.deploy();

  // Wait for deployment to complete
  await contract.waitForDeployment();

  // Get the deployed contract address
  const address = await contract.getAddress();
  console.log("Contract deployed to:", address);
}

main()
  .then(() => process.exit(0))
  .catch((error) => {
    console.error(error);
    process.exit(1);
  });
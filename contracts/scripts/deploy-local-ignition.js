const { ignition } = require("hardhat");
const Web3dbModule = require("../ignition/modules/Web3dbModule");

async function main() {
  console.log("🚀 Starting Web3DB Contract deployment using Ignition...");
  console.log("📍 Network: Local Hardhat Network");
  
  try {
    // Deploy the contract using Ignition
    const { web3dbContract } = await ignition.deploy(Web3dbModule);
    
    // Wait for deployment to complete
    await web3dbContract.waitForDeployment();
    
    // Get the deployed contract address
    const contractAddress = await web3dbContract.getAddress();
    
    console.log("✅ Web3DB Contract deployed successfully!");
    console.log("📍 Contract Address:", contractAddress);
    console.log("🔗 Network: Local Hardhat Network");
    
    // Verify the contract is working by calling a view function
    console.log("\n🧪 Testing contract functionality...");
    
    // Test getting an index CID (should return empty string for new contract)
    const testAttribute = "TestAttribute";
    const indexCID = await web3dbContract.getIndexCID(testAttribute);
    console.log(`📊 Test getIndexCID("${testAttribute}"):`, indexCID);
    
    // Test getting a table schema (should return empty string for new contract)
    const testTable = "TestTable";
    const tableSchema = await web3dbContract.getTableSchema(testTable);
    console.log(`📋 Test getTableSchema("${testTable}"):`, tableSchema);
    
    // Test getting policy count (should return 0 for new contract)
    const deployerAddress = await web3dbContract.runner.getAddress();
    const policyCount = await web3dbContract.getPolicyCount(deployerAddress);
    console.log(`🔐 Test getPolicyCount("${deployerAddress}"):`, policyCount.toString());
    
    console.log("\n🎉 Contract deployment and basic testing completed successfully!");
    console.log("\n📝 Next steps:");
    console.log("   1. Use this contract address in your application");
    console.log("   2. Run tests with: npm run test:local");
    console.log("   3. Interact with the contract using the provided scripts");
    
    return {
      contractAddress,
      contract: web3dbContract
    };
    
  } catch (error) {
    console.error("❌ Deployment failed:", error);
    throw error;
  }
}

// Execute the deployment
main()
  .then(() => {
    console.log("\n✨ Deployment script completed successfully!");
    process.exit(0);
  })
  .catch((error) => {
    console.error("\n💥 Deployment script failed:", error);
    process.exit(1);
  });

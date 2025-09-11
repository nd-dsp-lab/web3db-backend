const { ethers } = require("hardhat");

async function main() {
    console.log("Deploying IndexState contract with Schema Management...\n");

    // Get the deployer account
    const [deployer] = await ethers.getSigners();
    console.log("Deploying with account:", deployer.address);

    // Check account balance
    const balance = await ethers.provider.getBalance(deployer.address);
    let balanceFormatted;
    try {
        balanceFormatted = ethers.formatEther(balance);
    } catch (error) {
        balanceFormatted = ethers.utils.formatEther(balance);
    }
    console.log("Account balance:", balanceFormatted, "ETH\n");

    // Deploy the contract
    console.log("Deploying IndexState contract...");
    const IndexState = await ethers.getContractFactory("IndexState");
    const contract = await IndexState.deploy();

    // Handle deployment waiting for both ethers v5 and v6
    let deploymentReceipt;
    try {
        deploymentReceipt = await contract.deployed(); // v5 syntax
    } catch (error) {
        deploymentReceipt = await contract.waitForDeployment(); // v6 syntax
    }

    // Get contract address with fallback methods
    let contractAddress;
    if (contract.address) {
        contractAddress = contract.address; // v5
    } else if (contract.target) {
        contractAddress = contract.target; // v6
    } else {
        // Fallback: get from deployment transaction
        const deployTx = contract.deploymentTransaction();
        if (deployTx) {
            const receipt = await deployTx.wait();
            contractAddress = receipt.contractAddress;
        }
    }

    console.log("✅ IndexState contract deployed to:", contractAddress);

    // Get network information
    const network = await ethers.provider.getNetwork();
    let chainId;
    if (typeof network.chainId === 'bigint') {
        chainId = network.chainId;
    } else {
        chainId = BigInt(network.chainId);
    }

    console.log("Network:", network.name);
    console.log("Chain ID:", chainId.toString());

    // Test basic functionality
    console.log("\n=== Testing Basic Functionality ===");

    // Test index functionality
    console.log("Testing index update...");
    const testCID = "QmTestCID123456789abcdefghijklmnopqrstuvwxyz";
    const tx1 = await contract.updateIndexCID("PatientID", testCID);
    const receipt1 = await tx1.wait();
    console.log(`Index update gas used: ${receipt1.gasUsed.toString()}`);

    const retrievedCID = await contract.getIndexCID("PatientID");
    console.log(`Retrieved CID: ${retrievedCID}`);
    console.log(`Index test successful: ${retrievedCID === testCID}`);

    // Test schema functionality
    console.log("\nTesting schema update...");
    const testSchema = JSON.stringify({
        table_name: "patient_data",
        columns: [
            { name: "PatientID", type: "string" },
            { name: "Age", type: "integer" }
        ],
        created_at: new Date().toISOString()
    });

    const tx2 = await contract.updateTableSchema("patient_data", testSchema);
    const receipt2 = await tx2.wait();
    console.log(`Schema update gas used: ${receipt2.gasUsed.toString()}`);

    const retrievedSchema = await contract.getTableSchema("patient_data");
    console.log(`Schema test successful: ${retrievedSchema === testSchema}`);

    console.log("\n✅ Deployment and testing completed successfully!");
    console.log("\n📝 Update your .env file with the new contract address:");
    console.log(`CONTRACT_ADDRESS=${contractAddress}`);
}

main()
    .then(() => process.exit(0))
    .catch((error) => {
        console.error("❌ Deployment failed:", error);
        process.exit(1);
    });

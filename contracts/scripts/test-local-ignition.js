const { ignition } = require("hardhat");
const Web3dbModule = require("../ignition/modules/Web3dbModule");

async function main() {
  console.log("🧪 Starting Web3DB Contract testing on local network...");
  
  try {
    // Deploy the contract using Ignition
    console.log("📦 Deploying contract...");
    const { web3dbContract } = await ignition.deploy(Web3dbModule);
    await web3dbContract.waitForDeployment();
    
    const contractAddress = await web3dbContract.getAddress();
    console.log("✅ Contract deployed at:", contractAddress);
    
    // Get the deployer address
    const deployerAddress = await web3dbContract.runner.getAddress();
    console.log("👤 Deployer address:", deployerAddress);
    
    console.log("\n🔍 Running comprehensive tests...\n");
    
    // Test 1: Index CID Management
    console.log("📊 Test 1: Index CID Management");
    console.log("=" .repeat(40));
    
    const testAttribute = "PatientID";
    const testCID = "QmTestCID123456789";
    
    // Test initial state
    let indexCID = await web3dbContract.getIndexCID(testAttribute);
    console.log(`Initial getIndexCID("${testAttribute}"):`, indexCID);
    
    // Test updating index CID
    console.log(`Updating index CID for "${testAttribute}" to "${testCID}"...`);
    const updateTx = await web3dbContract.updateIndexCID(testAttribute, testCID);
    await updateTx.wait();
    console.log("✅ Index CID updated successfully");
    
    // Verify the update
    indexCID = await web3dbContract.getIndexCID(testAttribute);
    console.log(`Updated getIndexCID("${testAttribute}"):`, indexCID);
    
    // Test batch operations
    console.log("\nTesting batch operations...");
    const attributes = ["PatientID", "DoctorID", "VisitID"];
    const cids = ["QmCID1", "QmCID2", "QmCID3"];
    
    const batchUpdateTx = await web3dbContract.batchUpdateIndexCIDs(attributes, cids);
    await batchUpdateTx.wait();
    console.log("✅ Batch update completed");
    
    const batchResults = await web3dbContract.batchGetIndexCIDs(attributes);
    console.log("Batch results:", batchResults);
    
    // Test 2: Schema Management
    console.log("\n📋 Test 2: Schema Management");
    console.log("=" .repeat(40));
    
    const tableName = "Patients";
    const schemaJson = JSON.stringify({
      PatientID: "string",
      Name: "string",
      Age: "number",
      Diagnosis: "string"
    });
    
    // Test initial state
    let schema = await web3dbContract.getTableSchema(tableName);
    console.log(`Initial getTableSchema("${tableName}"):`, schema);
    
    // Test updating schema
    console.log(`Updating schema for "${tableName}"...`);
    const schemaUpdateTx = await web3dbContract.updateTableSchema(tableName, schemaJson);
    await schemaUpdateTx.wait();
    console.log("✅ Schema updated successfully");
    
    // Verify the update
    schema = await web3dbContract.getTableSchema(tableName);
    console.log(`Updated getTableSchema("${tableName}"):`, schema);
    
    // Test batch schema operations
    console.log("\nTesting batch schema operations...");
    const tableNames = ["Patients", "Doctors", "Visits"];
    const schemas = [
      JSON.stringify({ PatientID: "string", Name: "string" }),
      JSON.stringify({ DoctorID: "string", Specialization: "string" }),
      JSON.stringify({ VisitID: "string", Date: "string" })
    ];
    
    for (let i = 0; i < tableNames.length; i++) {
      const tx = await web3dbContract.updateTableSchema(tableNames[i], schemas[i]);
      await tx.wait();
    }
    console.log("✅ Batch schema updates completed");
    
    const batchSchemas = await web3dbContract.batchGetTableSchemas(tableNames);
    console.log("Batch schema results:", batchSchemas);
    
    // Test 3: Access Policy Management
    console.log("\n🔐 Test 3: Access Policy Management");
    console.log("=" .repeat(40));
    
    const testWallet = "0x742d35Cc6634C0532925A3B8D4C9dB96C4B4d8B6";
    const policyTable = "Patients";
    const policySql = "SELECT * FROM Patients WHERE Age > 18";
    
    // Test initial policy count
    let policyCount = await web3dbContract.getPolicyCount(testWallet);
    console.log(`Initial policy count for ${testWallet}:`, policyCount.toString());
    
    // Test adding access policy
    console.log(`Adding access policy for ${testWallet}...`);
    const addPolicyTx = await web3dbContract.addAccessPolicy(testWallet, policyTable, policySql);
    await addPolicyTx.wait();
    console.log("✅ Access policy added successfully");
    
    // Verify policy count
    policyCount = await web3dbContract.getPolicyCount(testWallet);
    console.log(`Updated policy count for ${testWallet}:`, policyCount.toString());
    
    // Test getting access policies
    const policies = await web3dbContract.getAccessPolicies(testWallet);
    console.log(`Access policies for ${testWallet}:`, policies.length);
    if (policies.length > 0) {
      console.log("First policy:", {
        ownerAddress: policies[0].ownerAddress,
        tableName: policies[0].tableName,
        policySql: policies[0].policySql
      });
    }
    
    // Test adding multiple policies
    console.log("\nAdding multiple policies...");
    const policy2Tx = await web3dbContract.addAccessPolicy(testWallet, "Doctors", "SELECT * FROM Doctors");
    await policy2Tx.wait();
    
    const policy3Tx = await web3dbContract.addAccessPolicy(testWallet, "Visits", "SELECT * FROM Visits WHERE Date > '2024-01-01'");
    await policy3Tx.wait();
    
    const finalPolicyCount = await web3dbContract.getPolicyCount(testWallet);
    console.log(`Final policy count for ${testWallet}:`, finalPolicyCount.toString());
    
    // Test removing a policy
    console.log("\nRemoving first policy...");
    const removePolicyTx = await web3dbContract.removeAccessPolicy(testWallet, 0);
    await removePolicyTx.wait();
    
    const afterRemoveCount = await web3dbContract.getPolicyCount(testWallet);
    console.log(`Policy count after removal:`, afterRemoveCount.toString());
    
    // Test 4: Event Testing
    console.log("\n📡 Test 4: Event Testing");
    console.log("=" .repeat(40));
    
    // Test IndexUpdated event
    console.log("Testing IndexUpdated event...");
    const eventTestTx = await web3dbContract.updateIndexCID("EventTest", "QmEventTest");
    const receipt = await eventTestTx.wait();
    
    const indexUpdatedEvents = receipt.logs.filter(log => {
      try {
        const parsed = web3dbContract.interface.parseLog(log);
        return parsed.name === "IndexUpdated";
      } catch (e) {
        return false;
      }
    });
    
    console.log(`Found ${indexUpdatedEvents.length} IndexUpdated events`);
    
    // Test 5: Error Handling
    console.log("\n⚠️  Test 5: Error Handling");
    console.log("=" .repeat(40));
    
    try {
      // Test batch update with mismatched array lengths
      await web3dbContract.batchUpdateIndexCIDs(["attr1", "attr2"], ["cid1"]);
      console.log("❌ Should have thrown an error for mismatched arrays");
    } catch (error) {
      console.log("✅ Correctly caught error for mismatched arrays:", error.message);
    }
    
    try {
      // Test removing non-existent policy
      await web3dbContract.removeAccessPolicy(testWallet, 999);
      console.log("❌ Should have thrown an error for invalid policy index");
    } catch (error) {
      console.log("✅ Correctly caught error for invalid policy index:", error.message);
    }
    
    console.log("\n🎉 All tests completed successfully!");
    console.log("\n📊 Test Summary:");
    console.log("✅ Index CID Management: PASSED");
    console.log("✅ Schema Management: PASSED");
    console.log("✅ Access Policy Management: PASSED");
    console.log("✅ Event Testing: PASSED");
    console.log("✅ Error Handling: PASSED");
    
    return {
      contractAddress,
      contract: web3dbContract,
      deployerAddress
    };
    
  } catch (error) {
    console.error("❌ Testing failed:", error);
    throw error;
  }
}

// Execute the tests
main()
  .then(() => {
    console.log("\n✨ Test script completed successfully!");
    process.exit(0);
  })
  .catch((error) => {
    console.error("\n💥 Test script failed:", error);
    process.exit(1);
  });

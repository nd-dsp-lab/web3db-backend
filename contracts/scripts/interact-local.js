const { ignition } = require("hardhat");
const Web3dbModule = require("../ignition/modules/Web3dbModule");

async function main() {
  console.log("🔗 Interacting with Web3DB Contract on local network...");
  
  try {
    // Deploy the contract using Ignition
    const { web3dbContract } = await ignition.deploy(Web3dbModule);
    await web3dbContract.waitForDeployment();
    
    const contractAddress = await web3dbContract.getAddress();
    console.log("📍 Contract Address:", contractAddress);
    
    const deployerAddress = await web3dbContract.runner.getAddress();
    console.log("👤 Deployer Address:", deployerAddress);
    
    console.log("\n🎯 Interactive Demo - Web3DB Contract Functions\n");
    
    // Demo 1: Index Management
    console.log("📊 Demo 1: Index CID Management");
    console.log("-".repeat(50));
    
    // Update some index CIDs
    console.log("Updating PatientID index...");
    await (await web3dbContract.updateIndexCID("PatientID", "QmPatientIndex123")).wait();
    
    console.log("Updating DoctorID index...");
    await (await web3dbContract.updateIndexCID("DoctorID", "QmDoctorIndex456")).wait();
    
    // Retrieve the CIDs
    const patientCID = await web3dbContract.getIndexCID("PatientID");
    const doctorCID = await web3dbContract.getIndexCID("DoctorID");
    
    console.log(`PatientID Index CID: ${patientCID}`);
    console.log(`DoctorID Index CID: ${doctorCID}`);
    
    // Demo 2: Schema Management
    console.log("\n📋 Demo 2: Schema Management");
    console.log("-".repeat(50));
    
    const patientSchema = JSON.stringify({
      PatientID: "string",
      Name: "string", 
      Age: "number",
      Diagnosis: "string",
      AdmissionDate: "string"
    });
    
    console.log("Setting Patient table schema...");
    await (await web3dbContract.updateTableSchema("Patients", patientSchema)).wait();
    
    const retrievedSchema = await web3dbContract.getTableSchema("Patients");
    console.log("Retrieved schema:", JSON.parse(retrievedSchema));
    
    // Demo 3: Access Policies
    console.log("\n🔐 Demo 3: Access Policy Management");
    console.log("-".repeat(50));
    
    const testUser = "0x742d35Cc6634C0532925A3B8D4C9dB96C4B4d8B6";
    
    console.log(`Adding access policy for user: ${testUser}`);
    await (await web3dbContract.addAccessPolicy(
      testUser, 
      "Patients", 
      "SELECT * FROM Patients WHERE Age > 18"
    )).wait();
    
    await (await web3dbContract.addAccessPolicy(
      testUser, 
      "Doctors", 
      "SELECT * FROM Doctors WHERE Specialization = 'Cardiology'"
    )).wait();
    
    const policyCount = await web3dbContract.getPolicyCount(testUser);
    console.log(`Total policies for user: ${policyCount.toString()}`);
    
    const policies = await web3dbContract.getAccessPolicies(testUser);
    console.log("User policies:");
    policies.forEach((policy, index) => {
      console.log(`  ${index + 1}. Table: ${policy.tableName}`);
      console.log(`     Policy: ${policy.policySql}`);
    });
    
    // Demo 4: Batch Operations
    console.log("\n📦 Demo 4: Batch Operations");
    console.log("-".repeat(50));
    
    const attributes = ["VisitID", "PrescriptionID", "LabResultID"];
    const cids = ["QmVisit789", "QmPrescription101", "QmLabResult202"];
    
    console.log("Performing batch index update...");
    await (await web3dbContract.batchUpdateIndexCIDs(attributes, cids)).wait();
    
    const batchResults = await web3dbContract.batchGetIndexCIDs(attributes);
    console.log("Batch index results:");
    attributes.forEach((attr, index) => {
      console.log(`  ${attr}: ${batchResults[index]}`);
    });
    
    // Demo 5: Contract State Summary
    console.log("\n📈 Demo 5: Contract State Summary");
    console.log("-".repeat(50));
    
    const allAttributes = ["PatientID", "DoctorID", "VisitID", "PrescriptionID", "LabResultID"];
    const allCids = await web3dbContract.batchGetIndexCIDs(allAttributes);
    
    console.log("All Index CIDs:");
    allAttributes.forEach((attr, index) => {
      if (allCids[index]) {
        console.log(`  ✅ ${attr}: ${allCids[index]}`);
      } else {
        console.log(`  ❌ ${attr}: Not set`);
      }
    });
    
    const tables = ["Patients", "Doctors", "Visits"];
    const schemas = await web3dbContract.batchGetTableSchemas(tables);
    
    console.log("\nAll Table Schemas:");
    tables.forEach((table, index) => {
      if (schemas[index]) {
        console.log(`  ✅ ${table}: Schema defined`);
      } else {
        console.log(`  ❌ ${table}: No schema`);
      }
    });
    
    console.log("\n🎉 Interactive demo completed successfully!");
    console.log("\n💡 Tips for further interaction:");
    console.log("  - Use the contract address to connect from other applications");
    console.log("  - Listen for events to track contract state changes");
    console.log("  - Implement proper error handling in production code");
    console.log("  - Consider gas optimization for batch operations");
    
    return {
      contractAddress,
      contract: web3dbContract,
      deployerAddress
    };
    
  } catch (error) {
    console.error("❌ Interaction failed:", error);
    throw error;
  }
}

// Execute the interaction demo
main()
  .then(() => {
    console.log("\n✨ Interaction script completed successfully!");
    process.exit(0);
  })
  .catch((error) => {
    console.error("\n💥 Interaction script failed:", error);
    process.exit(1);
  });

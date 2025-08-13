const { ethers } = require("hardhat");

async function main() {
    console.log("Testing Schema Management Functions...\n");

    // Get the contract address from environment or use default
    const contractAddress = process.env.CONTRACT_ADDRESS || "0xe4B4B17AA1Fe9f90fA1521ed87FfcC0f85452F91";

    // Get the deployer account
    const [deployer] = await ethers.getSigners();
    console.log("Testing with account:", deployer.address);

    // Check account balance - handle both ethers v5 and v6
    const balance = await ethers.provider.getBalance(deployer.address);
    let balanceFormatted;
    try {
        balanceFormatted = ethers.formatEther(balance);
    } catch (error) {
        balanceFormatted = ethers.utils.formatEther(balance);
    }
    console.log("Account balance:", balanceFormatted, "ETH\n");

    // Get the contract factory and attach to deployed contract
    const IndexState = await ethers.getContractFactory("IndexState");
    const contract = IndexState.attach(contractAddress);

    console.log("Contract address:", contractAddress);
    console.log("Contract attached successfully\n");

    try {
        // Test 1: Check initial schema state
        console.log("=== Test 1: Check Initial Schema State ===");
        const tables = ["patient_data", "hospital_data", "test_table"];

        for (const table of tables) {
            const schema = await contract.getTableSchema(table);
            console.log(`${table}: "${schema}"`);
        }
        console.log();

        // Test 2: Single schema update
        console.log("=== Test 2: Single Schema Update ===");
        const testSchema = JSON.stringify({
            table_name: "patient_data",
            columns: [
                { name: "PatientID", type: "string", nullable: false },
                { name: "HospitalID", type: "string", nullable: false },
                { name: "Age", type: "integer", nullable: true }
            ],
            primary_key: ["PatientID"],
            indexes: ["PatientID", "HospitalID", "Age"],
            created_at: new Date().toISOString()
        });

        console.log(`Updating patient_data schema...`);
        const tx1 = await contract.updateTableSchema("patient_data", testSchema);
        const receipt1 = await tx1.wait();
        console.log(`Transaction hash: ${tx1.hash}`);
        console.log(`Gas used: ${receipt1.gasUsed.toString()}`);

        // Verify the update
        const updatedSchema = await contract.getTableSchema("patient_data");
        console.log(`Verified schema length: ${updatedSchema.length} characters`);
        console.log(`Update successful: ${updatedSchema === testSchema}\n`);

        // Test 3: Batch schema retrieval
        console.log("=== Test 3: Batch Schema Retrieval ===");

        // Add another schema for testing
        const hospitalSchema = JSON.stringify({
            table_name: "hospital_data",
            columns: [
                { name: "HospitalID", type: "string", nullable: false },
                { name: "Name", type: "string", nullable: false },
                { name: "Location", type: "string", nullable: true }
            ],
            primary_key: ["HospitalID"]
        });

        const tx2 = await contract.updateTableSchema("hospital_data", hospitalSchema);
        await tx2.wait();
        console.log(`Added hospital_data schema`);

        // Batch retrieve
        const batchTables = ["patient_data", "hospital_data"];
        const retrievedSchemas = await contract.batchGetTableSchemas(batchTables);

        console.log("Retrieved schemas:");
        for (let i = 0; i < batchTables.length; i++) {
            const isMatch = retrievedSchemas[i] === (batchTables[i] === "patient_data" ? testSchema : hospitalSchema);
            console.log(`  ${batchTables[i]}: ${retrievedSchemas[i].length} chars`);
            console.log(`  Match: ${isMatch}`);
        }
        console.log();

        // Test 4: Schema removal
        console.log("=== Test 4: Schema Removal ===");

        // Remove hospital_data schema
        console.log("Removing hospital_data schema...");
        const tx3 = await contract.removeTableSchema("hospital_data");
        const receipt3 = await tx3.wait();
        console.log(`Transaction hash: ${tx3.hash}`);
        console.log(`Gas used: ${receipt3.gasUsed.toString()}`);

        // Verify removal
        const removedSchema = await contract.getTableSchema("hospital_data");
        console.log(`Schema after removal: "${removedSchema}"`);
        console.log(`Removal successful: ${removedSchema === ""}\n`);

        console.log("✅ All schema tests completed successfully!");

    } catch (error) {
        console.error("❌ Schema test failed:", error.message);
        if (error.reason) {
            console.error("Reason:", error.reason);
        }
        process.exit(1);
    }
}

main()
    .then(() => process.exit(0))
    .catch((error) => {
        console.error("Error:", error);
        process.exit(1);
    });

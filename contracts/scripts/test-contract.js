const { ethers } = require("hardhat");

async function main() {
    console.log("Testing Web3DB Index CID Contract...\n");

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
        // Test 1: Check initial state
        console.log("=== Test 1: Check Initial State ===");
        const attributes = ["PatientID", "HospitalID", "Age"];

        for (const attr of attributes) {
            const cid = await contract.getIndexCID(attr);
            console.log(`${attr}: "${cid}"`);
        }
        console.log();

        // Test 2: Single update
        console.log("=== Test 2: Single Index Update ===");
        const testCID = "QmTestCID123456789abcdefghijklmnopqrstuvwxyz123";

        console.log(`Updating PatientID index to: ${testCID}`);
        const tx1 = await contract.updateIndexCID("PatientID", testCID);
        const receipt1 = await tx1.wait();
        console.log(`Transaction hash: ${tx1.hash}`);
        console.log(`Gas used: ${receipt1.gasUsed.toString()}`);

        // Verify the update
        const updatedCID = await contract.getIndexCID("PatientID");
        console.log(`Verified PatientID CID: ${updatedCID}`);
        console.log(`Update successful: ${updatedCID === testCID}\n`);

        // Test 3: Batch update
        console.log("=== Test 3: Batch Index Update ===");
        const batchAttributes = ["PatientID", "HospitalID", "Age"];
        const batchCIDs = [
            "QmBatchPatientID123456789abcdefghijklmnopqrstuvwx",
            "QmBatchHospitalID123456789abcdefghijklmnopqrstuvw",
            "QmBatchAge123456789abcdefghijklmnopqrstuvwxyzabcde"
        ];

        console.log("Batch updating all indices...");
        for (let i = 0; i < batchAttributes.length; i++) {
            console.log(`  ${batchAttributes[i]} -> ${batchCIDs[i]}`);
        }

        const tx2 = await contract.batchUpdateIndexCIDs(batchAttributes, batchCIDs);
        const receipt2 = await tx2.wait();
        console.log(`Transaction hash: ${tx2.hash}`);
        console.log(`Gas used: ${receipt2.gasUsed.toString()}`);

        // Test 4: Batch retrieval
        console.log("\n=== Test 4: Batch Index Retrieval ===");
        const retrievedCIDs = await contract.batchGetIndexCIDs(batchAttributes);

        console.log("Retrieved CIDs:");
        for (let i = 0; i < batchAttributes.length; i++) {
            console.log(`  ${batchAttributes[i]}: ${retrievedCIDs[i]}`);
            console.log(`  Match: ${retrievedCIDs[i] === batchCIDs[i]}`);
        }
        console.log();

        // Test 5: Event parsing
        console.log("=== Test 5: Event Analysis ===");
        console.log("Parsing events from batch update transaction...");

        // Parse IndexUpdated events
        const indexUpdatedEvents = receipt2.events?.filter(e => e.event === "IndexUpdated") || [];
        console.log(`Found ${indexUpdatedEvents.length} IndexUpdated events:`);
        indexUpdatedEvents.forEach((event, i) => {
            console.log(`  Event ${i + 1}: ${event.args.attribute} updated from "${event.args.oldCID}" to "${event.args.newCID}"`);
        });

        // Parse BatchIndexUpdated events
        const batchUpdatedEvents = receipt2.events?.filter(e => e.event === "BatchIndexUpdated") || [];
        console.log(`Found ${batchUpdatedEvents.length} BatchIndexUpdated events:`);
        batchUpdatedEvents.forEach((event, i) => {
            console.log(`  Batch Event ${i + 1}: Updated ${event.args.attributes.length} attributes`);
        });
        console.log();

        // Test 6: Gas comparison
        console.log("=== Test 6: Gas Usage Comparison ===");
        console.log(`Single update gas: ${receipt1.gasUsed.toString()}`);
        console.log(`Batch update gas: ${receipt2.gasUsed.toString()}`);

        // Calculate gas savings - handle both ethers v5 and v6
        let estimatedSingleGas, savings;
        if (typeof receipt1.gasUsed.mul === 'function') {
            // ethers v5 - use BigNumber methods
            estimatedSingleGas = receipt1.gasUsed.mul(3);
            savings = estimatedSingleGas.sub(receipt2.gasUsed);
        } else {
            // ethers v6 - use native BigInt operations
            estimatedSingleGas = receipt1.gasUsed * 3n;
            savings = estimatedSingleGas - receipt2.gasUsed;
        }

        // Calculate savings percentage - handle both ethers v5 and v6
        let savingsPercent;
        if (typeof savings.mul === 'function') {
            // ethers v5 - use BigNumber methods
            savingsPercent = savings.mul(100).div(estimatedSingleGas);
        } else {
            // ethers v6 - use native BigInt operations
            savingsPercent = (savings * 100n) / estimatedSingleGas;
        }

        console.log(`Estimated 3 single updates: ${estimatedSingleGas.toString()}`);
        console.log(`Gas savings: ${savings.toString()} (${savingsPercent.toString()}%)`);
        console.log();

        console.log("✅ All tests completed successfully!");

    } catch (error) {
        console.error("❌ Test failed:", error.message);
        if (error.reason) {
            console.error("Reason:", error.reason);
        }
        process.exit(1);
    }
}

main()
    .then(() => process.exit(0))
    .catch((error) => {
        console.error("Script failed:", error);
        process.exit(1);
    });

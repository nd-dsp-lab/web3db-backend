const { expect } = require("chai");
const { ethers } = require("hardhat");

describe("IndexState", function () {
    let contract;
    let owner;
    let addr1;

    beforeEach(async function () {
        // Get signers
        [owner, addr1] = await ethers.getSigners();

        // Deploy contract - handle both ethers v5 and v6
        const IndexState = await ethers.getContractFactory("IndexState");
        contract = await IndexState.deploy();

        // Handle both ethers v5 and v6 deployment waiting
        try {
            await contract.deployed(); // v5 syntax
        } catch (error) {
            await contract.waitForDeployment(); // v6 syntax
        }
    });

    describe("Single Index Operations", function () {
        it("Should return empty string for non-existent index", async function () {
            const cid = await contract.getIndexCID("NonExistent");
            expect(cid).to.equal("");
        });

        it("Should update and retrieve a single index CID", async function () {
            const testCID = "QmTestCID123456789abcdefghijklmnopqrstuvwxyz";

            // Update index
            await contract.updateIndexCID("PatientID", testCID);

            // Retrieve and verify
            const retrievedCID = await contract.getIndexCID("PatientID");
            expect(retrievedCID).to.equal(testCID);
        });

        it("Should emit IndexUpdated event on update", async function () {
            const testCID = "QmTestCID123456789abcdefghijklmnopqrstuvwxyz";

            await expect(contract.updateIndexCID("PatientID", testCID))
                .to.emit(contract, "IndexUpdated")
                .withArgs("PatientID", "", testCID);
        });

        it("Should update existing index and emit correct old CID", async function () {
            const oldCID = "QmOldCID123456789abcdefghijklmnopqrstuvwxyz";
            const newCID = "QmNewCID123456789abcdefghijklmnopqrstuvwxyz";

            // Set initial CID
            await contract.updateIndexCID("PatientID", oldCID);

            // Update with new CID
            await expect(contract.updateIndexCID("PatientID", newCID))
                .to.emit(contract, "IndexUpdated")
                .withArgs("PatientID", oldCID, newCID);
        });
    });

    describe("Batch Operations", function () {
        it("Should batch update multiple indices", async function () {
            const attributes = ["PatientID", "HospitalID", "Age"];
            const cids = [
                "QmPatientCID123456789abcdefghijklmnopqrstuvwxyz",
                "QmHospitalCID123456789abcdefghijklmnopqrstuvwxy",
                "QmAgeCID123456789abcdefghijklmnopqrstuvwxyzabc"
            ];

            // Batch update
            await contract.batchUpdateIndexCIDs(attributes, cids);

            // Verify each update
            for (let i = 0; i < attributes.length; i++) {
                const retrievedCID = await contract.getIndexCID(attributes[i]);
                expect(retrievedCID).to.equal(cids[i]);
            }
        });

        it("Should batch retrieve multiple indices", async function () {
            const attributes = ["PatientID", "HospitalID", "Age"];
            const cids = [
                "QmPatientCID123456789abcdefghijklmnopqrstuvwxyz",
                "QmHospitalCID123456789abcdefghijklmnopqrstuvwxy",
                "QmAgeCID123456789abcdefghijklmnopqrstuvwxyzabc"
            ];

            // Set up data
            await contract.batchUpdateIndexCIDs(attributes, cids);

            // Batch retrieve
            const retrievedCIDs = await contract.batchGetIndexCIDs(attributes);

            // Verify all CIDs
            for (let i = 0; i < attributes.length; i++) {
                expect(retrievedCIDs[i]).to.equal(cids[i]);
            }
        });

        it("Should emit both BatchIndexUpdated and individual IndexUpdated events", async function () {
            const attributes = ["PatientID", "HospitalID"];
            const cids = [
                "QmPatientCID123456789abcdefghijklmnopqrstuvwxyz",
                "QmHospitalCID123456789abcdefghijklmnopqrstuvwxy"
            ];

            const tx = await contract.batchUpdateIndexCIDs(attributes, cids);
            const receipt = await tx.wait();

            // Handle events for both ethers v5 and v6
            let events;
            if (receipt.events) {
                events = receipt.events; // ethers v5
            } else {
                // ethers v6 - parse logs manually
                events = receipt.logs.map(log => {
                    try {
                        return contract.interface.parseLog(log);
                    } catch (e) {
                        return null;
                    }
                }).filter(Boolean);
            }

            // Check for BatchIndexUpdated event
            const batchEvents = events.filter(e => e.name === "BatchIndexUpdated" || e.event === "BatchIndexUpdated");
            expect(batchEvents).to.have.length(1);
            expect(batchEvents[0].args.attributes).to.deep.equal(attributes);
            expect(batchEvents[0].args.newCIDs).to.deep.equal(cids);

            // Check for individual IndexUpdated events
            const indexEvents = events.filter(e => e.name === "IndexUpdated" || e.event === "IndexUpdated");
            expect(indexEvents).to.have.length(2);

            for (let i = 0; i < attributes.length; i++) {
                expect(indexEvents[i].args.attribute).to.equal(attributes[i]);
                expect(indexEvents[i].args.oldCID).to.equal("");
                expect(indexEvents[i].args.newCID).to.equal(cids[i]);
            }
        });

        it("Should revert if attributes and CIDs arrays have different lengths", async function () {
            const attributes = ["PatientID", "HospitalID"];
            const cids = ["QmOnlyCID123456789abcdefghijklmnopqrstuvwxyz"];

            await expect(contract.batchUpdateIndexCIDs(attributes, cids))
                .to.be.revertedWith("Arrays must be same length");
        });

        it("Should handle empty arrays", async function () {
            await expect(contract.batchUpdateIndexCIDs([], []))
                .to.not.be.reverted;
        });
    });

    describe("Remove Index", function () {
        it("Should remove an index", async function () {
            const testCID = "QmTestCID123456789abcdefghijklmnopqrstuvwxyz";

            // Set initial CID
            await contract.updateIndexCID("PatientID", testCID);
            expect(await contract.getIndexCID("PatientID")).to.equal(testCID);

            // Remove index
            await contract.removeIndex("PatientID");
            expect(await contract.getIndexCID("PatientID")).to.equal("");
        });

        it("Should emit IndexUpdated event when removing", async function () {
            const testCID = "QmTestCID123456789abcdefghijklmnopqrstuvwxyz";

            // Set initial CID
            await contract.updateIndexCID("PatientID", testCID);

            // Remove and check event
            await expect(contract.removeIndex("PatientID"))
                .to.emit(contract, "IndexUpdated")
                .withArgs("PatientID", testCID, "");
        });
    });

    describe("Gas Optimization", function () {
        it("Should use less gas for batch updates vs individual updates", async function () {
            const attributes = ["PatientID", "HospitalID", "Age"];
            const cids = [
                "QmPatientCID123456789abcdefghijklmnopqrstuvwxyz",
                "QmHospitalCID123456789abcdefghijklmnopqrstuvwxy",
                "QmAgeCID123456789abcdefghijklmnopqrstuvwxyzabc"
            ];

            // Test individual updates
            const individualTxs = [];
            for (let i = 0; i < attributes.length; i++) {
                const tx = await contract.updateIndexCID(attributes[i], cids[i]);
                const receipt = await tx.wait();
                individualTxs.push(receipt.gasUsed);
            }

            // Handle BigNumber operations for both ethers v5 and v6
            let totalIndividualGas;
            if (typeof individualTxs[0].add === 'function') {
                // ethers v5 - use BigNumber methods
                totalIndividualGas = individualTxs.reduce((sum, gas) => sum.add(gas), ethers.BigNumber.from(0));
            } else {
                // ethers v6 - use native BigInt
                totalIndividualGas = individualTxs.reduce((sum, gas) => sum + gas, 0n);
            }

            // Reset contract for fair comparison
            const IndexState = await ethers.getContractFactory("IndexState");
            const freshContract = await IndexState.deploy();

            // Handle deployment waiting for both ethers v5 and v6
            try {
                await freshContract.deployed(); // v5 syntax
            } catch (error) {
                await freshContract.waitForDeployment(); // v6 syntax
            }

            // Test batch update
            const batchTx = await freshContract.batchUpdateIndexCIDs(attributes, cids);
            const batchReceipt = await batchTx.wait();

            console.log(`Individual updates total gas: ${totalIndividualGas.toString()}`);
            console.log(`Batch update gas: ${batchReceipt.gasUsed.toString()}`);

            // Batch should be more efficient - handle comparison for both versions
            let isBatchMoreEfficient;
            if (typeof batchReceipt.gasUsed.lt === 'function') {
                // ethers v5
                isBatchMoreEfficient = batchReceipt.gasUsed.lt(totalIndividualGas);
            } else {
                // ethers v6
                isBatchMoreEfficient = batchReceipt.gasUsed < totalIndividualGas;
            }

            expect(isBatchMoreEfficient).to.be.true;
        });
    });

    describe("Schema Management", function () {
        it("Should return empty string for non-existent table schema", async function () {
            const schema = await contract.getTableSchema("NonExistentTable");
            expect(schema).to.equal("");
        });

        it("Should update and retrieve a table schema", async function () {
            const tableName = "patient_data";
            const testSchema = JSON.stringify({
                columns: [
                    { name: "PatientID", type: "string", nullable: false },
                    { name: "Age", type: "integer", nullable: true }
                ],
                primary_key: ["PatientID"]
            });

            // Update schema
            await contract.updateTableSchema(tableName, testSchema);

            // Retrieve and verify
            const retrievedSchema = await contract.getTableSchema(tableName);
            expect(retrievedSchema).to.equal(testSchema);
        });

        it("Should emit SchemaUpdated event on schema update", async function () {
            const tableName = "test_table";
            const testSchema = JSON.stringify({ columns: [], primary_key: [] });

            await expect(contract.updateTableSchema(tableName, testSchema))
                .to.emit(contract, "SchemaUpdated")
                .withArgs(tableName, "", testSchema);
        });

        it("Should batch retrieve multiple table schemas", async function () {
            const tableNames = ["table1", "table2", "table3"];
            const schemas = [
                JSON.stringify({ table: "table1", columns: ["id", "name"] }),
                JSON.stringify({ table: "table2", columns: ["id", "value"] }),
                JSON.stringify({ table: "table3", columns: ["id", "data"] })
            ];

            // Set up test data
            for (let i = 0; i < tableNames.length; i++) {
                await contract.updateTableSchema(tableNames[i], schemas[i]);
            }

            // Batch retrieve
            const retrievedSchemas = await contract.batchGetTableSchemas(tableNames);

            // Verify all schemas
            for (let i = 0; i < tableNames.length; i++) {
                expect(retrievedSchemas[i]).to.equal(schemas[i]);
            }
        });

        it("Should remove a table schema", async function () {
            const tableName = "temp_table";
            const testSchema = JSON.stringify({ columns: ["id"], primary_key: ["id"] });

            // Set initial schema
            await contract.updateTableSchema(tableName, testSchema);
            expect(await contract.getTableSchema(tableName)).to.equal(testSchema);

            // Remove schema
            await contract.removeTableSchema(tableName);
            expect(await contract.getTableSchema(tableName)).to.equal("");
        });

        it("Should emit SchemaUpdated event when removing schema", async function () {
            const tableName = "remove_test_table";
            const testSchema = JSON.stringify({ columns: ["test"], primary_key: ["test"] });

            // Set initial schema
            await contract.updateTableSchema(tableName, testSchema);

            // Remove and check event
            await expect(contract.removeTableSchema(tableName))
                .to.emit(contract, "SchemaUpdated")
                .withArgs(tableName, testSchema, "");
        });
    });
});

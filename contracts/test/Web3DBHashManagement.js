const { expect } = require("chai");
const { ethers } = require("hardhat");

describe("Web3DBHashManagement", function () {
    let contract;
    let owner;
    let user1;
    let user2;

    beforeEach(async function () {
        [owner, user1, user2] = await ethers.getSigners();

        const Contract = await ethers.getContractFactory("Web3DBHashManagement");
        contract = await Contract.deploy();
    });

    describe("Hash Management", function () {
        it("should add a new hash mapping", async function () {
            const tx = await contract.addHashMapping(
                "users",                    // tableName
                "partition1",               // partition
                "QmHash1",                 // hash
                "",                        // prevHash (empty for first record)
                "create",                  // recordType
                "initial",                 // flag
                100                        // rowCount
            );

            await tx.wait();

            const latestHash = await contract.getLatestHash(owner.address, "users", "partition1");
            expect(latestHash).to.equal("QmHash1");
        });

        it("should maintain hash history", async function () {
            // Add first hash
            await contract.addHashMapping(
                "users",
                "partition1",
                "QmHash1",
                "",
                "create",
                "initial",
                100
            );

            // Add second hash
            await contract.addHashMapping(
                "users",
                "partition1",
                "QmHash2",
                "QmHash1",
                "update",
                "latest",
                150
            );

            const [hashes, prevHashes, types, timestamps, flags, rowCounts] =
                await contract.getHashHistory(owner.address, "users", "partition1");

            expect(hashes.length).to.equal(2);
            expect(hashes[0]).to.equal("QmHash1");
            expect(hashes[1]).to.equal("QmHash2");
            expect(prevHashes[1]).to.equal("QmHash1");
            expect(types[1]).to.equal("update");
            expect(flags[0]).to.equal("initial");
            expect(rowCounts[1]).to.equal(150);
        });
    });

    describe("Table Sharing", function () {
        beforeEach(async function () {
            // Create a table first
            await contract.addHashMapping(
                "users",
                "0",
                "QmHash1",
                "",
                "create",
                "initial",
                100
            );
        });

        it("should share table with another user", async function () {
            const tx = await contract.shareTable("users", user1.address);
            await tx.wait();

            // User1 should be able to read the hash
            const latestHash = await contract.getLatestHash(owner.address, "users", "0");
            expect(latestHash).to.equal("QmHash1");
        });

        it("should revoke access", async function () {
            // Share and then revoke
            await contract.shareTable("users", user1.address);
            const tx = await contract.revokeAccess("users", user1.address);
            await tx.wait();
        });
    });

    describe("User Tables", function () {
        it("should track user tables", async function () {
            await contract.addHashMapping(
                "users",
                "0",
                "QmHash1",
                "",
                "create",
                "initial",
                100
            );

            await contract.addHashMapping(
                "products",
                "0",
                "QmHash2",
                "",
                "create",
                "initial",
                50
            );

            const tables = await contract.getUserTables(owner.address);
            expect(tables.length).to.equal(2);
            expect(tables).to.include("users");
            expect(tables).to.include("products");
        });
    });
});
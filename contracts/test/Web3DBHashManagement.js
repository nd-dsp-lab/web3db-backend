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
        await contract.waitForDeployment();
    });

    describe("Hash Mapping", function () {
        it("Should add hash mapping correctly", async function () {
            const tableName = "users";
            const partition = "0";
            const hash = "QmTest123";
            const prevHash = "";
            const recordType = "create";
            const flag = "initial";
            const rowCount = 1;

            await expect(
                contract.addHashMapping(tableName, partition, hash, prevHash, recordType, flag, rowCount)
            ).to.emit(contract, "HashMappingAdded")
                .withArgs(owner.address, tableName, partition, hash, recordType);

            const latestHash = await contract.getLatestHash(owner.address, tableName, partition);
            expect(latestHash).to.equal(hash);
        });

        it("Should track user tables correctly", async function () {
            const tableName = "users";
            const partition = "0";

            await contract.addHashMapping(tableName, partition, "QmTest123", "", "create", "initial", 1);

            const userTables = await contract.getUserTables(owner.address);
            expect(userTables).to.have.lengthOf(1);
            expect(userTables[0]).to.equal(tableName);
        });

        it("Should maintain correct hash history", async function () {
            const tableName = "users";
            const partition = "0";

            // Add multiple hash mappings
            await contract.addHashMapping(tableName, partition, "Hash1", "", "create", "initial", 1);
            await contract.addHashMapping(tableName, partition, "Hash2", "Hash1", "update", "latest", 2);

            const [hashes, prevHashes, types, timestamps, flags, rowCounts] =
                await contract.getHashHistory(owner.address, tableName, partition);

            expect(hashes).to.have.lengthOf(2);
            expect(hashes[0]).to.equal("Hash1");
            expect(hashes[1]).to.equal("Hash2");
            expect(prevHashes[1]).to.equal("Hash1");
            expect(types[0]).to.equal("create");
            expect(types[1]).to.equal("update");
            expect(flags[0]).to.equal("initial");
            expect(flags[1]).to.equal("latest");
            expect(rowCounts[0]).to.equal(1);
            expect(rowCounts[1]).to.equal(2);
        });
    });

    describe("CID Sharing", function () {
        beforeEach(async function () {
            // Setup: Add a hash mapping first
            await contract.addHashMapping("users", "0", "QmTest123", "", "create", "initial", 1);
        });

        it("Should share CID correctly", async function () {
            await expect(
                contract.shareCID(user1.address, "users", "QmTest123")
            ).to.emit(contract, "CIDShared")
                .withArgs(owner.address, user1.address, "users", "QmTest123");

            const [cids, owners] = await contract.getSharedCIDs(user1.address, "users");
            expect(cids).to.have.lengthOf(1);
            expect(cids[0]).to.equal("QmTest123");
            expect(owners[0]).to.equal(owner.address);
        });

        it("Should prevent sharing of unowned CID", async function () {
            await expect(
                contract.connect(user1).shareCID(user2.address, "users", "QmTest123")
            ).to.be.revertedWith("Only CID owner can share it");
        });

        it("Should revoke CID access correctly", async function () {
            await contract.shareCID(user1.address, "users", "QmTest123");
            await contract.revokeCIDAccess(user1.address, "users", "QmTest123");

            const [cids, owners] = await contract.getSharedCIDs(user1.address, "users");
            expect(cids).to.have.lengthOf(0);
        });

        it("Should prevent invalid address in sharing", async function () {
            await expect(
                contract.shareCID(ethers.ZeroAddress, "users", "QmTest123")
            ).to.be.revertedWith("Invalid user address");
        });

        it("Should prevent sharing empty CID", async function () {
            await expect(
                contract.shareCID(user1.address, "users", "")
            ).to.be.revertedWith("Invalid CID");
        });
    });

    describe("Edge Cases", function () {
        it("Should handle empty history correctly", async function () {
            const [hashes, , , , ,] = await contract.getHashHistory(owner.address, "nonexistent", "0");
            expect(hashes).to.have.lengthOf(0);
        });

        it("Should handle multiple partitions", async function () {
            await contract.addHashMapping("users", "0", "Hash1", "", "create", "initial", 1);
            await contract.addHashMapping("users", "1", "Hash2", "", "create", "initial", 1);

            const hash0 = await contract.getLatestHash(owner.address, "users", "0");
            const hash1 = await contract.getLatestHash(owner.address, "users", "1");

            expect(hash0).to.equal("Hash1");
            expect(hash1).to.equal("Hash2");
        });
    });
});
const { expect } = require("chai");
const { ethers } = require("hardhat");

/**
 * Sepolia integration test — single signer, one deployment, unique params per test.
 *
 * On Sepolia there is only ONE account (the private key in hardhat.config.js).
 * This test uses that account as both sender and receiver to exercise the full
 * CIDBatchLog and SecLog lifecycles end-to-end on a real network.
 *
 * Role-based access tests (non-sender, non-receiver reverts) are covered by
 * the local Hardhat test suite (Web3dbContract.test.js) where multiple signers
 * are available for free.
 */

// secp256k1 generator G — ecMul(1, GX, GY, 0, PP) = (GX, GY)
const GX = BigInt("0x79BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798");
const GY = BigInt("0x483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8");

describe("Web3dbContract — Sepolia Integration", function () {
    this.timeout(300_000); // 5 min — Sepolia blocks take ~12s each

    let contract;
    let signer;

    // Deploy once for the entire suite (deploying per-test wastes ETH)
    before(async function () {
        [signer] = await ethers.getSigners();
        console.log(`    Signer: ${signer.address}`);

        const Web3dbContract = await ethers.getContractFactory("Web3dbContract");
        contract = await Web3dbContract.deploy();
        await contract.waitForDeployment();
        const addr = await contract.getAddress();
        console.log(`    Contract deployed to: ${addr}`);
    });

    // Use a unique nonce in timelock to avoid "batch exists" / "log already exists"
    let testCounter = 0;
    function uniqueTimelock() {
        testCounter++;
        return Math.floor(Date.now() / 1000) + 3600 + testCounter;
    }

    // ===================================================================
    // CIDBatchLog
    // ===================================================================

    describe("CIDBatchLog lifecycle", function () {
        const CID1 = ethers.keccak256(ethers.toUtf8Bytes("sepolia-cid-1"));
        const CID2 = ethers.keccak256(ethers.toUtf8Bytes("sepolia-cid-2"));
        const cids = [CID1, CID2];

        let batchId;
        let aggregateHash;

        it("computeAggregate returns a deterministic hash", async function () {
            const h1 = await contract.computeAggregate(cids);
            const h2 = await contract.computeAggregate(cids);
            expect(h1).to.equal(h2);
            expect(h1).to.not.equal(ethers.ZeroHash);
        });

        it("createBatch commits CIDs and emits events", async function () {
            const timelock = uniqueTimelock();
            const MESSAGE = ethers.toUtf8Bytes("sepolia batch test");
            const messageHash = ethers.keccak256(MESSAGE);

            // Get return values via staticCall
            [batchId, aggregateHash] = await contract.createBatch.staticCall(
                signer.address, cids, messageHash, timelock
            );
            expect(batchId).to.not.equal(ethers.ZeroHash);

            // Send the actual transaction
            const tx = await contract.createBatch(signer.address, cids, messageHash, timelock);
            const receipt = await tx.wait();
            expect(receipt.status).to.equal(1);

            console.log(`      batchId: ${batchId.substring(0, 18)}...`);
            console.log(`      block:   ${receipt.blockNumber}`);
        });

        it("getBatch returns correct metadata", async function () {
            const [sender, receiver, cidCount, aggHash, , , released, verified] =
                await contract.getBatch(batchId);

            expect(sender).to.equal(signer.address);
            expect(receiver).to.equal(signer.address);
            expect(cidCount).to.equal(2n);
            expect(aggHash).to.equal(aggregateHash);
            expect(released).to.be.false;
            expect(verified).to.be.false;
        });

        it("releaseAggregate marks batch released", async function () {
            const tx = await contract.releaseAggregate(batchId);
            const receipt = await tx.wait();
            expect(receipt.status).to.equal(1);

            const [, , , , , , released] = await contract.getBatch(batchId);
            expect(released).to.be.true;
        });

        it("verifyCIDs proves the CID list matches the commitment", async function () {
            const tx = await contract.verifyCIDs(batchId, cids);
            const receipt = await tx.wait();
            expect(receipt.status).to.equal(1);

            const [, , , , , , , verified] = await contract.getBatch(batchId);
            expect(verified).to.be.true;
        });

        it("verifyCIDs reverts on tampered data", async function () {
            // Create a fresh batch to test against
            const timelock = uniqueTimelock();
            const tx = await contract.createBatch(signer.address, cids, ethers.ZeroHash, timelock);
            await tx.wait();

            const [freshBatchId] = await contract.createBatch.staticCall(
                signer.address, cids, ethers.ZeroHash, timelock
            ).catch(() => {
                // staticCall after actual tx will revert with "batch exists" — expected
                return [null];
            });

            // We already created the batch above; now verify it with wrong CIDs
            // Need to get the batchId from the tx receipt
            const receipt = await tx.wait();
            const logs = receipt.logs.map(log => {
                try { return contract.interface.parseLog(log); } catch { return null; }
            }).filter(Boolean);
            const batchCreatedEvent = logs.find(e => e.name === "BatchCreated");

            if (batchCreatedEvent) {
                const freshId = batchCreatedEvent.args.batchId;
                const tampered = [CID1, CID1]; // CID2 replaced with CID1
                await expect(contract.verifyCIDs(freshId, tampered))
                    .to.be.revertedWith("aggregate mismatch");
            }
        });
    });

    // ===================================================================
    // SecLog
    // ===================================================================

    describe("SecLog lifecycle", function () {
        const SK2 = 1n;
        const SK1X = GX;
        const SK1Y = GY;
        const MESSAGE = ethers.toUtf8Bytes("sepolia seclog test");

        let logId;

        it("newLog creates an EC-committed log entry", async function () {
            const messageHash = ethers.keccak256(MESSAGE);
            const timelock = uniqueTimelock();

            logId = await contract.newLog.staticCall(
                signer.address, SK1X, SK1Y, messageHash, timelock
            );
            expect(logId).to.not.equal(ethers.ZeroHash);

            const tx = await contract.newLog(signer.address, SK1X, SK1Y, messageHash, timelock);
            const receipt = await tx.wait();
            expect(receipt.status).to.equal(1);

            console.log(`      logId: ${logId.substring(0, 18)}...`);
            console.log(`      block: ${receipt.blockNumber}`);
        });

        it("getLog returns correct metadata", async function () {
            const [sender, receiver, sk1x, sk1y, , mh, verified] =
                await contract.getLog(logId);

            expect(sender).to.equal(signer.address);
            expect(receiver).to.equal(signer.address);
            expect(sk1x).to.equal(SK1X);
            expect(sk1y).to.equal(SK1Y);
            expect(verified).to.be.false;
        });

        it("verifyLog proves EC key + message on-chain", async function () {
            const tx = await contract.verifyLog(logId, SK2, MESSAGE);
            const receipt = await tx.wait();
            expect(receipt.status).to.equal(1);

            const [, , , , , , verified] = await contract.getLog(logId);
            expect(verified).to.be.true;

            console.log(`      gasUsed: ${receipt.gasUsed.toString()}`);
        });

        it("verifyLog reverts with wrong sk2", async function () {
            // Create a fresh log
            const messageHash = ethers.keccak256(MESSAGE);
            const timelock = uniqueTimelock();

            const tx = await contract.newLog(signer.address, SK1X, SK1Y, messageHash, timelock);
            await tx.wait();

            const logs = (await tx.wait()).logs.map(log => {
                try { return contract.interface.parseLog(log); } catch { return null; }
            }).filter(Boolean);
            const newLogEvent = logs.find(e => e.name === "LogEntryNew");

            if (newLogEvent) {
                const freshLogId = newLogEvent.args.logId;
                await expect(contract.verifyLog(freshLogId, 2n, MESSAGE))
                    .to.be.revertedWith("Invalid sk2 proof");
            }
        });
    });
});

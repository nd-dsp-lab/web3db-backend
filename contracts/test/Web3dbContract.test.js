const { expect } = require("chai");
const { ethers } = require("hardhat");

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Returns a Unix timestamp `offsetSeconds` from now (defaults to 1 hour). */
function futureTimelock(offsetSeconds = 3600) {
    return Math.floor(Date.now() / 1000) + offsetSeconds;
}

// secp256k1 generator point G.
// ecMul(1, GX, GY, AA=0, PP) == (GX, GY), so sk2=1 is the simplest valid scalar.
const GX = BigInt("0x79BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798");
const GY = BigInt("0x483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8");

// Deterministic test CIDs (bytes32 digests)
const CID1 = ethers.keccak256(ethers.toUtf8Bytes("web3db-cid-1"));
const CID2 = ethers.keccak256(ethers.toUtf8Bytes("web3db-cid-2"));
const CID3 = ethers.keccak256(ethers.toUtf8Bytes("web3db-cid-3"));

// ---------------------------------------------------------------------------
// Test suite
// ---------------------------------------------------------------------------

describe("Web3dbContract — CIDBatchLog & SecLog", function () {
    let contract;
    let owner;   // deployer / default sender
    let addr1;   // used as receiver
    let addr2;   // third-party (not sender, not receiver)

    beforeEach(async function () {
        [owner, addr1, addr2] = await ethers.getSigners();

        const Web3dbContract = await ethers.getContractFactory("Web3dbContract");
        contract = await Web3dbContract.deploy();
        await contract.waitForDeployment();
    });

    // =======================================================================
    // SECTION 9 — CIDBatchLog
    // =======================================================================

    describe("computeAggregate", function () {
        it("is deterministic: same CID list always produces the same hash", async function () {
            const cids = [CID1, CID2, CID3];
            const h1 = await contract.computeAggregate(cids);
            const h2 = await contract.computeAggregate(cids);
            expect(h1).to.equal(h2);
        });

        it("is order-sensitive: [CID1, CID2] ≠ [CID2, CID1]", async function () {
            const h1 = await contract.computeAggregate([CID1, CID2]);
            const h2 = await contract.computeAggregate([CID2, CID1]);
            expect(h1).to.not.equal(h2);
        });

        it("is length-prefix protected: [CID1] ≠ [CID1, CID2] even with subset prefix", async function () {
            const h1 = await contract.computeAggregate([CID1]);
            const h2 = await contract.computeAggregate([CID1, CID2]);
            expect(h1).to.not.equal(h2);
        });
    });

    // -----------------------------------------------------------------------

    describe("createBatch", function () {
        it("returns a non-zero batchId and the correct aggregateHash", async function () {
            const cids = [CID1, CID2];
            const timelock = futureTimelock();

            const [batchId, aggregateHash] = await contract.createBatch.staticCall(
                addr1.address, cids, ethers.ZeroHash, timelock
            );

            expect(batchId).to.not.equal(ethers.ZeroHash);
            expect(aggregateHash).to.equal(await contract.computeAggregate(cids));
        });

        it("emits BatchCreated and CIDsLogged in the same transaction", async function () {
            const cids = [CID1, CID2];
            const timelock = futureTimelock();

            await expect(contract.createBatch(addr1.address, cids, ethers.ZeroHash, timelock))
                .to.emit(contract, "BatchCreated")
                .and.to.emit(contract, "CIDsLogged");
        });

        it("stores correct metadata accessible via getBatch", async function () {
            const cids = [CID1];
            const timelock = futureTimelock();

            const [batchId] = await contract.createBatch.staticCall(
                addr1.address, cids, ethers.ZeroHash, timelock
            );
            await contract.createBatch(addr1.address, cids, ethers.ZeroHash, timelock);

            const [sender, receiver, cidCount, aggregateHash, , , released, verified] =
                await contract.getBatch(batchId);

            expect(sender).to.equal(owner.address);
            expect(receiver).to.equal(addr1.address);
            expect(cidCount).to.equal(1n);
            expect(aggregateHash).to.equal(await contract.computeAggregate(cids));
            expect(released).to.be.false;
            expect(verified).to.be.false;
        });

        it("stores the optional messageHash when provided", async function () {
            const message = ethers.toUtf8Bytes("payload for this batch");
            const messageHash = ethers.keccak256(message);
            const timelock = futureTimelock();

            const [batchId] = await contract.createBatch.staticCall(
                addr1.address, [CID1], messageHash, timelock
            );
            await contract.createBatch(addr1.address, [CID1], messageHash, timelock);

            const [, , , , storedMsgHash] = await contract.getBatch(batchId);
            expect(storedMsgHash).to.equal(messageHash);
        });

        it("reverts when timelock is in the past", async function () {
            const past = Math.floor(Date.now() / 1000) - 1;
            await expect(
                contract.createBatch(addr1.address, [CID1], ethers.ZeroHash, past)
            ).to.be.revertedWith("timelock time must be in the future");
        });

        it("reverts when receiver is the zero address", async function () {
            await expect(
                contract.createBatch(ethers.ZeroAddress, [CID1], ethers.ZeroHash, futureTimelock())
            ).to.be.revertedWith("receiver=0");
        });

        it("reverts when CID list is empty", async function () {
            await expect(
                contract.createBatch(addr1.address, [], ethers.ZeroHash, futureTimelock())
            ).to.be.revertedWith("empty CID list");
        });

        it("reverts when identical parameters are submitted twice (duplicate batchId)", async function () {
            const cids = [CID1];
            const timelock = futureTimelock();

            await contract.createBatch(addr1.address, cids, ethers.ZeroHash, timelock);
            await expect(
                contract.createBatch(addr1.address, cids, ethers.ZeroHash, timelock)
            ).to.be.revertedWith("batch exists");
        });

        it("allows two batches with different timelocks (different batchIds)", async function () {
            const cids = [CID1];
            await contract.createBatch(addr1.address, cids, ethers.ZeroHash, futureTimelock(3600));
            // Different timelock → different batchId → should not revert
            await expect(
                contract.createBatch(addr1.address, cids, ethers.ZeroHash, futureTimelock(7200))
            ).to.not.be.reverted;
        });
    });

    // -----------------------------------------------------------------------

    describe("releaseAggregate", function () {
        let batchId;
        const cids = [CID1, CID2];

        beforeEach(async function () {
            const timelock = futureTimelock();
            [batchId] = await contract.createBatch.staticCall(
                addr1.address, cids, ethers.ZeroHash, timelock
            );
            await contract.createBatch(addr1.address, cids, ethers.ZeroHash, timelock);
        });

        it("marks the batch as released and emits AggregateReleased", async function () {
            const expectedAggregateHash = await contract.computeAggregate(cids);

            await expect(contract.releaseAggregate(batchId))
                .to.emit(contract, "AggregateReleased")
                .withArgs(batchId, expectedAggregateHash);

            const [, , , , , , released] = await contract.getBatch(batchId);
            expect(released).to.be.true;
        });

        it("reverts when called by a non-sender account", async function () {
            await expect(
                contract.connect(addr1).releaseAggregate(batchId)
            ).to.be.revertedWith("only sender");
        });

        it("reverts when the batch does not exist", async function () {
            await expect(
                contract.releaseAggregate(ethers.ZeroHash)
            ).to.be.revertedWith("batch does not exist");
        });

        it("reverts on a second release attempt", async function () {
            await contract.releaseAggregate(batchId);
            await expect(
                contract.releaseAggregate(batchId)
            ).to.be.revertedWith("already released");
        });
    });

    // -----------------------------------------------------------------------

    describe("verifyCIDs", function () {
        let batchId;
        const cids = [CID1, CID2, CID3];

        beforeEach(async function () {
            const timelock = futureTimelock();
            [batchId] = await contract.createBatch.staticCall(
                addr1.address, cids, ethers.ZeroHash, timelock
            );
            await contract.createBatch(addr1.address, cids, ethers.ZeroHash, timelock);
        });

        it("verifies a correct CID list and emits BatchVerified", async function () {
            await expect(contract.connect(addr2).verifyCIDs(batchId, cids))
                .to.emit(contract, "BatchVerified")
                .withArgs(batchId, addr2.address);

            const [, , , , , , , verified] = await contract.getBatch(batchId);
            expect(verified).to.be.true;
        });

        it("anyone (including a third party) can verify", async function () {
            // addr2 is neither sender nor receiver — still allowed
            await expect(contract.connect(addr2).verifyCIDs(batchId, cids))
                .to.not.be.reverted;
        });

        it("reverts when the supplied CID count does not match", async function () {
            await expect(contract.verifyCIDs(batchId, [CID1, CID2]))
                .to.be.revertedWith("CID count mismatch");
        });

        it("reverts when the CID content differs (aggregate mismatch)", async function () {
            const tampered = [CID1, CID2, CID1]; // CID3 replaced with CID1
            await expect(contract.verifyCIDs(batchId, tampered))
                .to.be.revertedWith("aggregate mismatch");
        });

        it("reverts on a second verifyCIDs call (already verified)", async function () {
            await contract.verifyCIDs(batchId, cids);
            await expect(contract.verifyCIDs(batchId, cids))
                .to.be.revertedWith("already verified");
        });

        it("reverts for a non-existent batch", async function () {
            await expect(contract.verifyCIDs(ethers.ZeroHash, cids))
                .to.be.revertedWith("batch does not exist");
        });
    });

    // -----------------------------------------------------------------------

    describe("verifyBatchMessage", function () {
        const MESSAGE = ethers.toUtf8Bytes("hello web3db message");
        let batchId;

        beforeEach(async function () {
            const messageHash = ethers.keccak256(MESSAGE);
            const timelock = futureTimelock();
            [batchId] = await contract.createBatch.staticCall(
                addr1.address, [CID1], messageHash, timelock
            );
            await contract.createBatch(addr1.address, [CID1], messageHash, timelock);
        });

        it("returns true for the correct message", async function () {
            const result = await contract.verifyBatchMessage.staticCall(batchId, MESSAGE);
            expect(result).to.be.true;
        });

        it("reverts when the message does not match the stored hash", async function () {
            const wrongMessage = ethers.toUtf8Bytes("wrong message");
            await expect(contract.verifyBatchMessage(batchId, wrongMessage))
                .to.be.revertedWith("message hash mismatch");
        });

        it("reverts when no messageHash was set at creation (bytes32 zero)", async function () {
            const timelock = futureTimelock(7200);
            const [noMsgBatchId] = await contract.createBatch.staticCall(
                addr1.address, [CID2], ethers.ZeroHash, timelock
            );
            await contract.createBatch(addr1.address, [CID2], ethers.ZeroHash, timelock);

            await expect(contract.verifyBatchMessage(noMsgBatchId, MESSAGE))
                .to.be.revertedWith("no messageHash set");
        });

        it("reverts for a non-existent batch", async function () {
            await expect(contract.verifyBatchMessage(ethers.ZeroHash, MESSAGE))
                .to.be.revertedWith("batch does not exist");
        });
    });

    // -----------------------------------------------------------------------

    describe("getBatch", function () {
        it("returns zeroed fields for a non-existent batchId", async function () {
            const [sender, receiver, cidCount, , , , released, verified] =
                await contract.getBatch(ethers.ZeroHash);

            expect(sender).to.equal(ethers.ZeroAddress);
            expect(receiver).to.equal(ethers.ZeroAddress);
            expect(cidCount).to.equal(0n);
            expect(released).to.be.false;
            expect(verified).to.be.false;
        });
    });

    // =======================================================================
    // SECTION 10 — SecLog
    //
    // Cryptographic note:
    //   sk2 = 1  →  ecMul(1, GX, GY, 0, PP) = G = (GX, GY)
    //   sk2 = 2  →  ecMul(2, GX, GY, 0, PP) = 2G ≠ (GX, GY)
    //
    // Using sk2=1 allows us to test the EC proof with minimal setup while
    // remaining mathematically correct on secp256k1.
    // =======================================================================

    describe("newLog", function () {
        const SK2  = 1n;  // private scalar
        const SK1X = GX;  // public key x = SK2 * G.x
        const SK1Y = GY;  // public key y = SK2 * G.y
        const MESSAGE = ethers.toUtf8Bytes("newLog test message");

        it("returns a non-zero logId", async function () {
            const messageHash = ethers.keccak256(MESSAGE);
            const timelock = futureTimelock();

            const logId = await contract.newLog.staticCall(
                addr1.address, SK1X, SK1Y, messageHash, timelock
            );
            expect(logId).to.not.equal(ethers.ZeroHash);
        });

        it("emits LogEntryNew", async function () {
            const messageHash = ethers.keccak256(MESSAGE);
            await expect(
                contract.newLog(addr1.address, SK1X, SK1Y, messageHash, futureTimelock())
            ).to.emit(contract, "LogEntryNew");
        });

        it("stores correct metadata accessible via getLog", async function () {
            const messageHash = ethers.keccak256(MESSAGE);
            const timelock = futureTimelock();

            const logId = await contract.newLog.staticCall(
                addr1.address, SK1X, SK1Y, messageHash, timelock
            );
            await contract.newLog(addr1.address, SK1X, SK1Y, messageHash, timelock);

            const [sender, receiver, sk1x, sk1y, , mh, verified] =
                await contract.getLog(logId);

            expect(sender).to.equal(owner.address);
            expect(receiver).to.equal(addr1.address);
            expect(sk1x).to.equal(SK1X);
            expect(sk1y).to.equal(SK1Y);
            expect(mh).to.equal(messageHash);
            expect(verified).to.be.false;
        });

        it("reverts when timelock is in the past", async function () {
            const past = Math.floor(Date.now() / 1000) - 1;
            const messageHash = ethers.keccak256(MESSAGE);
            await expect(
                contract.newLog(addr1.address, SK1X, SK1Y, messageHash, past)
            ).to.be.revertedWith("timelock time must be in the future");
        });

        it("reverts when identical parameters are submitted twice (duplicate logId)", async function () {
            const messageHash = ethers.keccak256(MESSAGE);
            const timelock = futureTimelock();

            await contract.newLog(addr1.address, SK1X, SK1Y, messageHash, timelock);
            await expect(
                contract.newLog(addr1.address, SK1X, SK1Y, messageHash, timelock)
            ).to.be.revertedWith("Log already exists");
        });

        it("allows two logs with different timelocks (different logIds)", async function () {
            const messageHash = ethers.keccak256(MESSAGE);
            await contract.newLog(addr1.address, SK1X, SK1Y, messageHash, futureTimelock(3600));
            await expect(
                contract.newLog(addr1.address, SK1X, SK1Y, messageHash, futureTimelock(7200))
            ).to.not.be.reverted;
        });
    });

    // -----------------------------------------------------------------------
    // verifyLog exercises on-chain EC scalar multiplication — set a generous
    // mocha timeout because secp256k1 jacMul runs ~256 iterations in the EVM.
    // -----------------------------------------------------------------------

    describe("verifyLog", function () {
        this.timeout(120_000);

        const SK2  = 1n;  // private scalar  (ecMul(1,GX,GY,0,PP) = (GX,GY))
        const SK1X = GX;
        const SK1Y = GY;
        const MESSAGE = ethers.toUtf8Bytes("verifyLog test message");

        let logId;

        beforeEach(async function () {
            const messageHash = ethers.keccak256(MESSAGE);
            const timelock = futureTimelock();

            logId = await contract.newLog.staticCall(
                addr1.address, SK1X, SK1Y, messageHash, timelock
            );
            await contract.newLog(addr1.address, SK1X, SK1Y, messageHash, timelock);
        });

        it("verifies correct sk2 + message and emits LogVerified", async function () {
            await expect(
                contract.connect(addr1).verifyLog(logId, SK2, MESSAGE)
            )
                .to.emit(contract, "LogVerified")
                .withArgs(logId);

            const [, , , , , , verified] = await contract.getLog(logId);
            expect(verified).to.be.true;
        });

        it("marks the log as verified after a successful call", async function () {
            await contract.connect(addr1).verifyLog(logId, SK2, MESSAGE);

            const [, , , , , , verified] = await contract.getLog(logId);
            expect(verified).to.be.true;
        });

        it("reverts when called by a non-receiver account", async function () {
            await expect(
                contract.connect(addr2).verifyLog(logId, SK2, MESSAGE)
            ).to.be.revertedWith("Only receiver can verify");
        });

        it("reverts when sk2 is wrong (EC point mismatch: 2*G ≠ G)", async function () {
            const wrongSk2 = 2n; // 2*G produces a different point than (GX, GY)
            await expect(
                contract.connect(addr1).verifyLog(logId, wrongSk2, MESSAGE)
            ).to.be.revertedWith("Invalid sk2 proof");
        });

        it("reverts when the supplied message does not match the stored hash", async function () {
            const wrongMessage = ethers.toUtf8Bytes("this is not the right message");
            await expect(
                contract.connect(addr1).verifyLog(logId, SK2, wrongMessage)
            ).to.be.revertedWith("Message hash mismatch");
        });

        it("reverts on a second verifyLog call (already verified)", async function () {
            await contract.connect(addr1).verifyLog(logId, SK2, MESSAGE);
            await expect(
                contract.connect(addr1).verifyLog(logId, SK2, MESSAGE)
            ).to.be.revertedWith("Already verified");
        });

        it("reverts for a non-existent logId", async function () {
            await expect(
                contract.connect(addr1).verifyLog(ethers.ZeroHash, SK2, MESSAGE)
            ).to.be.revertedWith("Log does not exist");
        });
    });

    // -----------------------------------------------------------------------

    describe("getLog", function () {
        it("returns zeroed fields for a non-existent logId", async function () {
            const [sender, receiver, sk1x, sk1y, , , verified] =
                await contract.getLog(ethers.ZeroHash);

            expect(sender).to.equal(ethers.ZeroAddress);
            expect(receiver).to.equal(ethers.ZeroAddress);
            expect(sk1x).to.equal(0n);
            expect(sk1y).to.equal(0n);
            expect(verified).to.be.false;
        });
    });
});

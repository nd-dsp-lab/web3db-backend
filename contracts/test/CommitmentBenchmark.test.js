const { expect } = require("chai");
const { ethers } = require("hardhat");

/**
 * CommitmentBenchmark.test.js
 *
 * Compares three CID commitment schemes across increasing batch sizes:
 *   1. ConcatHash  — sha256(count || cid1 || ... || cidN)   [current CIDBatchLog]
 *   2. HashChain   — sequential sha256 chaining
 *   3. MerkleRoot  — binary Merkle tree
 *
 * For each scheme and batch size, reports:
 *   - Gas used to compute the commitment
 *   - Gas used to verify (full list vs Merkle proof)
 *   - Whether the commitment is order-sensitive
 *   - Whether partial proof (single-leaf) is possible
 */

const BATCH_SIZES = [1, 2, 4, 8, 16, 32, 64];

// Generate N deterministic CID-like bytes32 values
function makeCIDs(n) {
    return Array.from({ length: n }, (_, i) =>
        ethers.keccak256(ethers.toUtf8Bytes(`benchmark-cid-${i}`))
    );
}

// Pretty-print the results table
function printTable(rows) {
    const cols = ["Scheme", "N", "Commit Gas", "Verify Gas", "Total Gas", "Order-sensitive", "Partial Proof"];
    const widths = cols.map((c, i) => Math.max(c.length, ...rows.map(r => String(r[i]).length)));
    const line = "+-" + widths.map(w => "-".repeat(w)).join("-+-") + "-+";
    const header = "| " + cols.map((c, i) => c.padEnd(widths[i])).join(" | ") + " |";
    console.log("\n" + line);
    console.log(header);
    console.log(line);
    rows.forEach(row => {
        console.log("| " + row.map((v, i) => String(v).padEnd(widths[i])).join(" | ") + " |");
    });
    console.log(line + "\n");
}

describe("CommitmentBenchmark — Gas Comparison", function () {
    let contract;
    const results = [];

    before(async function () {
        const Factory = await ethers.getContractFactory("CommitmentBenchmark");
        contract = await Factory.deploy();
        await contract.waitForDeployment();
    });

    // =========================================================================
    // Scheme 1: ConcatHash
    // =========================================================================
    describe("Scheme 1: ConcatHash (current CIDBatchLog)", function () {
        BATCH_SIZES.forEach(n => {
            it(`n=${n}: commit gas`, async function () {
                const cids = makeCIDs(n);
                const tx = await contract.concatHash.send(cids);
                const receipt = await tx.wait();
                const gas = Number(receipt.gasUsed);
                results.push(["ConcatHash", n, gas, "N/A (full list)", gas, "Yes", "No"]);
                console.log(`      ConcatHash  n=${String(n).padStart(2)}  commit=${gas}`);
            });
        });
    });

    // =========================================================================
    // Scheme 2: HashChain
    // =========================================================================
    describe("Scheme 2: HashChain", function () {
        BATCH_SIZES.forEach(n => {
            it(`n=${n}: commit gas`, async function () {
                const cids = makeCIDs(n);
                const tx = await contract.hashChain.send(cids);
                const receipt = await tx.wait();
                const gas = Number(receipt.gasUsed);
                results.push(["HashChain", n, gas, "N/A (full list)", gas, "Yes", "No"]);
                console.log(`      HashChain   n=${String(n).padStart(2)}  commit=${gas}`);
            });
        });
    });

    // =========================================================================
    // Scheme 3: MerkleRoot — commit gas
    // =========================================================================
    describe("Scheme 3: MerkleRoot — commit", function () {
        BATCH_SIZES.forEach(n => {
            it(`n=${n}: commit gas`, async function () {
                const cids = makeCIDs(n);
                const tx = await contract.merkleRoot.send(cids);
                const receipt = await tx.wait();
                const gas = Number(receipt.gasUsed);
                console.log(`      MerkleRoot  n=${String(n).padStart(2)}  commit=${gas}`);
                // Partial verify gas for single leaf
                const root = await contract.merkleRoot.staticCall(cids);
                const _pf0 = await contract.merkleProof.staticCall(cids, 0);
                const proof = Array.from(_pf0[0]), flags = Array.from(_pf0[1]);
                const vtx = await contract.merkleVerify.send(cids[0], proof, flags, root);
                const vreceipt = await vtx.wait();
                const vgas = Number(vreceipt.gasUsed);
                console.log(`      MerkleRoot  n=${String(n).padStart(2)}  verify_partial=${vgas}`);
                results.push(["MerkleRoot", n, gas, vgas, gas + vgas, "No*", "Yes"]);
            });
        });
    });

    // =========================================================================
    // Cross-scheme verification comparison (n=16)
    // Verifying the full list vs a single-leaf Merkle proof
    // =========================================================================
    describe("Verification cost at n=16", function () {
        const N = 16;
        const cids = makeCIDs(N);

        it("ConcatHash — full-list verification", async function () {
            // Verification = just recomputing the hash with the full list
            const tx = await contract.concatHash.send(cids);
            const receipt = await tx.wait();
            console.log(`      ConcatHash  verify_full(n=16)=${receipt.gasUsed}`);
        });

        it("HashChain — full-list verification", async function () {
            const tx = await contract.hashChain.send(cids);
            const receipt = await tx.wait();
            console.log(`      HashChain   verify_full(n=16)=${receipt.gasUsed}`);
        });

        it("MerkleRoot — single-leaf proof verification (log₂(16)=4 hashes)", async function () {
            const root = await contract.merkleRoot.staticCall(cids);
            const _pf7 = await contract.merkleProof.staticCall(cids, 7); // mid-tree leaf
            const proof = Array.from(_pf7[0]), flags = Array.from(_pf7[1]);
            expect(proof.length).to.equal(4); // log₂(16) = 4 levels
            const tx = await contract.merkleVerify.send(cids[7], proof, flags, root);
            const receipt = await tx.wait();
            console.log(`      MerkleRoot  verify_partial(n=16,leaf=7)=${receipt.gasUsed}  proof_depth=${proof.length}`);
        });

        it("MerkleRoot — full-list verification (all 16 leaves)", async function () {
            const tx = await contract.merkleRoot.send(cids);
            const receipt = await tx.wait();
            console.log(`      MerkleRoot  verify_full(n=16)=${receipt.gasUsed}`);
        });
    });

    // =========================================================================
    // Correctness checks — all three schemes must be deterministic
    // =========================================================================
    describe("Correctness", function () {
        const cids = makeCIDs(4);

        it("ConcatHash is deterministic", async function () {
            const h1 = await contract.concatHash.staticCall(cids);
            const h2 = await contract.concatHash.staticCall(cids);
            expect(h1).to.equal(h2);
        });

        it("ConcatHash is order-sensitive", async function () {
            const reversed = [...cids].reverse();
            const h1 = await contract.concatHash.staticCall(cids);
            const h2 = await contract.concatHash.staticCall(reversed);
            expect(h1).to.not.equal(h2);
        });

        it("HashChain is deterministic", async function () {
            const h1 = await contract.hashChain.staticCall(cids);
            const h2 = await contract.hashChain.staticCall(cids);
            expect(h1).to.equal(h2);
        });

        it("HashChain is order-sensitive", async function () {
            const reversed = [...cids].reverse();
            const h1 = await contract.hashChain.staticCall(cids);
            const h2 = await contract.hashChain.staticCall(reversed);
            expect(h1).to.not.equal(h2);
        });

        it("MerkleRoot is deterministic", async function () {
            const h1 = await contract.merkleRoot.staticCall(cids);
            const h2 = await contract.merkleRoot.staticCall(cids);
            expect(h1).to.equal(h2);
        });

        it("MerkleRoot: _hashPair sorts, so it is NOT naively order-sensitive (by design)", async function () {
            // MerkleRoot sorts pairs — the root depends on the SET of CIDs, not their order.
            // This is a trade-off: enables simpler proofs but loses ordering guarantees.
            const reversed = [...cids].reverse();
            const h1 = await contract.merkleRoot.staticCall(cids);
            const h2 = await contract.merkleRoot.staticCall(reversed);
            // With sorted pairs, h1 MAY equal h2 — annotate rather than assert
            console.log(`      MerkleRoot  order-test: original=${h1.slice(0,10)}... reversed=${h2.slice(0,10)}... equal=${h1 === h2}`);
        });

        it("Merkle proof verifies correctly for first leaf", async function () {
            const root = await contract.merkleRoot.staticCall(cids);
            const _pf_first = await contract.merkleProof.staticCall(cids, 0);
            const proof = Array.from(_pf_first[0]), flags = Array.from(_pf_first[1]);
            const valid = await contract.merkleVerify.staticCall(cids[0], proof, flags, root);
            expect(valid).to.be.true;
        });

        it("Merkle proof verifies correctly for last leaf", async function () {
            const root = await contract.merkleRoot.staticCall(cids);
            const _pf_last = await contract.merkleProof.staticCall(cids, cids.length - 1);
            const proof = Array.from(_pf_last[0]), flags = Array.from(_pf_last[1]);
            const valid = await contract.merkleVerify.staticCall(cids[cids.length - 1], proof, flags, root);
            expect(valid).to.be.true;
        });

        it("Merkle proof rejects a tampered leaf", async function () {
            const root = await contract.merkleRoot.staticCall(cids);
            const _pf_tamper = await contract.merkleProof.staticCall(cids, 0);
            const proof = Array.from(_pf_tamper[0]), flags = Array.from(_pf_tamper[1]);
            const fake = ethers.keccak256(ethers.toUtf8Bytes("tampered"));
            const valid = await contract.merkleVerify.staticCall(fake, proof, flags, root);
            expect(valid).to.be.false;
        });

        it("HashChain step-by-step matches bulk", async function () {
            const bulk = await contract.hashChain.staticCall(cids);
            // Manually step through
            let h = ethers.sha256(cids[0]);
            for (let i = 1; i < cids.length; i++) {
                h = await contract.hashChainVerifyStep.staticCall(h, cids[i]);
            }
            expect(h).to.equal(bulk);
        });
    });

    // Print summary table after all tests
    after(function () {
        if (results.length > 0) {
            console.log("\n  ===== COMMITMENT SCHEME BENCHMARK RESULTS =====");
            printTable(results);
            console.log("  Notes:");
            console.log("  * MerkleRoot uses sorted pairs — order-insensitive by default.");
            console.log("    To make it order-sensitive, remove sorting in _hashPair.");
            console.log("  * Verify gas for ConcatHash/HashChain = recompute entire commitment (full list required).");
            console.log("  * Verify gas for MerkleRoot = single-leaf proof, O(log n) hashes.");
            console.log("  * CIDBatchLog uses ConcatHash — cheapest to commit, requires full list to verify.");
            console.log("  * 'N/A (full list)' means no partial proof is possible.\n");
        }
    });
});

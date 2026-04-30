// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

/**
 * CommitmentBenchmark.sol
 *
 * Three CID commitment schemes implemented in pure Solidity for gas benchmarking.
 * All functions are `pure` — they compute and return commitments with no state writes.
 *
 * Schemes:
 *   1. ConcatHash  — sha256(uint256(n) || cid1 || ... || cidN)   [current CIDBatchLog approach]
 *   2. HashChain   — h0=sha256(cid1); h1=sha256(h0||cid2); ...   [sequential chaining]
 *   3. MerkleRoot  — binary Merkle tree over sorted pairs         [tree-based commitment]
 */
contract CommitmentBenchmark {

    // -----------------------------------------------------------------------
    // Scheme 1: Concat Hash  (current CIDBatchLog approach)
    //
    // Packs count + all CIDs into a single abi.encodePacked call, then sha256s
    // the whole thing in one shot.  O(n) memory, 1 hash call.
    // -----------------------------------------------------------------------
    function concatHash(bytes32[] memory cids)
        public pure returns (bytes32)
    {
        require(cids.length > 0, "empty");
        return sha256(abi.encodePacked(uint256(cids.length), cids));
    }

    // -----------------------------------------------------------------------
    // Scheme 2: Hash Chain
    //
    // h = sha256(cid[0])
    // h = sha256(h || cid[1])
    // h = sha256(h || cid[2])
    // ...
    //
    // Order-sensitive. O(n) hash calls.  Verification requires full list.
    // Advantage: streaming — can extend chain without seeing earlier CIDs.
    // -----------------------------------------------------------------------
    function hashChain(bytes32[] memory cids)
        public pure returns (bytes32)
    {
        require(cids.length > 0, "empty");
        bytes32 h = sha256(abi.encodePacked(cids[0]));
        for (uint256 i = 1; i < cids.length; i++) {
            h = sha256(abi.encodePacked(h, cids[i]));
        }
        return h;
    }

    // Verify one step in the chain: caller supplies previous hash + new CID
    function hashChainVerifyStep(bytes32 prevHash, bytes32 cid)
        public pure returns (bytes32)
    {
        return sha256(abi.encodePacked(prevHash, cid));
    }

    // -----------------------------------------------------------------------
    // Scheme 3: Merkle Root
    //
    // Standard binary Merkle tree.  Leaf nodes: sha256(cid).
    // Internal nodes: sha256(left || right), smaller hash on left (sorted).
    // Pads odd-length layers by duplicating the last node.
    //
    // O(n) hash calls to build.  O(log n) hash calls to verify a single leaf.
    // Supports partial proof: prove one CID is in the set without revealing all.
    // -----------------------------------------------------------------------
    function merkleRoot(bytes32[] memory cids)
        public pure returns (bytes32)
    {
        require(cids.length > 0, "empty");

        // Hash leaves
        bytes32[] memory layer = new bytes32[](cids.length);
        for (uint256 i = 0; i < cids.length; i++) {
            layer[i] = sha256(abi.encodePacked(cids[i]));
        }

        // Build tree bottom-up
        while (layer.length > 1) {
            uint256 nextLen = (layer.length + 1) / 2;
            bytes32[] memory next = new bytes32[](nextLen);
            for (uint256 i = 0; i < nextLen; i++) {
                uint256 left  = i * 2;
                uint256 right = left + 1 < layer.length ? left + 1 : left; // duplicate last if odd
                next[i] = _hashPair(layer[left], layer[right]);
            }
            layer = next;
        }
        return layer[0];
    }

    // Verify a Merkle proof for a single leaf
    // proof: sibling hashes from leaf to root; proofFlags: true = sibling is on the right
    function merkleVerify(
        bytes32   leaf,
        bytes32[] memory proof,
        bool[]    memory proofFlags,
        bytes32   root
    ) public pure returns (bool) {
        bytes32 h = sha256(abi.encodePacked(leaf));
        for (uint256 i = 0; i < proof.length; i++) {
            h = proofFlags[i]
                ? _hashPair(h, proof[i])      // sibling on the right
                : _hashPair(proof[i], h);     // sibling on the left
        }
        return h == root;
    }

    function _hashPair(bytes32 a, bytes32 b) internal pure returns (bytes32) {
        // Sort so merkle root is deterministic regardless of leaf order
        return a <= b
            ? sha256(abi.encodePacked(a, b))
            : sha256(abi.encodePacked(b, a));
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    // Generate a Merkle proof for leaf at index `leafIndex`
    // Returns (proof[], proofFlags[]) — useful for off-chain proof generation test
    function merkleProof(bytes32[] memory cids, uint256 leafIndex)
        public pure returns (bytes32[] memory proof, bool[] memory flags)
    {
        require(leafIndex < cids.length, "index out of range");

        uint256 depth = 0;
        uint256 n = cids.length;
        while (n > 1) { n = (n + 1) / 2; depth++; }

        proof = new bytes32[](depth);
        flags = new bool[](depth);

        bytes32[] memory layer = new bytes32[](cids.length);
        for (uint256 i = 0; i < cids.length; i++) {
            layer[i] = sha256(abi.encodePacked(cids[i]));
        }

        uint256 idx = leafIndex;
        uint256 d = 0;
        while (layer.length > 1) {
            uint256 siblingIdx = (idx % 2 == 0)
                ? (idx + 1 < layer.length ? idx + 1 : idx)
                : idx - 1;

            proof[d]  = layer[siblingIdx];
            flags[d]  = (idx % 2 == 0); // true: sibling is on the right
            d++;
            idx = idx / 2;

            uint256 nextLen = (layer.length + 1) / 2;
            bytes32[] memory next = new bytes32[](nextLen);
            for (uint256 i = 0; i < nextLen; i++) {
                uint256 left  = i * 2;
                uint256 right = left + 1 < layer.length ? left + 1 : left;
                next[i] = _hashPair(layer[left], layer[right]);
            }
            layer = next;
        }
    }
}

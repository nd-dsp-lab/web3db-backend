// SPDX-License-Identifier: MIT
pragma solidity ^0.5.0;

/**
 * CIDBatchLog
 *
 * Purpose:
 * - Handle a batch of CIDs (modeled as bytes32 digests).
 * - Deterministically concatenate (packed) them and hash the result.
 * - Release that aggregate hash to the receiver (explicit on-chain event).
 * - Log creation / release / optional proof verification.
 */
contract CIDBatchLog {

    event BatchCreated(
        bytes32 indexed batchId,
        address indexed sender,
        address indexed receiver,
        uint cidCount,
        bytes32 aggregateHash,   // sha256(count || cid1 || cid2 || ...)
        bytes32 messageHash,    // optional
        uint timelock
    );

    event CIDsLogged(
        bytes32 indexed batchId,
        bytes32[] cids
    );

    event AggregateReleased(
        bytes32 indexed batchId,
        bytes32 aggregateHash
    );

    event BatchVerified(
        bytes32 indexed batchId,
        address indexed verifier
    );

    struct Batch {
        address sender;
        address receiver;
        uint cidCount;
        bytes32 aggregateHash;
        bytes32 messageHash;   // optional
        uint timelock;
        bool released;
        bool verified;
    }

    mapping(bytes32 => Batch) public batches;

    modifier futureTimelock(uint _time) {
        require(_time > now, "timelock time must be in the future");
        _;
    }

    modifier batchExists(bytes32 _batchId) {
        require(batches[_batchId].sender != address(0), "batch does not exist");
        _;
    }

    modifier onlySender(bytes32 _batchId) {
        require(msg.sender == batches[_batchId].sender, "only sender");
        _;
    }

    modifier onlyReceiver(bytes32 _batchId) {
        require(msg.sender == batches[_batchId].receiver, "only receiver");
        _;
    }

    modifier notReleased(bytes32 _batchId) {
        require(!batches[_batchId].released, "already released");
        _;
    }

    modifier notVerified(bytes32 _batchId) {
        require(!batches[_batchId].verified, "already verified");
        _;
    }

    /**
     * @dev Deterministic aggregation of a CID list.
     * Includes length to avoid ambiguity.
     */
    function computeAggregate(bytes32[] memory _cids)
        public
        pure
        returns (bytes32)
    {
        return sha256(abi.encodePacked(uint256(_cids.length), _cids));
    }

    function createBatch(
        address _receiver,
        bytes32[] calldata _cids,
        bytes32 _messageHash,
        uint _timelock
    )
        external
        futureTimelock(_timelock)
        returns (bytes32 batchId, bytes32 aggregateHash)
    {
        require(_receiver != address(0), "receiver=0");
        require(_cids.length > 0, "empty CID list");

        aggregateHash = computeAggregate(_cids);

        batchId = sha256(
            abi.encodePacked(
                msg.sender,
                _receiver,
                aggregateHash,
                _messageHash,
                _timelock
            )
        );

        require(batches[batchId].sender == address(0), "batch exists");

        batches[batchId] = Batch(
            msg.sender,
            _receiver,
            _cids.length,
            aggregateHash,
            _messageHash,
            _timelock,
            false,
            false
        );

        emit BatchCreated(
            batchId,
            msg.sender,
            _receiver,
            _cids.length,
            aggregateHash,
            _messageHash,
            _timelock
        );

        // Logs the full CID list without storing it in contract storage
        emit CIDsLogged(batchId, _cids);
    }

    function releaseAggregate(bytes32 _batchId)
        external
        batchExists(_batchId)
        onlySender(_batchId)
        notReleased(_batchId)
        returns (bytes32 aggregateHash)
    {
        Batch storage b = batches[_batchId];
        b.released = true;

        emit AggregateReleased(_batchId, b.aggregateHash);
        return b.aggregateHash;
    }

    /**
     * @dev Receiver (or anyone) proves a CID list matches the stored aggregate commitment.
     * Useful for "user independently checks inclusion" in your paper.
     */
    function verifyCIDs(bytes32 _batchId, bytes32[] calldata _cids)
        external
        batchExists(_batchId)
        notVerified(_batchId)
        returns (bool)
    {
        Batch storage b = batches[_batchId];
        require(_cids.length == b.cidCount, "CID count mismatch");
        require(computeAggregate(_cids) == b.aggregateHash, "aggregate mismatch");

        b.verified = true;
        emit BatchVerified(_batchId, msg.sender);
        return true;
    }

    function verifyMessage(bytes32 _batchId, bytes calldata _message)
        external
        batchExists(_batchId)
        returns (bool)
    {
        Batch storage b = batches[_batchId];
        require(b.messageHash != bytes32(0), "no messageHash set");
        require(keccak256(_message) == b.messageHash, "message hash mismatch");
        return true;
    }

    function getBatch(bytes32 _batchId)
        external
        view
        returns (
            address sender,
            address receiver,
            uint cidCount,
            bytes32 aggregateHash,
            bytes32 messageHash,
            uint timelock,
            bool released,
            bool verified
        )
    {
        Batch storage b = batches[_batchId];
        return (
            b.sender,
            b.receiver,
            b.cidCount,
            b.aggregateHash,
            b.messageHash,
            b.timelock,
            b.released,
            b.verified
        );
    }
}


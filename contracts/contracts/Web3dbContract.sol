// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "./EllipticCurve.sol";

contract Web3dbContract {

    // SECTION 1: Structs

    // --- Original: Access Control ---
    struct AccessPolicy {
        address subject;  // Address of the policy creator/owner (msg.sender)
        string tableName;      // Target table name (e.g., "patient_data")
        string policySql;      // SQL policy condition (e.g., "SELECT * FROM patient_data WHERE PatientID = '38'")
        address object; // Address of the querier
    }

    // --- Original: Schema Registry ---
    struct TableSchema {
        string tableName;
        string schemaJson;
    }

    // --- CIDBatchLog: Batch commitment ---
    struct Batch {
        address sender;
        address receiver;
        uint cidCount;
        bytes32 aggregateHash;  // sha256(count || cid1 || cid2 || ...)
        bytes32 messageHash;    // optional application-level message hash
        uint timelock;
        bool released;
        bool verified;
    }

    // --- SecLog: EC-committed log entry ---
    struct LogContract {
        address sender;
        address receiver;
        uint256 sk1x;       // x-coordinate of sk2*G, committed by sender
        uint256 sk1y;       // y-coordinate of sk2*G, committed by sender
        uint timelock;
        bytes32 messageHash;
        bool verified;
    }

    // SECTION 2: Constants (secp256k1 curve parameters, from SecLog)

    uint256 public constant GX = 0x79BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798;
    uint256 public constant GY = 0x483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8;
    uint256 public constant AA = 0;
    uint256 public constant PP = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F;

    // SECTION 3: State Variables

    // --- Original: Index CID registry ---
    mapping(string => string) private indexCIDs;

    // --- Original: Schema registry ---
    mapping(string => string) private tableSchemas;
    string[] private tableNames;
    mapping(string => bool) private tableExists;

    // --- Original: Access policies ---
    mapping(address => AccessPolicy[]) private accessPolicies;

    // --- CIDBatchLog: Batch storage (public for direct lookup by batchId) ---
    mapping(bytes32 => Batch) public batches;

    // --- SecLog: Log storage (public for direct lookup by logId) ---
    mapping(bytes32 => LogContract) public logs;

    // SECTION 4: Events

    // --- Original events ---
    event IndexUpdated(string attribute, string oldCID, string newCID);
    event BatchIndexUpdated(string[] attributes, string[] newCIDs);
    event SchemaUpdated(string tableName, string oldSchema, string newSchema);
    event AccessPolicyAdded(address indexed object, address indexed subject, string tableName, string policySql);
    event AccessPolicyRemoved(address indexed object, address indexed subject, string tableName, string policySql);

    // --- CIDBatchLog events ---
    event BatchCreated(
        bytes32 indexed batchId,
        address indexed sender,
        address indexed receiver,
        uint cidCount,
        bytes32 aggregateHash,
        bytes32 messageHash,
        uint timelock
    );
    event CIDsLogged(bytes32 indexed batchId, bytes32[] cids);
    event AggregateReleased(bytes32 indexed batchId, bytes32 aggregateHash);
    event BatchVerified(bytes32 indexed batchId, address indexed verifier);

    // --- SecLog events ---
    event LogEntryNew(
        bytes32 indexed logId,
        address indexed sender,
        address indexed receiver,
        uint256 sk1x,
        uint256 sk1y,
        bytes32 messageHash,
        uint timelock
    );
    event LogVerified(bytes32 indexed logId);

    // SECTION 5: Modifiers

    // Shared by both CIDBatchLog and SecLog — single merged definition
    modifier futureTimelock(uint _time) {
        require(_time > block.timestamp, "timelock time must be in the future");
        _;
    }

    // --- CIDBatchLog modifiers ---
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

    // Renamed from notVerified (collision with logNotVerified)
    modifier batchNotVerified(bytes32 _batchId) {
        require(!batches[_batchId].verified, "already verified");
        _;
    }

    // --- SecLog modifiers ---
    modifier logExists(bytes32 _logId) {
        require(haveLog(_logId), "Log does not exist");
        _;
    }

    // Renamed from notVerified (collision with batchNotVerified)
    modifier logNotVerified(bytes32 _logId) {
        require(!logs[_logId].verified, "Already verified");
        _;
    }

    // SECTION 6: Original — Index CID Management

    function updateIndexCID(
        string memory attribute,
        string memory newCID
    ) public {
        string memory oldCID = indexCIDs[attribute];
        indexCIDs[attribute] = newCID;
        emit IndexUpdated(attribute, oldCID, newCID);
    }

    function batchUpdateIndexCIDs(
        string[] memory attributes,
        string[] memory newCIDs
    ) public {
        require(
            attributes.length == newCIDs.length,
            "Arrays must be same length"
        );

        for (uint i = 0; i < attributes.length; i++) {
            string memory oldCID = indexCIDs[attributes[i]];
            indexCIDs[attributes[i]] = newCIDs[i];
            emit IndexUpdated(attributes[i], oldCID, newCIDs[i]);
        }

        emit BatchIndexUpdated(attributes, newCIDs);
    }

    function getIndexCID(
        string memory attribute
    ) public view returns (string memory) {
        return indexCIDs[attribute];
    }

    function batchGetIndexCIDs(
        string[] memory attributes
    ) public view returns (string[] memory) {
        string[] memory results = new string[](attributes.length);

        for (uint i = 0; i < attributes.length; i++) {
            results[i] = indexCIDs[attributes[i]];
        }

        return results;
    }

    function removeIndex(string memory attribute) public {
        string memory oldCID = indexCIDs[attribute];
        delete indexCIDs[attribute];
        emit IndexUpdated(attribute, oldCID, "");
    }

    // SECTION 7: Original — Schema Management

    function updateTableSchema(
        string memory tableName,
        string memory schemaJson
    ) public {
        string memory oldSchema = tableSchemas[tableName];
        tableSchemas[tableName] = schemaJson;

        if (!tableExists[tableName]) {
            tableNames.push(tableName);
            tableExists[tableName] = true;
        }

        emit SchemaUpdated(tableName, oldSchema, schemaJson);
    }

    function getTableSchema(
        string memory tableName
    ) public view returns (string memory) {
        return tableSchemas[tableName];
    }

    function batchGetTableSchemas(
        string[] memory tableNamesList
    ) public view returns (string[] memory) {
        string[] memory results = new string[](tableNamesList.length);

        for (uint i = 0; i < tableNamesList.length; i++) {
            results[i] = tableSchemas[tableNamesList[i]];
        }

        return results;
    }

    function getAllTableNames() public view returns (string[] memory) {
        return tableNames;
    }

    function getAllTableSchemas() public view returns (TableSchema[] memory) {
        TableSchema[] memory results = new TableSchema[](tableNames.length);

        for (uint i = 0; i < tableNames.length; i++) {
            results[i] = TableSchema({
                tableName: tableNames[i],
                schemaJson: tableSchemas[tableNames[i]]
            });
        }

        return results;
    }

    function removeTableSchema(string memory tableName) public {
        string memory oldSchema = tableSchemas[tableName];
        delete tableSchemas[tableName];

        if (tableExists[tableName]) {
            tableExists[tableName] = false;

            for (uint i = 0; i < tableNames.length; i++) {
                if (keccak256(abi.encodePacked(tableNames[i])) == keccak256(abi.encodePacked(tableName))) {
                    tableNames[i] = tableNames[tableNames.length - 1];
                    tableNames.pop();
                    break;
                }
            }
        }

        emit SchemaUpdated(tableName, oldSchema, "");
    }

    // SECTION 8: Original — Access Policy Management

    function addAccessPolicy(
        address subject,
        address object,
        string memory tableName,
        string memory policySql
    ) public {
        accessPolicies[object].push(AccessPolicy({
            subject: subject,
            tableName: tableName,
            policySql: policySql,
            object: object
        }));
        emit AccessPolicyAdded(object, subject, tableName, policySql);
    }

    function getAccessPolicies(
        address objectAddress
    ) public view returns (AccessPolicy[] memory) {
        return accessPolicies[objectAddress];
    }

    function getPolicyCount(address objectAddress) public view returns (uint) {
        return accessPolicies[objectAddress].length;
    }

    function removeAccessPolicy(address objectAddress, uint policyIndex) public {
        require(policyIndex < accessPolicies[objectAddress].length, "Invalid policy index");

        AccessPolicy memory removedPolicy = accessPolicies[objectAddress][policyIndex];

        accessPolicies[objectAddress][policyIndex] = accessPolicies[objectAddress][accessPolicies[objectAddress].length - 1];
        accessPolicies[objectAddress].pop();

        emit AccessPolicyRemoved(objectAddress, removedPolicy.subject, removedPolicy.tableName, removedPolicy.policySql);
    }

    function removeAllAccessPolicies(address objectAddress) public {
        AccessPolicy[] memory removed = accessPolicies[objectAddress];
        delete accessPolicies[objectAddress];
        for (uint i = 0; i < removed.length; i++) {
            emit AccessPolicyRemoved(objectAddress, removed[i].subject, removed[i].tableName, removed[i].policySql);
        }
    }

    // SECTION 9: CIDBatchLog — CID Batch Commitment & Verification

    /**
     * @dev Deterministically aggregate a CID list into a single hash.
     *      Includes a length prefix to prevent length-extension ambiguity.
     *      Result: sha256(uint256(count) || cid1 || cid2 || ...)
     */
    function computeAggregate(bytes32[] memory _cids)
        public
        pure
        returns (bytes32)
    {
        return sha256(abi.encodePacked(uint256(_cids.length), _cids));
    }

    /**
     * @dev Create a batch: commit to an ordered list of CIDs via their aggregate hash.
     *      The full CID list is emitted in CIDsLogged but NOT stored on-chain,
     *      keeping storage costs low while preserving a full auditable event log.
     * @param _receiver     Address authorised to verify the batch.
     * @param _cids         Ordered list of CIDs as bytes32 digests.
     * @param _messageHash  Optional application-level message hash (pass bytes32(0) to omit).
     * @param _timelock     Unix timestamp — batch is valid until this time.
     * @return batchId       Deterministic ID derived from all batch parameters.
     * @return aggregateHash sha256 commitment over the full CID list.
     */
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

        // Log the full CID list via event (not in contract storage)
        emit CIDsLogged(batchId, _cids);
    }

    /**
     * @dev Sender explicitly releases the aggregate hash on-chain as a delivery signal.
     *      Marks the batch as released; the receiver can now act on the commitment.
     */
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
     * @dev Anyone can verify a CID list matches the stored aggregate commitment.
     *      Primary use: receiver independently confirms received CIDs are exactly
     *      the set the sender committed to at batch creation time.
     */
    function verifyCIDs(bytes32 _batchId, bytes32[] calldata _cids)
        external
        batchExists(_batchId)
        batchNotVerified(_batchId)
        returns (bool)
    {
        Batch storage b = batches[_batchId];
        require(_cids.length == b.cidCount, "CID count mismatch");
        require(computeAggregate(_cids) == b.aggregateHash, "aggregate mismatch");

        b.verified = true;
        emit BatchVerified(_batchId, msg.sender);
        return true;
    }

    /**
     * @dev Verify that a raw message matches the messageHash stored in the batch.
     */
    function verifyBatchMessage(bytes32 _batchId, bytes calldata _message)
        external
        view
        batchExists(_batchId)
        returns (bool)
    {
        Batch storage b = batches[_batchId];
        require(b.messageHash != bytes32(0), "no messageHash set");
        require(keccak256(_message) == b.messageHash, "message hash mismatch");
        return true;
    }

    /**
     * @dev Retrieve full batch metadata by ID.
     */
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

    // SECTION 10: SecLog — EC Key-Committed Secure Logging

    /**
     * @dev Internal helper — returns true if a log entry for _logId exists.
     */
    function haveLog(bytes32 _logId)
        internal
        view
        returns (bool)
    {
        return logs[_logId].sender != address(0);
    }

    /**
     * @dev Create a new log entry committing to an elliptic curve public key point (sk1x, sk1y).
     *      The point must equal sk2 * G where sk2 is the sender's private scalar.
     *      The receiver proves knowledge of sk2 later via verifyLog().
     *
     * @param _receiver     Address that will verify the log.
     * @param _sk1x         x-coordinate of the committed EC public key (= sk2 * Gx).
     * @param _sk1y         y-coordinate of the committed EC public key (= sk2 * Gy).
     * @param _messageHash  keccak256 hash of the message to be logged.
     * @param _timelock     Unix timestamp — log must be created before this time.
     * @return logId  Deterministic ID derived from all log parameters.
     */
    function newLog(
        address _receiver,
        uint256 _sk1x,
        uint256 _sk1y,
        bytes32 _messageHash,
        uint _timelock
    )
        external
        futureTimelock(_timelock)
        returns (bytes32 logId)
    {
        logId = keccak256(
            abi.encodePacked(
                msg.sender,
                _receiver,
                _sk1x,
                _sk1y,
                _messageHash,
                _timelock
            )
        );

        require(!haveLog(logId), "Log already exists");

        logs[logId] = LogContract(
            msg.sender,
            _receiver,
            _sk1x,
            _sk1y,
            _timelock,
            _messageHash,
            false
        );

        emit LogEntryNew(
            logId,
            msg.sender,
            _receiver,
            _sk1x,
            _sk1y,
            _messageHash,
            _timelock
        );
    }

    /**
     * @dev Receiver verifies the log by supplying the private scalar sk2 and the original message.
     *      Two proofs must pass simultaneously:
     *        1. EC proof:      sk2 * G == (sk1x, sk1y)       — proves knowledge of the secret
     *        2. Message proof: keccak256(message) == messageHash — proves message integrity
     *
     *      Note: EllipticCurve.ecMul() on secp256k1 is compute-intensive.
     *      Use gas: 3000000 when calling this from the Python wrapper.
     *
     * @param _logId   The log entry to verify.
     * @param _sk2     The private scalar whose public key matches the committed (sk1x, sk1y).
     * @param _message The original plaintext message whose hash was committed at creation.
     */
    function verifyLog(
        bytes32 _logId,
        uint256 _sk2,
        bytes calldata _message
    )
        external
        logExists(_logId)
        logNotVerified(_logId)
        returns (bool)
    {
        LogContract storage log = logs[_logId];
        require(log.receiver == msg.sender, "Only receiver can verify");

        (uint256 sk2x, uint256 sk2y) = EllipticCurve.ecMul(_sk2, GX, GY, AA, PP);
        require(log.sk1x == sk2x && log.sk1y == sk2y, "Invalid sk2 proof");

        require(keccak256(_message) == log.messageHash, "Message hash mismatch");

        log.verified = true;
        emit LogVerified(_logId);
        return true;
    }

    /**
     * @dev Retrieve full log metadata by ID.
     */
    function getLog(bytes32 _logId)
        external
        view
        returns (
            address sender,
            address receiver,
            uint256 sk1x,
            uint256 sk1y,
            uint timelock,
            bytes32 messageHash,
            bool verified
        )
    {
        LogContract storage l = logs[_logId];
        return (
            l.sender,
            l.receiver,
            l.sk1x,
            l.sk1y,
            l.timelock,
            l.messageHash,
            l.verified
        );
    }
}

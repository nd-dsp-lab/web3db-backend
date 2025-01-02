// SPDX-License-Identifier: MIT
pragma solidity ^0.8.28;

contract Web3DBHashManagement {
    // Struct to store hash record details
    struct HashRecord {
        string hash; // IPFS hash
        string prevHash; // Previous state hash
        string recordType; // create/insert/update/delete/share
        uint256 timestamp; // Operation timestamp
        string flag; // initial/latest/stable
        uint256 rowCount; // Number of rows
    }

    // Main storage mapping: user => table => partition => records[]
    mapping(address => mapping(string => mapping(string => HashRecord[])))
        private hashMappings;

    // Track tables owned by users
    mapping(address => string[]) private userTables;

    // Track shared access: owner => table => authorized users
    mapping(address => mapping(string => mapping(address => bool)))
        private sharedAccess;

    // Events
    event HashMappingAdded(
        address indexed user,
        string table,
        string partition,
        string hash,
        string recordType
    );

    event TableShared(
        address indexed fromUser,
        address indexed toUser,
        string table
    );

    // Add new hash mapping
    function addHashMapping(
        string memory tableName,
        string memory partition,
        string memory hash,
        string memory prevHash,
        string memory recordType,
        string memory flag,
        uint256 rowCount
    ) public returns (bool) {
        HashRecord memory record = HashRecord({
            hash: hash,
            prevHash: prevHash,
            recordType: recordType,
            timestamp: block.timestamp,
            flag: flag,
            rowCount: rowCount
        });

        hashMappings[msg.sender][tableName][partition].push(record);

        if (hashMappings[msg.sender][tableName][partition].length == 1) {
            userTables[msg.sender].push(tableName);
        }

        emit HashMappingAdded(
            msg.sender,
            tableName,
            partition,
            hash,
            recordType
        );

        return true;
    }

    // Get latest hash
    function getLatestHash(
        address user,
        string memory tableName,
        string memory partition
    ) public view returns (string memory) {
        HashRecord[] storage records = hashMappings[user][tableName][partition];
        if (records.length > 0) {
            return records[records.length - 1].hash;
        }
        return "";
    }

    // Get hash history
    function getHashHistory(
        address user,
        string memory tableName,
        string memory partition
    )
        public
        view
        returns (
            string[] memory hashes,
            string[] memory prevHashes,
            string[] memory types,
            uint256[] memory timestamps,
            string[] memory flags,
            uint256[] memory rowCounts
        )
    {
        HashRecord[] storage records = hashMappings[user][tableName][partition];
        uint256 length = records.length;

        hashes = new string[](length);
        prevHashes = new string[](length);
        types = new string[](length);
        timestamps = new uint256[](length);
        flags = new string[](length);
        rowCounts = new uint256[](length);

        for (uint256 i = 0; i < length; i++) {
            hashes[i] = records[i].hash;
            prevHashes[i] = records[i].prevHash;
            types[i] = records[i].recordType;
            timestamps[i] = records[i].timestamp;
            flags[i] = records[i].flag;
            rowCounts[i] = records[i].rowCount;
        }

        return (hashes, prevHashes, types, timestamps, flags, rowCounts);
    }

    // Share table with another user
    function shareTable(
        string memory tableName,
        address toUser
    ) public returns (bool) {
        require(
            hashMappings[msg.sender][tableName]["0"].length > 0,
            "Table does not exist"
        );

        sharedAccess[msg.sender][tableName][toUser] = true;

        emit TableShared(msg.sender, toUser, tableName);
        return true;
    }

    // Revoke shared access
    function revokeAccess(
        string memory tableName,
        address fromUser
    ) public returns (bool) {
        sharedAccess[msg.sender][tableName][fromUser] = false;
        return true;
    }

    // Get user's tables
    function getUserTables(address user) public view returns (string[] memory) {
        return userTables[user];
    }
}

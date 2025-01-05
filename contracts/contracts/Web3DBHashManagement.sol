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

    // Struct to store CID sharing information
    struct SharedCID {
        string cid;
        address owner;
        bool isValid;
    }

    // Main storage mapping: user => table => partition => records[]
    mapping(address => mapping(string => mapping(string => HashRecord[])))
        private hashMappings;

    // Track tables owned by users
    mapping(address => string[]) private userTables;

    // Track shared CIDs: user => table => SharedCID[]
    mapping(address => mapping(string => SharedCID[])) private sharedCIDs;

    // Events
    event HashMappingAdded(
        address indexed user,
        string table,
        string partition,
        string hash,
        string recordType
    );

    event CIDShared(
        address indexed owner,
        address indexed sharedWith,
        string table,
        string cid
    );

    // -------------------------------------------------------------------------
    // Private helper to check if msg.sender owns the CID in any table/partition
    // -------------------------------------------------------------------------
    function isCIDOwner(string memory cid) private view returns (bool) {
        // Check through user's tables
        string[] memory tables = userTables[msg.sender];

        for (uint256 i = 0; i < tables.length; i++) {
            HashRecord[] storage records = hashMappings[msg.sender][tables[i]][
                "0"
            ];

            for (uint256 j = 0; j < records.length; j++) {
                if (
                    keccak256(bytes(records[j].hash)) == keccak256(bytes(cid))
                ) {
                    return true;
                }
            }
        }
        return false;
    }

    // -------------------------------------------------------------------------
    // Add new hash mapping
    // -------------------------------------------------------------------------
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

    // -------------------------------------------------------------------------
    // Share CID with a specific user
    // -------------------------------------------------------------------------
    function shareCID(
        address user,
        string memory tableName,
        string memory cid
    ) public returns (bool) {
        require(user != address(0), "Invalid user address");
        require(bytes(cid).length > 0, "Invalid CID");
        require(isCIDOwner(cid), "Only CID owner can share it");

        SharedCID memory sharedCID = SharedCID({
            cid: cid,
            owner: msg.sender,
            isValid: true
        });

        sharedCIDs[user][tableName].push(sharedCID);

        emit CIDShared(msg.sender, user, tableName, cid);
        return true;
    }

    // -------------------------------------------------------------------------
    // (Modified!) Get shared CIDs for any user
    // -------------------------------------------------------------------------
    function getSharedCIDs(
        address user,
        string memory tableName
    ) public view returns (string[] memory cids, address[] memory owners) {
        // Retrieve the shared records stored under `user`
        SharedCID[] storage cidRecords = sharedCIDs[user][tableName];
        uint256 validCount = 0;

        // First count valid records
        for (uint256 i = 0; i < cidRecords.length; i++) {
            if (cidRecords[i].isValid) {
                validCount++;
            }
        }

        // Initialize arrays with the count of valid records
        cids = new string[](validCount);
        owners = new address[](validCount);

        // Fill arrays with valid records
        uint256 currentIndex = 0;
        for (uint256 i = 0; i < cidRecords.length; i++) {
            if (cidRecords[i].isValid) {
                cids[currentIndex] = cidRecords[i].cid;
                owners[currentIndex] = cidRecords[i].owner;
                currentIndex++;
            }
        }

        return (cids, owners);
    }

    // -------------------------------------------------------------------------
    // Revoke shared CID
    // -------------------------------------------------------------------------
    function revokeCIDAccess(
        address user,
        string memory tableName,
        string memory cid
    ) public returns (bool) {
        SharedCID[] storage cidRecords = sharedCIDs[user][tableName];
        for (uint256 i = 0; i < cidRecords.length; i++) {
            if (
                keccak256(bytes(cidRecords[i].cid)) == keccak256(bytes(cid)) &&
                cidRecords[i].owner == msg.sender
            ) {
                cidRecords[i].isValid = false;
                return true;
            }
        }
        return false;
    }

    // -------------------------------------------------------------------------
    // Get latest hash
    // -------------------------------------------------------------------------
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

    // -------------------------------------------------------------------------
    // Get hash history
    // -------------------------------------------------------------------------
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

    // -------------------------------------------------------------------------
    // Get user's tables
    // -------------------------------------------------------------------------
    function getUserTables(address user) public view returns (string[] memory) {
        return userTables[user];
    }
}

// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract Web3dbContract {
    // Struct to store access policies
    struct AccessPolicy {
        address ownerAddress;
        string tableName;
        string policySql;
    }
    
    // Mapping from attribute name (like "PatientID") to its current index CID
    mapping(string => string) private indexCIDs;
    
    // Mapping from table name to its schema (stored as JSON string)
    mapping(string => string) private tableSchemas;
    
    // Mapping from wallet address to their access policies
    mapping(address => AccessPolicy[]) private accessPolicies;

    // Event to log index updates
    event IndexUpdated(string attribute, string oldCID, string newCID);

    // Event to log batch updates
    event BatchIndexUpdated(string[] attributes, string[] newCIDs);
    
    // Event to log schema updates
    event SchemaUpdated(string tableName, string oldSchema, string newSchema);
    
    // Event to log access policy updates
    event AccessPolicyAdded(address indexed walletAddress, string tableName, string policySql);
    event AccessPolicyRemoved(address indexed walletAddress, string tableName);

    // Function to update the index CID for an attribute
    function updateIndexCID(
        string memory attribute,
        string memory newCID
    ) public {
        string memory oldCID = indexCIDs[attribute];
        indexCIDs[attribute] = newCID;
        emit IndexUpdated(attribute, oldCID, newCID);
    }

    // Function to update multiple index CIDs in one transaction
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

    // Function to get the current index CID for an attribute
    function getIndexCID(
        string memory attribute
    ) public view returns (string memory) {
        return indexCIDs[attribute];
    }

    // Function to get multiple index CIDs in one call
    function batchGetIndexCIDs(
        string[] memory attributes
    ) public view returns (string[] memory) {
        string[] memory results = new string[](attributes.length);

        for (uint i = 0; i < attributes.length; i++) {
            results[i] = indexCIDs[attributes[i]];
        }

        return results;
    }

    // Function to remove an index CID
    function removeIndex(string memory attribute) public {
        string memory oldCID = indexCIDs[attribute];
        delete indexCIDs[attribute];
        emit IndexUpdated(attribute, oldCID, "");
    }
    
    // Schema management functions
    
    // Function to store/update a table schema
    function updateTableSchema(
        string memory tableName,
        string memory schemaJson
    ) public {
        string memory oldSchema = tableSchemas[tableName];
        tableSchemas[tableName] = schemaJson;
        emit SchemaUpdated(tableName, oldSchema, schemaJson);
    }
    
    // Function to get a table schema
    function getTableSchema(
        string memory tableName
    ) public view returns (string memory) {
        return tableSchemas[tableName];
    }
    
    // Function to get multiple table schemas in one call
    function batchGetTableSchemas(
        string[] memory tableNames
    ) public view returns (string[] memory) {
        string[] memory results = new string[](tableNames.length);
        
        for (uint i = 0; i < tableNames.length; i++) {
            results[i] = tableSchemas[tableNames[i]];
        }
        
        return results;
    }
    
    // Function to remove a table schema
    function removeTableSchema(string memory tableName) public {
        string memory oldSchema = tableSchemas[tableName];
        delete tableSchemas[tableName];
        emit SchemaUpdated(tableName, oldSchema, "");
    }
    
    // Access policy management functions
    
    // Function to add an access policy for a wallet address
    function addAccessPolicy(
        address walletAddress,
        string memory tableName,
        string memory policySql
    ) public {
        accessPolicies[walletAddress].push(AccessPolicy({
            ownerAddress: msg.sender,
            tableName: tableName,
            policySql: policySql
        }));
        emit AccessPolicyAdded(walletAddress, tableName, policySql);
    }
    
    // Function to get all access policies for a wallet address
    function getAccessPolicies(
        address walletAddress
    ) public view returns (AccessPolicy[] memory) {
        return accessPolicies[walletAddress];
    }
    
    // Function to get the count of policies for a wallet address
    function getPolicyCount(address walletAddress) public view returns (uint) {
        return accessPolicies[walletAddress].length;
    }
    
    // Function to remove a specific policy by index
    function removeAccessPolicy(address walletAddress, uint policyIndex) public {
        require(policyIndex < accessPolicies[walletAddress].length, "Invalid policy index");
        
        AccessPolicy memory removedPolicy = accessPolicies[walletAddress][policyIndex];
        
        // Move the last element to the deleted spot and remove the last element
        accessPolicies[walletAddress][policyIndex] = accessPolicies[walletAddress][accessPolicies[walletAddress].length - 1];
        accessPolicies[walletAddress].pop();
        
        emit AccessPolicyRemoved(walletAddress, removedPolicy.tableName);
    }
    
    // Function to remove all policies for a wallet address
    function removeAllAccessPolicies(address walletAddress) public {
        delete accessPolicies[walletAddress];
        emit AccessPolicyRemoved(walletAddress, "ALL");
    }
}
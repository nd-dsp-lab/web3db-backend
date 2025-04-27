// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract IndexState {
    // Mapping from attribute name (like "PatientID") to its current index CID
    mapping(string => string) private indexCIDs;

    // Event to log index updates
    event IndexUpdated(string attribute, string oldCID, string newCID);

    // Function to update the index CID for an attribute
    function updateIndexCID(
        string memory attribute,
        string memory newCID
    ) public {
        string memory oldCID = indexCIDs[attribute];
        indexCIDs[attribute] = newCID;
        emit IndexUpdated(attribute, oldCID, newCID);
    }

    // Function to get the current index CID for an attribute
    function getIndexCID(
        string memory attribute
    ) public view returns (string memory) {
        return indexCIDs[attribute];
    }

    // Function to remove an index CID
    function removeIndex(string memory attribute) public {
        string memory oldCID = indexCIDs[attribute];
        delete indexCIDs[attribute];
        emit IndexUpdated(attribute, oldCID, "");
    }
}

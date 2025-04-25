// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

contract IndexStorage {
    string private currentIndexCID;
    
    // Event to log when the index is updated
    event IndexUpdated(string previousCID, string newCID);
    
    // Function to update the current index CID
    function updateCurrentIndex(string memory _newCID) public {
        string memory oldCID = currentIndexCID;
        currentIndexCID = _newCID;
        emit IndexUpdated(oldCID, _newCID);
    }
    
    // Function to get the current index CID
    function getCurrentIndex() public view returns (string memory) {
        return currentIndexCID;
    }
}
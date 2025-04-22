// SPDX-License-Identifier: MIT
pragma solidity ^0.8.17;

contract IndexManagement {
    // Structure to store index update information
    struct IndexUpdate {
        bytes32 h_onchain; // Current hash of the index
        bytes32 h_prev; // Previous hash of the index
        bytes32[] d_onchain; // List of delta CIDs for this epoch
        uint256 epoch_version;
        uint256 timestamp;
        address updater; // Address that submitted this update
    }

    // Contract state variables
    bytes32 public currentIndexHash; // Current hash of the index (h_onchain)
    uint256 public currentEpochVersion; // Current epoch version

    // Ring buffer of index updates
    uint8 private constant RING_BUFFER_SIZE = 16; // to store max 16 recent epochs
    mapping(uint256 => IndexUpdate) public updates; // Ring buffer of updates

    // Events
    event IndexUpdated(
        bytes32 indexed h_onchain,
        bytes32 indexed h_prev,
        uint256 indexed epoch_version,
        bytes32[] d_onchain,
        uint256 timestamp,
        address updater
    );

    // Errors
    error InvalidPreviousHash();
    error EmptyDeltaList();

    constructor(bytes32 _initialIndexHash) {
        currentIndexHash = _initialIndexHash;
        currentEpochVersion = 0;
    }

    function postNewEpoch(
        bytes32 h_prev,
        bytes32 h_new,
        bytes32[] calldata deltaCIDs
    ) external {
        // Check that h_prev matches the current index hash (optimistic concurrency control)
        if (h_prev != currentIndexHash) {
            revert InvalidPreviousHash();
        }

        // Validate delta list is not empty
        if (deltaCIDs.length == 0) {
            revert EmptyDeltaList();
        }

        // Increment epoch version
        currentEpochVersion += 1;

        // Store the update in the ring buffer
        uint256 bufferIndex = currentEpochVersion % RING_BUFFER_SIZE;
        updates[bufferIndex] = IndexUpdate({
            h_onchain: h_new,
            h_prev: h_prev,
            d_onchain: deltaCIDs,
            epoch_version: currentEpochVersion,
            timestamp: block.timestamp,
            updater: msg.sender
        });

        // Update the current index hash
        currentIndexHash = h_new;

        // Emit event
        emit IndexUpdated(
            h_new,
            h_prev,
            currentEpochVersion,
            deltaCIDs,
            block.timestamp,
            msg.sender
        );
    }

    // get the latest onchain epoch
    function getLatestEpoch()
        external
        view
        returns (
            bytes32 h_onchain,
            bytes32 h_prev,
            bytes32[] memory deltas,
            uint256 epoch_version,
            uint256 timestamp,
            address updater
        )
    {
        uint256 bufferIndex = currentEpochVersion % RING_BUFFER_SIZE;
        IndexUpdate storage latestUpdate = updates[bufferIndex];

        return (
            latestUpdate.h_onchain,
            latestUpdate.h_prev,
            latestUpdate.d_onchain,
            latestUpdate.epoch_version,
            latestUpdate.timestamp,
            latestUpdate.updater
        );
    }

    // get delta CIDs for a specific epoch
    function getDeltaCIDs(
        uint256 epoch_version
    ) external view returns (bytes32[] memory) {
        uint256 bufferIndex = epoch_version % RING_BUFFER_SIZE;

        // Check if the requested epoch exists in the buffer
        if (
            epoch_version > currentEpochVersion ||
            currentEpochVersion - epoch_version >= RING_BUFFER_SIZE
        ) {
            return new bytes32[](0);
        }

        return updates[bufferIndex].d_onchain;
    }
}

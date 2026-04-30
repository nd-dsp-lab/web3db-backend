// SPDX-License-Identifier: MIT

pragma solidity ^0.5.0;

import "./EllipticCurve.sol";

contract SecLog {

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

    struct LogContract {
        address sender;
        address receiver;
        uint256 sk1x;
        uint256 sk1y; //the pre-set results calculated by the buyer(sender) for future checking
        uint timelock; // UNIX timestamp seconds - locked UNTIL this time
        bytes32 messageHash; // the hash of the message to be logged
        bool verified; // true if the log was verified
    }
    
    uint256 public constant GX = 0x79BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798;
    uint256 public constant GY = 0x483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8;
    uint256 public constant AA = 0;
    uint256 public constant PP = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F;
    //uint256 public constant sk1x = 112711660439710606056748659173929673102114977341539408544630613555209775888121;
    //uint256 public constant sk1y = 25583027980570883691656905877401976406448868254816295069919888960541586679410;

    modifier futureTimelock(uint _time) {
        // only requirement is the timelock time is after the last blocktime (now).
        // probably want something a bit further in the future then this.
        // but this is still a useful sanity check:
        require(_time > now, "timelock time must be in the future");
        _;
    }


    function haveLog(bytes32 _logId)
        internal
        view
        returns (bool)
    {
        return logs[_logId].sender != address(0);
    }

    modifier logExists(bytes32 _logId) {
        require(haveLog(_logId), "Log does not exist");
        _;
    }

    modifier sklockMatches(bytes32 _logId, uint256 sk2) {
        uint256 sk2x;
        uint256 sk2y;
        (sk2x, sk2y) = EllipticCurve.ecMul(sk2,GX,GY,AA,PP);                  //sk2*G(X,Y) 
        require(logs[_logId].sk1x == sk2x, "sk2 does not match");
        require(logs[_logId].sk1y == sk2y, "sk2 does not match");
        _;
    }

    mapping (bytes32 => LogContract) public logs;

    function newLog(address _receiver, uint256 _sk1x, uint256 _sk1y, bytes32 _messageHash, uint _timelock)
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

        // Reject if a contract already exists with the same parameters. The
        // sender must change one of these parameters to create a new distinct
        // contract.
        if (haveLog(logId))
            revert("Log already exists");

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

    modifier notVerified(bytes32 _logId) {
        require(!logs[_logId].verified, "Already verified");
        _;
    }


     function verifyLog(bytes32 _logId, uint256 _sk2, bytes calldata _message)
         external
         logExists(_logId)
         notVerified(_logId)
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
}

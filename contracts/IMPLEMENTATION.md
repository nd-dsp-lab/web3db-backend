# Web3dbContract — Implementation Reference

This document gives a detailed technical account of the merged `Web3dbContract.sol` and every layer that touches it. It covers the original three responsibilities and the two newly integrated modules — **CIDBatchLog** and **SecLog** — together with the supporting `EllipticCurve.sol` library, the Python wrapper, and the unit test suite.

---

## Table of Contents

1. [Overview](#1-overview)
2. [File Map](#2-file-map)
3. [Data Structures](#3-data-structures)
4. [State Variables](#4-state-variables)
5. [Events](#5-events)
6. [Modifiers](#6-modifiers)
7. [Original Functions — Index CIDs, Schemas, Access Policies](#7-original-functions)
8. [CIDBatchLog Integration](#8-cidbatchlog-integration)
9. [SecLog Integration](#9-seclog-integration)
10. [EllipticCurve.sol — Compatibility Changes](#10-ellipticcurvesol--compatibility-changes)
11. [Python Wrapper](#11-python-wrapper)
12. [Contract Size and Deployment](#12-contract-size-and-deployment)
13. [Test Suite](#13-test-suite)
14. [Design Constraints and Gotchas](#14-design-constraints-and-gotchas)

---

## 1. Overview

`Web3dbContract.sol` is a single Solidity contract deployed on Ethereum Sepolia testnet that serves **five distinct purposes** for the MtDB research system:

| Responsibility | Mapping Key | Value Stored | Added In |
|---|---|---|---|
| Index CID registry | `string` attribute key | `string` IPFS CID | Original |
| Table schema registry | `string` table name | `string` JSON schema | Original |
| Access policies | `address` querier | `AccessPolicy[]` array | Original |
| CID Batch Log | `bytes32` batchId | `Batch` struct | New |
| Secure Log | `bytes32` logId | `LogContract` struct | New |

The contract has **no access control** — any Ethereum address can call any function. Security enforcement is delegated to the Intel SGX enclave running the application. The contract is the auditable commitment layer only.

### Why CIDBatchLog and SecLog were added

- **CIDBatchLog** provides a verifiable, storage-efficient commitment to an ordered set of IPFS CIDs. The full CID list is published via an event (not stored in contract state), so the on-chain footprint is a constant 32 bytes regardless of batch size.
- **SecLog** provides a cryptographic log entry backed by an elliptic curve public key commitment. The sender commits to a secp256k1 public key point; the receiver later proves knowledge of the corresponding private scalar. This creates a non-repudiable delivery receipt.

Both modules were originally standalone contracts (`contracts/reference/CIDBatchLog.sol`, `contracts/reference/SecLog.sol`) and have been merged into `Web3dbContract.sol` to avoid managing multiple deployments.

---

## 2. File Map

| File | Role |
|---|---|
| `contracts/contracts/Web3dbContract.sol` | Merged on-chain contract (635 lines, 10 sections) |
| `contracts/contracts/EllipticCurve.sol` | secp256k1 EC math library — imported by the contract |
| `contracts/reference/CIDBatchLog.sol` | Original standalone CIDBatchLog — reference only, not compiled |
| `contracts/reference/SecLog.sol` | Original standalone SecLog — reference only, not compiled |
| `app/scripts/web3db_contract.py` | Python wrapper class `Web3dbContract` (web3.py, ~1750 lines) |
| `contracts/test/Web3dbContract.test.js` | 41-test Hardhat/Chai suite (ethers v6) |
| `contracts/hardhat.config.js` | Hardhat config — optimizer enabled, local size limit lifted |

---

## 3. Data Structures

### 3.1 `AccessPolicy`

```solidity
struct AccessPolicy {
    address subject;    // owner of the data (policy creator)
    string tableName;   // target table (e.g. "patient_data")
    string policySql;   // raw SQL condition string
    address object;     // querier granted access by this policy
}
```

Stored in `mapping(address => AccessPolicy[]) private accessPolicies` keyed by the **querier** (`object`) address. At query time the application fetches all policies for a querier's address, rewrites the SQL inside the SGX enclave, and enforces row-level access.

### 3.2 `TableSchema`

```solidity
struct TableSchema {
    string tableName;
    string schemaJson;   // CREATE TABLE SQL or JSON schema
}
```

Used only as a return type in `getAllTableSchemas()`. Storage uses a plain `mapping(string => string)`.

### 3.3 `Batch`

```solidity
struct Batch {
    address sender;          // address that called createBatch
    address receiver;        // address authorised to trigger verification
    uint    cidCount;        // number of CIDs committed
    bytes32 aggregateHash;   // sha256(uint256(count) || cid1 || ... || cidN)
    bytes32 messageHash;     // optional application message hash (bytes32(0) = omitted)
    uint    timelock;        // Unix timestamp — batch is valid until this time
    bool    released;        // true once sender calls releaseAggregate
    bool    verified;        // true once verifyCIDs succeeds
}
```

`released` and `verified` are independent flags. A batch can be verified without being released and vice versa. Neither flag can be reset once set.

### 3.4 `LogContract`

```solidity
struct LogContract {
    address sender;        // address that called newLog
    address receiver;      // only address that can call verifyLog
    uint256 sk1x;          // x-coordinate of the committed EC public key (= sk2 * G.x)
    uint256 sk1y;          // y-coordinate of the committed EC public key (= sk2 * G.y)
    uint    timelock;      // Unix timestamp — log valid until this time
    bytes32 messageHash;   // keccak256(message) committed at creation
    bool    verified;      // true once verifyLog succeeds
}
```

The pair `(sk1x, sk1y)` is the sender's commitment to a secp256k1 public key. The receiver proves knowledge of the corresponding private scalar `sk2` in `verifyLog`. Once `verified` is set to `true` it cannot be changed back.

---

## 4. State Variables

```solidity
// Section 6 — Index CID registry
mapping(string => string) private indexCIDs;

// Section 7 — Schema registry
mapping(string => string) private tableSchemas;
string[]                  private tableNames;       // ordered list for enumeration
mapping(string => bool)   private tableExists;      // O(1) duplicate check

// Section 8 — Access policies (keyed by querier address)
mapping(address => AccessPolicy[]) private accessPolicies;

// Section 9 — CID Batch Log (public: direct lookup by batchId)
mapping(bytes32 => Batch) public batches;

// Section 10 — Secure Log (public: direct lookup by logId)
mapping(bytes32 => LogContract) public logs;
```

`batches` and `logs` are `public` — the Solidity compiler generates a getter for each, enabling direct slot reads without calling `getBatch`/`getLog`. The Python wrapper uses the explicit getter functions for the structured return type.

---

## 5. Events

### Original (5 events)

| Event | Emitted by | Key parameters |
|---|---|---|
| `IndexUpdated(string attribute, string oldCID, string newCID)` | `updateIndexCID`, `batchUpdateIndexCIDs`, `removeIndex` | attribute key, before/after CIDs |
| `BatchIndexUpdated(string[] attributes, string[] newCIDs)` | `batchUpdateIndexCIDs` | full arrays |
| `SchemaUpdated(string tableName, string oldSchema, string newSchema)` | `updateTableSchema`, `removeTableSchema` | table name, before/after schema |
| `AccessPolicyAdded(address indexed walletAddress, string tableName, string policySql)` | `addAccessPolicy` | querier address |
| `AccessPolicyRemoved(address indexed walletAddress, string tableName)` | `removeAccessPolicy`, `removeAllAccessPolicies` | querier address; tableName is `"ALL"` for bulk removal |

### CIDBatchLog (4 events)

| Event | Emitted by | Purpose |
|---|---|---|
| `BatchCreated(bytes32 indexed batchId, address indexed sender, address indexed receiver, uint cidCount, bytes32 aggregateHash, bytes32 messageHash, uint timelock)` | `createBatch` | announces new batch commitment |
| `CIDsLogged(bytes32 indexed batchId, bytes32[] cids)` | `createBatch` | publishes the full CID list as event data — **not stored in contract storage** |
| `AggregateReleased(bytes32 indexed batchId, bytes32 aggregateHash)` | `releaseAggregate` | delivery signal from sender |
| `BatchVerified(bytes32 indexed batchId, address indexed verifier)` | `verifyCIDs` | records who verified and when |

`CIDsLogged` is the key design element: the CID list lives permanently in the Ethereum event log (cheap, immutable) but costs nothing in contract storage.

### SecLog (2 events)

| Event | Emitted by | Purpose |
|---|---|---|
| `LogEntryNew(bytes32 indexed logId, address indexed sender, address indexed receiver, uint256 sk1x, uint256 sk1y, bytes32 messageHash, uint timelock)` | `newLog` | announces new log commitment |
| `LogVerified(bytes32 indexed logId)` | `verifyLog` | confirms both proofs passed |

---

## 6. Modifiers

### Shared

| Modifier | Guard | Used by |
|---|---|---|
| `futureTimelock(uint _time)` | `_time > block.timestamp` | `createBatch`, `newLog` |

### CIDBatchLog modifiers

| Modifier | Guard | Used by |
|---|---|---|
| `batchExists(bytes32 _batchId)` | `batches[_batchId].sender != address(0)` | `releaseAggregate`, `verifyCIDs`, `verifyBatchMessage`, `getBatch` (implicitly via public mapping) |
| `onlySender(bytes32 _batchId)` | `msg.sender == batches[_batchId].sender` | `releaseAggregate` |
| `onlyReceiver(bytes32 _batchId)` | `msg.sender == batches[_batchId].receiver` | declared but not applied to any function (verification open to anyone) |
| `notReleased(bytes32 _batchId)` | `!batches[_batchId].released` | `releaseAggregate` |
| `batchNotVerified(bytes32 _batchId)` | `!batches[_batchId].verified` | `verifyCIDs` |

### SecLog modifiers

| Modifier | Guard | Used by |
|---|---|---|
| `logExists(bytes32 _logId)` | `haveLog(_logId)` (see internal helper below) | `verifyLog` |
| `logNotVerified(bytes32 _logId)` | `!logs[_logId].verified` | `verifyLog` |

### Naming collision resolution

Both source contracts defined a modifier called `notVerified` (on different mappings) and `futureTimelock` (identical logic). During the merge:
- `notVerified` (CIDBatchLog) → renamed **`batchNotVerified`**
- `notVerified` (SecLog) → renamed **`logNotVerified`**
- `futureTimelock` → merged into a single definition using `block.timestamp` (replacing the deprecated `now` keyword from Solidity < 0.8)

---

## 7. Original Functions

These three sections existed before the CIDBatchLog/SecLog integration. They are unchanged in the merged contract.

### 7.1 Index CID Management (Section 6)

```solidity
updateIndexCID(string attr, string newCID)                    // write; emits IndexUpdated
batchUpdateIndexCIDs(string[] attrs, string[] newCIDs)        // write; emits IndexUpdated × N + BatchIndexUpdated
getIndexCID(string attr) view returns (string)                // read single
batchGetIndexCIDs(string[] attrs) view returns (string[])     // read multiple
removeIndex(string attr)                                      // delete; emits IndexUpdated("", newCID→"")
```

The Python application always uses composite keys of the form `"table_name.attribute"` (e.g. `"patient_data.PatientID"`). See the bridge helpers in `app/scripts/app.py` (`make_index_key`, `parse_index_key`) for key construction.

### 7.2 Schema Management (Section 7)

```solidity
updateTableSchema(string tableName, string schemaJson)        // write; adds to tableNames[] if new
getTableSchema(string tableName) view returns (string)
batchGetTableSchemas(string[] names) view returns (string[])
getAllTableNames() view returns (string[])
getAllTableSchemas() view returns (TableSchema[])
removeTableSchema(string tableName)                           // swap-and-pop on tableNames[]; order changes
```

**Note on `removeTableSchema`**: the internal `tableNames[]` array uses swap-and-pop for O(1) deletion. The last element is moved into the deleted slot, so the on-chain ordering of table names changes on every removal. Do not rely on positional ordering.

### 7.3 Access Policy Management (Section 8)

```solidity
addAccessPolicy(address subject, address object, string tableName, string policySql)
getAccessPolicies(address objectAddress) view returns (AccessPolicy[])
getPolicyCount(address objectAddress) view returns (uint)
removeAccessPolicy(address objectAddress, uint policyIndex)   // swap-and-pop
removeAllAccessPolicies(address objectAddress)                // delete entire array
```

Policies are stored keyed by the **querier** (`object`) address. At query time the enclave fetches policies for a given querier, constructs a CTE that enforces `OwnerID = subject` per row, and wraps the user query in it.

---

## 8. CIDBatchLog Integration

### 8.1 Design Rationale

The central design goal is **minimising on-chain storage** while providing a tamper-evident, auditable commitment to an ordered set of CIDs. The solution:

- Store only a 32-byte `aggregateHash` on-chain regardless of CID count.
- Emit the full CID list in the `CIDsLogged` event — cheap event storage, permanently indexed on-chain.
- Allow anyone to reconstruct and verify the list from event logs at any time.

### 8.2 Aggregate Hash Construction

```
aggregateHash = sha256(uint256(count) || cid1 || cid2 || ... || cidN)
```

Implemented as:
```solidity
function computeAggregate(bytes32[] memory _cids) public pure returns (bytes32) {
    return sha256(abi.encodePacked(uint256(_cids.length), _cids));
}
```

The `uint256(count)` length prefix prevents length-extension attacks: without it, the concatenation `[A]` and `[A, B]` could be confused if `A` happened to start with length-encoding bytes.

### 8.3 batchId Derivation

```
batchId = sha256(sender || receiver || aggregateHash || messageHash || timelock)
```

Implemented as:
```solidity
batchId = sha256(abi.encodePacked(
    msg.sender, _receiver, aggregateHash, _messageHash, _timelock
));
```

This ID is **deterministic**: the same inputs always produce the same batchId. If `createBatch` is called twice with identical arguments, the second call reverts with `"batch exists"`. To create a distinct batch with the same CID set, change at least one parameter (e.g. a different `timelock`).

### 8.4 Batch State Machine

```
          createBatch()
               │
               ▼
          [Created]
         released=false
         verified=false
          /         \
         │           │
   releaseAggregate()  verifyCIDs()
         │           │
         ▼           ▼
    released=true  verified=true
```

`released` and `verified` are independent. Either can be set first, or both can be set in the same or different transactions. Once set, neither flag can be reverted.

### 8.5 Function Reference

#### `computeAggregate`
```solidity
function computeAggregate(bytes32[] memory _cids)
    public pure returns (bytes32)
```
**Pure** helper. Used internally by `createBatch` and `verifyCIDs`, also callable off-chain for pre-computation.

#### `createBatch`
```solidity
function createBatch(
    address  _receiver,
    bytes32[] calldata _cids,
    bytes32  _messageHash,
    uint     _timelock
) external futureTimelock(_timelock)
  returns (bytes32 batchId, bytes32 aggregateHash)
```

| Guard | Error |
|---|---|
| `_timelock > block.timestamp` | `"timelock time must be in the future"` |
| `_receiver != address(0)` | `"receiver=0"` |
| `_cids.length > 0` | `"empty CID list"` |
| `batches[batchId].sender == address(0)` | `"batch exists"` |

Emits: `BatchCreated`, `CIDsLogged` (both in the same transaction).

Returns: the deterministic `batchId` and the computed `aggregateHash`.

#### `releaseAggregate`
```solidity
function releaseAggregate(bytes32 _batchId)
    external batchExists(_batchId) onlySender(_batchId) notReleased(_batchId)
    returns (bytes32 aggregateHash)
```

Sets `batches[_batchId].released = true`. Signals to the receiver that the sender has acknowledged delivery. Emits `AggregateReleased`.

| Guard | Error |
|---|---|
| batch must exist | `"batch does not exist"` |
| `msg.sender == batch.sender` | `"only sender"` |
| `!batch.released` | `"already released"` |

#### `verifyCIDs`
```solidity
function verifyCIDs(bytes32 _batchId, bytes32[] calldata _cids)
    external batchExists(_batchId) batchNotVerified(_batchId)
    returns (bool)
```

Recomputes `computeAggregate(_cids)` and checks it against the stored `aggregateHash`. Open to any caller — the receiver, sender, or a third party can all trigger verification.

| Guard | Error |
|---|---|
| batch must exist | `"batch does not exist"` |
| `!batch.verified` | `"already verified"` |
| `_cids.length == batch.cidCount` | `"CID count mismatch"` |
| `computeAggregate(_cids) == batch.aggregateHash` | `"aggregate mismatch"` |

Emits: `BatchVerified(batchId, msg.sender)`.

#### `verifyBatchMessage`
```solidity
function verifyBatchMessage(bytes32 _batchId, bytes calldata _message)
    external view batchExists(_batchId)
    returns (bool)
```

Checks `keccak256(_message) == batch.messageHash`. Reverts if `messageHash` is `bytes32(0)` (i.e. no message hash was set at creation). This is a **view** function — no state change.

| Guard | Error |
|---|---|
| batch must exist | `"batch does not exist"` |
| `batch.messageHash != bytes32(0)` | `"no messageHash set"` |
| `keccak256(_message) == batch.messageHash` | `"message hash mismatch"` |

> **Renamed from original**: `verifyMessage` in `CIDBatchLog.sol` → `verifyBatchMessage` in the merged contract to avoid future name collision with the SecLog side.

#### `getBatch`
```solidity
function getBatch(bytes32 _batchId)
    external view
    returns (address sender, address receiver, uint cidCount,
             bytes32 aggregateHash, bytes32 messageHash,
             uint timelock, bool released, bool verified)
```

Returns all 8 fields of the `Batch` struct. Returns all-zero values for a non-existent `batchId` (no revert).

---

## 9. SecLog Integration

### 9.1 Cryptographic Protocol

SecLog uses secp256k1 elliptic curve cryptography to bind a log entry to a secret known only to the receiver. The protocol is:

**Sender side (off-chain)**:
1. Choose a random 256-bit private scalar `sk2`
2. Compute the public key point: `(sk1x, sk1y) = sk2 * G` (where G is the secp256k1 generator)
3. Commit the message: `messageHash = keccak256(message)`
4. Call `newLog(receiver, sk1x, sk1y, messageHash, timelock)`

**Receiver side (on-chain)**:
1. Receive `sk2` from the sender (off-band delivery)
2. Call `verifyLog(logId, sk2, message)`
3. The contract recomputes `sk2 * G` on-chain via `EllipticCurve.ecMul`
4. Checks: `sk2 * G == (sk1x, sk1y)` — proves knowledge of `sk2`
5. Checks: `keccak256(message) == messageHash` — proves message integrity
6. Both must pass simultaneously

This provides a verifiable, non-repudiable receipt: the receiver cannot claim they verified without actually knowing `sk2`, and the message content is bound to the commitment.

### 9.2 secp256k1 Constants

```solidity
uint256 public constant GX = 0x79BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798;
uint256 public constant GY = 0x483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8;
uint256 public constant AA = 0;   // curve coefficient a (secp256k1: y² = x³ + 7)
uint256 public constant PP = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F;
```

These are the standard secp256k1 parameters as used in Bitcoin and Ethereum key generation.

### 9.3 logId Derivation

```
logId = keccak256(sender || receiver || sk1x || sk1y || messageHash || timelock)
```

Implemented as:
```solidity
logId = keccak256(abi.encodePacked(
    msg.sender, _receiver, _sk1x, _sk1y, _messageHash, _timelock
));
```

Deterministic: the same 6-tuple always produces the same `logId`. Duplicate submissions revert with `"Log already exists"`.

### 9.4 Function Reference

#### `haveLog` (internal)
```solidity
function haveLog(bytes32 _logId) internal view returns (bool)
```
Returns `true` if `logs[_logId].sender != address(0)`. Used by the `logExists` modifier and the duplicate-check in `newLog`.

#### `newLog`
```solidity
function newLog(
    address  _receiver,
    uint256  _sk1x,
    uint256  _sk1y,
    bytes32  _messageHash,
    uint     _timelock
) external futureTimelock(_timelock) returns (bytes32 logId)
```

| Guard | Error |
|---|---|
| `_timelock > block.timestamp` | `"timelock time must be in the future"` |
| `!haveLog(logId)` | `"Log already exists"` |

Stores the `LogContract` struct and emits `LogEntryNew`. Returns the deterministic `logId`.

#### `verifyLog`
```solidity
function verifyLog(
    bytes32  _logId,
    uint256  _sk2,
    bytes calldata _message
) external logExists(_logId) logNotVerified(_logId) returns (bool)
```

| Guard | Error |
|---|---|
| `haveLog(_logId)` | `"Log does not exist"` |
| `!log.verified` | `"Already verified"` |
| `log.receiver == msg.sender` | `"Only receiver can verify"` |
| `ecMul(sk2, GX, GY, AA, PP) == (sk1x, sk1y)` | `"Invalid sk2 proof"` |
| `keccak256(_message) == log.messageHash` | `"Message hash mismatch"` |

Sets `log.verified = true` and emits `LogVerified`. Returns `true`.

> **Gas**: This function calls `EllipticCurve.ecMul` which performs ~256 iterations of elliptic curve doubling-and-adding in pure Solidity (binary double-and-add, Jacobian coordinates). Actual gas consumption is ~500,000–1,000,000. The Python wrapper uses `gas: 3000000` to provide headroom.

#### `getLog`
```solidity
function getLog(bytes32 _logId)
    external view
    returns (address sender, address receiver,
             uint256 sk1x, uint256 sk1y,
             uint timelock, bytes32 messageHash, bool verified)
```

Returns all 7 fields of the `LogContract` struct. Returns all-zero values for a non-existent `logId`.

---

## 10. EllipticCurve.sol — Compatibility Changes

`EllipticCurve.sol` is a secp256k1 arithmetic library originally written by the Witnet Foundation (pragma `>=0.5.3 <0.7.0`). It provides the EC point operations used by `verifyLog`. Two changes were made for Solidity 0.8.x compatibility:

### Change 1 — Pragma

```diff
- pragma solidity >=0.5.3 <0.7.0;
+ pragma solidity ^0.8.0;
```

### Change 2 — `unchecked` block in `invMod`

The modular inverse function uses the Extended Euclidean Algorithm:

```solidity
function invMod(uint256 _x, uint256 _pp) internal pure returns (uint256) {
    require(_x != 0 && _x != _pp && _pp != 0, "Invalid number");
    uint256 q = 0;
    uint256 newT = 1;
    uint256 r = _pp;
    uint256 t;
    while (_x != 0) {
        t = r / _x;
        (q, newT) = (newT, addmod(q, (_pp - mulmod(t, newT, _pp)), _pp));
        unchecked { (r, _x) = (_x, r - t * _x); }   // ← added unchecked
    }
    return q;
}
```

**Why the `unchecked` block is safe**: at each iteration, `r` and `_x` are the current remainder pair in the Euclidean algorithm: `r = t * _x + remainder`. The remainder `r - t * _x` is mathematically guaranteed to be in `[0, _x)` — it is never negative. Solidity 0.8.x checked arithmetic would incorrectly treat this as a potential underflow and revert. The `unchecked {}` block disables the check for this one operation only.

### Library Function Map

| Function | Coordinates | Notes |
|---|---|---|
| `invMod(x, pp)` | field arithmetic | Extended Euclidean Algorithm |
| `expMod(base, exp, pp)` | field arithmetic | binary exponentiation |
| `toAffine(X, Y, Z, pp)` | Jacobian → affine | used at end of `ecMul` |
| `isOnCurve(x, y, aa, bb, pp)` | affine | curve membership check |
| `ecInv(x, y, pp)` | affine | point negation |
| `ecAdd(x1, y1, x2, y2, aa, pp)` | affine | calls `jacAdd` internally |
| `ecSub(x1, y1, x2, y2, aa, pp)` | affine | add with negated point |
| `ecMul(k, x, y, aa, pp)` | affine | calls `jacMul`, then `toAffine` |
| `jacAdd(X1, Y1, Z1, X2, Y2, Z2, pp)` | Jacobian | projective addition |
| `jacDouble(X, Y, Z, aa, pp)` | Jacobian | projective doubling |
| `jacMul(d, X, Y, Z, aa, pp)` | Jacobian | binary double-and-add, ~256 iterations |

Only `ecMul` is called by `Web3dbContract.sol`.

---

## 11. Python Wrapper

The `Web3dbContract` class in `app/scripts/web3db_contract.py` provides Python-level access to all contract functions. All ABI entries are hardcoded in the class `__init__` as a Python list (no external JSON artifact).

### 11.1 Write Method Pattern

Every state-modifying method follows this exact pattern:

```python
nonce = self.w3.eth.get_transaction_count(self.address)
tx = self.contract.functions.SomeFunction(args).build_transaction({
    'from':     self.address,
    'gas':      2000000,          # 3000000 for verifyLog only
    'gasPrice': self._get_gas_price(),  # floors at 2 gwei
    'nonce':    nonce,
})
signed_tx = self.w3.eth.account.sign_transaction(tx, self.private_key)
tx_hash   = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
tx_receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
# success: tx_receipt.get('status') == 1
```

### 11.2 Read Method Pattern

```python
result = self.contract.functions.SomeViewFunction(args).call()
return True, result
```

### 11.3 CIDBatchLog Methods

#### `create_batch(receiver_address, cids, message_hash=None, timelock=None)`
```
Returns: (success: bool, batch_id: bytes, aggregate_hash: bytes)
Gas:     2_000_000
Event:   BatchCreated  →  extracts batch_id and aggregate_hash from logs[0]['args']
```
CIDs are normalised to `bytes` (strips `0x` prefix if hex string). `message_hash` defaults to `bytes(32)` (32 zero bytes) when `None`.

#### `release_aggregate(batch_id)`
```
Returns: (success: bool, aggregate_hash: bytes)
Gas:     2_000_000
Event:   AggregateReleased  →  extracts aggregate_hash from logs[0]['args']
```

#### `verify_cids(batch_id, cids)`
```
Returns: bool
Gas:     2_000_000
Event:   BatchVerified (not extracted — success inferred from receipt status)
```

#### `verify_batch_message(batch_id, message)`
```
Returns: bool
Gas:     2_000_000
Note:    verifyBatchMessage is view in Solidity but called as a transaction here.
         Equivalent result can be obtained via .call() with no gas cost.
```

#### `get_batch(batch_id)`
```
Returns: (success: bool, dict)
dict keys: sender, receiver, cid_count, aggregate_hash, message_hash,
           timelock, released, verified
Call:    .call()  (no transaction, no gas)
```

### 11.4 SecLog Methods

#### `new_log(receiver_address, sk1x, sk1y, message_hash, timelock)`
```
Returns: (success: bool, log_id: bytes)
Gas:     2_000_000
Event:   LogEntryNew  →  extracts log_id from logs[0]['args']['logId']
```
`sk1x` and `sk1y` are Python `int` values. The receiver address is normalised to EIP-55 checksum format via `Web3.to_checksum_address`.

#### `verify_log(log_id, sk2, message)`
```
Returns: bool
Gas:     3_000_000  (elevated for EllipticCurve.ecMul cost)
Event:   LogVerified (extracted with a nested try/except — failure to parse
         event is logged as a warning but does not cause the method to return False)
```
`sk2` is a Python `int`. `message` is the original plaintext `bytes` (not its hash).

#### `get_log(log_id)`
```
Returns: (success: bool, dict)
dict keys: sender, receiver, sk1x, sk1y, timelock, message_hash, verified
Call:    .call()  (no transaction, no gas)
```

---

## 12. Contract Size and Deployment

### Size

The merged contract compiled without optimisation is **26,456 bytes**, exceeding the 24,576-byte limit introduced by EIP-170 (Spurious Dragon hardfork). With the Solidity optimizer at 200 runs, the bytecode shrinks to within the limit.

Hardhat configuration (`contracts/hardhat.config.js`):
```javascript
solidity: {
    version: "0.8.28",
    settings: {
        optimizer: { enabled: true, runs: 200 }
    }
},
networks: {
    hardhat: { allowUnlimitedContractSize: true },  // local tests only
    sepolia: { ... }
}
```

`allowUnlimitedContractSize: true` applies **only to the local Hardhat in-process node** used during testing. It has no effect on Sepolia or mainnet deployments.

### Redeployment

The merged contract is a **new contract** — it does not share storage with any previously deployed contract. After deploying with Hardhat:

```bash
npx hardhat run scripts/deploy.js --network sepolia
```

Update `CONTRACT_ADDRESS` in `app/scripts/.env` and restart the application. All previously indexed CIDs, schemas, and access policies from the old contract must be re-uploaded manually or via a migration script.

---

## 13. Test Suite

**File**: `contracts/test/Web3dbContract.test.js`
**Framework**: Hardhat + Chai + ethers v6
**Total**: 41 tests, all passing

### How to Run

```bash
cd contracts
NODE=/home/shady/.nvm/versions/node/v24.14.0/bin/node
PRIVATE_KEY=0000000000000000000000000000000000000000000000000000000000000001 \
  $NODE node_modules/.bin/hardhat test test/Web3dbContract.test.js --network hardhat
```

> `node` is not on the default PATH — use the full nvm path. `PRIVATE_KEY` is a dummy value required by the Hardhat config's Sepolia network account definition; it is not used during local testing.

### Test Helpers

```javascript
// Returns Unix timestamp (seconds) this many seconds from now
function futureTimelock(offsetSeconds = 3600) { ... }

// secp256k1 generator G — ecMul(1, GX, GY, AA, PP) = (GX, GY)
const GX = BigInt("0x79BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798");
const GY = BigInt("0x483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8");

// Deterministic test CIDs
const CID1 = ethers.keccak256(ethers.toUtf8Bytes("web3db-cid-1"));  // bytes32
const CID2 = ethers.keccak256(ethers.toUtf8Bytes("web3db-cid-2"));
const CID3 = ethers.keccak256(ethers.toUtf8Bytes("web3db-cid-3"));
```

### EC Test Key Choice

```
sk2 = 1  →  ecMul(1, GX, GY, AA, PP) = G = (GX, GY)
```

Using `sk2 = 1` is the simplest valid secp256k1 scalar: multiplying the generator by 1 returns the generator itself. This lets tests verify the EC proof path with no off-chain elliptic curve library. Using `sk2 = 2` produces a different point (`2G`), used by the "wrong sk2" negative test.

### CIDBatchLog Test Coverage (28 tests)

| Describe | Test | Validates |
|---|---|---|
| `computeAggregate` | deterministic | same inputs → same hash |
| | order-sensitive | `[A,B] ≠ [B,A]` |
| | length-prefix protected | `[A] ≠ [A,B]` |
| `createBatch` | returns correct IDs | batchId ≠ 0, aggregateHash matches |
| | emits both events | `BatchCreated` + `CIDsLogged` in one tx |
| | stores metadata | `getBatch` returns correct values |
| | stores messageHash | optional field round-trips correctly |
| | rejects past timelock | reverts `"timelock time must be in the future"` |
| | rejects zero receiver | reverts `"receiver=0"` |
| | rejects empty CID list | reverts `"empty CID list"` |
| | rejects duplicate batchId | reverts `"batch exists"` |
| | allows different timelocks | different timelock → different batchId |
| `releaseAggregate` | marks released, emits event | `released=true`, `AggregateReleased` with correct hash |
| | non-sender fails | reverts `"only sender"` |
| | non-existent batch fails | reverts `"batch does not exist"` |
| | double release fails | reverts `"already released"` |
| `verifyCIDs` | verifies correct list, emits event | `verified=true`, `BatchVerified(batchId, verifier)` |
| | anyone can verify | third-party address allowed |
| | count mismatch fails | reverts `"CID count mismatch"` |
| | content mismatch fails | reverts `"aggregate mismatch"` |
| | double verification fails | reverts `"already verified"` |
| | non-existent batch fails | reverts `"batch does not exist"` |
| `verifyBatchMessage` | correct message → true | `staticCall` returns `true` |
| | wrong message fails | reverts `"message hash mismatch"` |
| | missing messageHash fails | reverts `"no messageHash set"` |
| | non-existent batch fails | reverts `"batch does not exist"` |
| `getBatch` | non-existent → zeroes | `sender=0x0`, `cidCount=0n`, `released=false` |

### SecLog Test Coverage (13 tests)

| Describe | Test | Validates |
|---|---|---|
| `newLog` | returns non-zero logId | logId ≠ `0x000...` |
| | emits `LogEntryNew` | event present in tx |
| | stores metadata | `getLog` returns correct fields |
| | rejects past timelock | reverts `"timelock time must be in the future"` |
| | rejects duplicate logId | reverts `"Log already exists"` |
| | allows different timelocks | different timelock → different logId |
| `verifyLog` *(120s timeout)* | verifies sk2=1 + message | `LogVerified(logId)` emitted, `verified=true` |
| | marks log verified | `getLog[6] == true` |
| | non-receiver fails | reverts `"Only receiver can verify"` |
| | wrong sk2 fails | reverts `"Invalid sk2 proof"` (sk2=2 → 2G ≠ G) |
| | wrong message fails | reverts `"Message hash mismatch"` |
| | double verify fails | reverts `"Already verified"` |
| | non-existent logId fails | reverts `"Log does not exist"` |
| `getLog` | non-existent → zeroes | `sender=0x0`, `sk1x=0n`, `verified=false` |

The `verifyLog` describe block sets `this.timeout(120_000)` because `EllipticCurve.ecMul` in the local EVM is compute-intensive even without gas limits.

---

## 14. Design Constraints and Gotchas

### Nonce collisions
The Python wrapper uses `get_transaction_count` for each nonce. Two write calls fired concurrently from the same account will both get the same nonce and one will fail. The system is designed for single-threaded write access only.

### Fixed gas limits
`create_batch`, `release_aggregate`, `verify_cids`, `verify_batch_message`, `new_log` all use `gas: 2_000_000`. Very large CID arrays or complex access policies could approach this limit. `verify_log` uses `gas: 3_000_000` — do not lower this; the EC multiplication is genuinely expensive.

### CID list is not recoverable from contract storage
The full CID list emitted in `CIDsLogged` is stored in Ethereum event logs, not in contract storage. Recovering it requires querying historical events (e.g. via Infura's `eth_getLogs`). The contract itself only stores the 32-byte `aggregateHash`.

### No access control on-chain
All functions are callable by any Ethereum address. Security is enforced by the SGX enclave that intermediates all application calls to the contract. A caller bypassing the enclave can write arbitrary data to the contract.

### Contract size
26KB unoptimised — exceeds EIP-170. The Hardhat optimizer must remain enabled. Without it, the contract will fail to deploy on any real network.

### Redeployment and data migration
The merged contract is a new deployment at a new address. On-chain data from any previously deployed contract (index CIDs, schemas, policies) does not transfer automatically and must be migrated manually.

#!/usr/bin/env python3
"""
Verify CIDBatchLog and SecLog integration against the live Sepolia contract.
Tests the full lifecycle of both modules using the Python wrapper.
"""
import sys, os, time

# Add app/scripts to path so we can import the wrapper
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'app', 'scripts'))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '..', 'app', 'scripts', '.env'))

from web3db_contract import Web3dbContract
from web3 import Web3


def wait_for_nonce(w3, address, expected_nonce, timeout=60):
    """Wait until the node's nonce catches up after a confirmed transaction."""
    start = time.time()
    while time.time() - start < timeout:
        current = w3.eth.get_transaction_count(address)
        if current >= expected_nonce:
            return
        time.sleep(2)
    print(f"  Warning: nonce did not reach {expected_nonce} within {timeout}s")

PASS = "\033[92m✓\033[0m"
FAIL = "\033[91m✗\033[0m"

def main():
    contract = Web3dbContract(
        contract_address=os.environ['CONTRACT_ADDRESS'],
        infura_api_key=os.environ['INFURA_API_KEY'],
        private_key=os.environ['PRIVATE_KEY'],
    )

    sender_address = contract.address
    w3 = contract.w3
    # Use a second address as "receiver" (just a random valid address for testing)
    receiver_address = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8"
    # Track nonce to avoid "nonce too low" between sequential transactions
    nonce = w3.eth.get_transaction_count(sender_address)

    print("=" * 60)
    print("  Verifying CIDBatchLog & SecLog on Sepolia")
    print(f"  Contract: {os.environ['CONTRACT_ADDRESS']}")
    print(f"  Sender:   {sender_address}")
    print("=" * 60)

    errors = []

    # ------------------------------------------------------------------
    # 1. CIDBatchLog — computeAggregate (view call, no gas)
    # ------------------------------------------------------------------
    print("\n--- CIDBatchLog ---\n")

    cid1 = Web3.keccak(text="verify-cid-1")
    cid2 = Web3.keccak(text="verify-cid-2")
    cids = [cid1, cid2]

    success, agg = contract.compute_aggregate(cids)
    if success and agg:
        print(f"  {PASS} computeAggregate: {agg.hex()}")
    else:
        print(f"  {FAIL} computeAggregate failed")
        errors.append("computeAggregate")

    # ------------------------------------------------------------------
    # 2. CIDBatchLog — createBatch
    # ------------------------------------------------------------------
    timelock = int(time.time()) + 7200  # 2 hours from now
    message = b"integration test message"
    message_hash = Web3.keccak(message)

    print(f"  ... creating batch (this sends a transaction, may take ~15s)")
    success, batch_id, agg_hash = contract.create_batch(
        receiver_address=receiver_address,
        cids=cids,
        message_hash=message_hash,
        timelock=timelock,
    )

    if success and batch_id:
        nonce += 1
        wait_for_nonce(w3, sender_address, nonce)
        print(f"  {PASS} createBatch: batchId={batch_id.hex()[:16]}...")
        print(f"     aggregateHash={agg_hash.hex()[:16]}...")
    else:
        print(f"  {FAIL} createBatch failed")
        errors.append("createBatch")
        print("\n  Cannot continue CIDBatchLog tests without a batch.")
        return run_seclog(contract, sender_address, receiver_address, errors, w3, nonce)

    # ------------------------------------------------------------------
    # 3. CIDBatchLog — getBatch (view call)
    # ------------------------------------------------------------------
    success, batch = contract.get_batch(batch_id)
    if success and batch.get('sender', '').lower() == sender_address.lower():
        print(f"  {PASS} getBatch: sender={batch['sender'][:10]}..., cidCount={batch['cid_count']}, released={batch['released']}, verified={batch['verified']}")
    else:
        print(f"  {FAIL} getBatch returned unexpected data")
        errors.append("getBatch")

    # ------------------------------------------------------------------
    # 4. CIDBatchLog — verifyBatchMessage
    # ------------------------------------------------------------------
    print(f"  ... verifying batch message")
    success = contract.verify_batch_message(batch_id, message)
    if success:
        nonce += 1
        wait_for_nonce(w3, sender_address, nonce)
        print(f"  {PASS} verifyBatchMessage: message matches")
    else:
        print(f"  {FAIL} verifyBatchMessage failed")
        errors.append("verifyBatchMessage")

    # ------------------------------------------------------------------
    # 5. CIDBatchLog — releaseAggregate
    # ------------------------------------------------------------------
    print(f"  ... releasing aggregate")
    success, released_hash = contract.release_aggregate(batch_id)
    if success and released_hash:
        nonce += 1
        wait_for_nonce(w3, sender_address, nonce)
        print(f"  {PASS} releaseAggregate: hash={released_hash.hex()[:16]}...")
    else:
        print(f"  {FAIL} releaseAggregate failed")
        errors.append("releaseAggregate")

    # ------------------------------------------------------------------
    # 6. CIDBatchLog — verifyCIDs
    # ------------------------------------------------------------------
    print(f"  ... verifying CIDs against commitment")
    success = contract.verify_cids(batch_id, cids)
    if success:
        print(f"  {PASS} verifyCIDs: CID list matches aggregate commitment")
    else:
        print(f"  {FAIL} verifyCIDs failed")
        errors.append("verifyCIDs")

    # ------------------------------------------------------------------
    # 7. Confirm final batch state
    # ------------------------------------------------------------------
    success, batch = contract.get_batch(batch_id)
    if success and batch.get('released') and batch.get('verified'):
        print(f"  {PASS} Final batch state: released={batch['released']}, verified={batch['verified']}")
    else:
        print(f"  {FAIL} Unexpected final batch state: {batch}")
        errors.append("final batch state")

    run_seclog(contract, sender_address, receiver_address, errors, w3, nonce)


def run_seclog(contract, sender_address, receiver_address, errors, w3=None, nonce=None):
    """Test the SecLog lifecycle."""
    print("\n--- SecLog ---\n")

    if w3 is None:
        w3 = contract.w3
    if nonce is None:
        nonce = w3.eth.get_transaction_count(sender_address)

    # sk2 = 1 → public key = secp256k1 generator G = (GX, GY)
    sk2 = 1
    GX = 0x79BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798
    GY = 0x483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8

    message = b"seclog integration test"
    message_hash = Web3.keccak(message)
    timelock = int(time.time()) + 7200

    # ------------------------------------------------------------------
    # 8. SecLog — newLog
    # ------------------------------------------------------------------
    print(f"  ... creating log entry (transaction)")
    success, log_id = contract.new_log(
        receiver_address=sender_address,  # sender verifies own log for testing
        sk1x=GX,
        sk1y=GY,
        message_hash=message_hash,
        timelock=timelock,
    )

    if success and log_id:
        nonce += 1
        wait_for_nonce(w3, sender_address, nonce)
        print(f"  {PASS} newLog: logId={log_id.hex()[:16]}...")
    else:
        print(f"  {FAIL} newLog failed")
        errors.append("newLog")
        return print_summary(errors)

    # ------------------------------------------------------------------
    # 9. SecLog — getLog (view call)
    # ------------------------------------------------------------------
    success, log = contract.get_log(log_id)
    if success and Web3.to_checksum_address(log.get('sender', '')) == Web3.to_checksum_address(sender_address):
        print(f"  {PASS} getLog: sender={log['sender'][:10]}..., verified={log['verified']}")
    else:
        print(f"  {FAIL} getLog returned unexpected data")
        errors.append("getLog")

    # ------------------------------------------------------------------
    # 10. SecLog — verifyLog (expensive: EC multiplication on-chain)
    # ------------------------------------------------------------------
    print(f"  ... verifying log (EC multiplication on-chain, may take ~20s)")
    success = contract.verify_log(log_id, sk2, message)
    if success:
        nonce += 1
        wait_for_nonce(w3, sender_address, nonce)
        print(f"  {PASS} verifyLog: EC proof + message hash verified on-chain")
    else:
        print(f"  {FAIL} verifyLog failed")
        errors.append("verifyLog")

    # ------------------------------------------------------------------
    # 11. Confirm final log state
    # ------------------------------------------------------------------
    success, log = contract.get_log(log_id)
    if success and log.get('verified'):
        print(f"  {PASS} Final log state: verified={log['verified']}")
    else:
        print(f"  {FAIL} Unexpected final log state: {log}")
        errors.append("final log state")

    print_summary(errors)


def print_summary(errors):
    print("\n" + "=" * 60)
    if not errors:
        print(f"  {PASS} ALL CHECKS PASSED — CIDBatchLog & SecLog integration verified")
    else:
        print(f"  {FAIL} {len(errors)} check(s) failed: {', '.join(errors)}")
    print("=" * 60)
    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()

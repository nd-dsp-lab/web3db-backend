from web3 import Web3
import time
from datetime import datetime
from typing import Dict, List, Optional, Union, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Web3HashManager:
    contract_abi = [
        {
            "inputs": [
                {"internalType": "string", "name": "tableName", "type": "string"},
                {"internalType": "string", "name": "partition", "type": "string"},
                {"internalType": "string", "name": "hash", "type": "string"},
                {"internalType": "string", "name": "prevHash", "type": "string"},
                {"internalType": "string", "name": "recordType", "type": "string"},
                {"internalType": "string", "name": "flag", "type": "string"},
                {"internalType": "uint256", "name": "rowCount", "type": "uint256"}
            ],
            "name": "addHashMapping",
            "outputs": [{"internalType": "bool", "name": "", "type": "bool"}],
            "stateMutability": "nonpayable",
            "type": "function"
        },
        {
            "inputs": [
                {"internalType": "address", "name": "user", "type": "address"},
                {"internalType": "string", "name": "tableName", "type": "string"},
                {"internalType": "string", "name": "cid", "type": "string"}
            ],
            "name": "shareCID",
            "outputs": [{"internalType": "bool", "name": "", "type": "bool"}],
            "stateMutability": "nonpayable",
            "type": "function"
        },
        {
            "inputs": [
                {"internalType": "address", "name": "user", "type": "address"},
                {"internalType": "string", "name": "tableName", "type": "string"},
                {"internalType": "string", "name": "partition", "type": "string"}
            ],
            "name": "getLatestHash",
            "outputs": [{"internalType": "string", "name": "", "type": "string"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [
                {"internalType": "address", "name": "user", "type": "address"},
                {"internalType": "string", "name": "tableName", "type": "string"}
            ],
            "name": "getSharedCIDs",
            "outputs": [
                {"internalType": "string[]", "name": "cids", "type": "string[]"},
                {"internalType": "address[]", "name": "owners", "type": "address[]"}
            ],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [
                {"internalType": "address", "name": "user", "type": "address"},
                {"internalType": "string", "name": "tableName", "type": "string"},
                {"internalType": "string", "name": "partition", "type": "string"}
            ],
            "name": "getHashHistory",
            "outputs": [
                {"internalType": "string[]", "name": "hashes", "type": "string[]"},
                {"internalType": "string[]", "name": "prevHashes", "type": "string[]"},
                {"internalType": "string[]", "name": "types", "type": "string[]"},
                {"internalType": "uint256[]", "name": "timestamps", "type": "uint256[]"},
                {"internalType": "string[]", "name": "flags", "type": "string[]"},
                {"internalType": "uint256[]", "name": "rowCounts", "type": "uint256[]"}
            ],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [
                {"internalType": "address", "name": "user", "type": "address"}
            ],
            "name": "getUserTables",
            "outputs": [{"internalType": "string[]", "name": "", "type": "string[]"}],
            "stateMutability": "view",
            "type": "function"
        },
        {
            "inputs": [
                {"internalType": "address", "name": "user", "type": "address"},
                {"internalType": "string", "name": "tableName", "type": "string"},
                {"internalType": "string", "name": "cid", "type": "string"}
            ],
            "name": "revokeCIDAccess",
            "outputs": [{"internalType": "bool", "name": "", "type": "bool"}],
            "stateMutability": "nonpayable",
            "type": "function"
        },
        {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "internalType": "address", "name": "user", "type": "address"},
                {"indexed": False, "internalType": "string", "name": "table", "type": "string"},
                {"indexed": False, "internalType": "string", "name": "partition", "type": "string"},
                {"indexed": False, "internalType": "string", "name": "hash", "type": "string"},
                {"indexed": False, "internalType": "string", "name": "recordType", "type": "string"}
            ],
            "name": "HashMappingAdded",
            "type": "event"
        },
        {
            "anonymous": False,
            "inputs": [
                {"indexed": True, "internalType": "address", "name": "owner", "type": "address"},
                {"indexed": True, "internalType": "address", "name": "sharedWith", "type": "address"},
                {"indexed": False, "internalType": "string", "name": "table", "type": "string"},
                {"indexed": False, "internalType": "string", "name": "cid", "type": "string"}
            ],
            "name": "CIDShared",
            "type": "event"
        }
    ]

    SEPOLIA_CHAIN_ID = 11155111
    MAX_ATTEMPTS = 3          # Max attempts for transaction submission
    RETRY_BASE_WAIT = 3       # Base time in seconds to wait before retry
    BASE_MULTIPLIER = 1.5     # Base multiplier for gas price calculations
    MIN_GAS_PRICE = 10_000_000_000  # 10 Gwei as minimum gas price

    def __init__(self, provider_url: str, contract_address: str, private_key: str):
        """Initialize Web3 and contract connection."""
        try:
            self.w3 = Web3(Web3.HTTPProvider(provider_url))
            if not self.w3.is_connected():
                raise ConnectionError("Failed to connect to Ethereum network.")

            self.contract_address = self.w3.to_checksum_address(contract_address)
            self.private_key = private_key
            self.contract = self.w3.eth.contract(
                address=self.contract_address,
                abi=self.contract_abi
            )
            self.account = self.w3.eth.account.from_key(self.private_key)

            # For Sepolia, we'll determine nonce dynamically
            self.last_nonce = self.w3.eth.get_transaction_count(
                self.account.address, 'latest'
            )

            logger.info(f"Connected to Ethereum. Account: {self.account.address}")

        except Exception as e:
            logger.error(f"Failed to connect to Ethereum network: {str(e)}")
            raise

    def check_connection(self) -> bool:
        """Check the connection to the Ethereum network."""
        if not self.w3.is_connected():
            error_msg = "Lost connection to Ethereum network."
            logger.error(error_msg)
            raise ConnectionError(error_msg)
        return True

    def _get_next_nonce(self) -> int:
        """Get the next valid nonce for the current account."""
        try:
            latest_nonce = self.w3.eth.get_transaction_count(
                self.account.address, 'latest'
            )
            self.last_nonce = latest_nonce
            logger.info(f"Using nonce: {self.last_nonce}")
            return self.last_nonce
        except Exception as e:
            logger.error(f"Error getting next nonce: {str(e)}")
            raise

    def _calculate_gas_price(self, attempt: int = 0) -> int:
        """Calculate appropriate gas price with retry multiplier."""
        try:
            base_fee = self.w3.eth.gas_price
            retry_multiplier = self.BASE_MULTIPLIER ** (attempt + 1)
            final_gas_price = int(base_fee * retry_multiplier)
            final_gas_price = max(final_gas_price, self.MIN_GAS_PRICE)

            logger.info(
                f"Gas price calculation: base={base_fee}, "
                f"retry_multiplier={retry_multiplier:.2f}, "
                f"final={final_gas_price}"
            )
            return final_gas_price
        except Exception as e:
            logger.error(f"Error calculating gas price: {str(e)}")
            raise

    def _prepare_transaction(
        self, function_call, attempt: int = 0
    ) -> Dict[str, Union[int, str]]:
        """Prepare transaction parameters."""
        try:
            nonce = self._get_next_nonce()
            gas_price = self._calculate_gas_price(attempt)

            estimated_gas = function_call.estimate_gas({'from': self.account.address})
            gas_limit = int(estimated_gas * 1.5)  # Add 50% buffer

            transaction_params = {
                'chainId': self.SEPOLIA_CHAIN_ID,
                'from': self.account.address,
                'nonce': nonce,
                'gas': gas_limit,
                'gasPrice': gas_price
            }
            logger.info(f"Prepared transaction: {transaction_params}")
            return transaction_params
        except Exception as e:
            logger.error(f"Error preparing transaction: {str(e)}")
            raise

    def _send_transaction_with_retry(self, function_call) -> bool:
        """
        Send a transaction with retry logic.
        Reusable by share_cid, revoke_cid_access, add_hash_mapping, etc.
        """
        self.check_connection()

        for attempt in range(self.MAX_ATTEMPTS):
            try:
                tx_params = self._prepare_transaction(function_call, attempt)
                transaction = function_call.build_transaction(tx_params)

                signed_txn = self.w3.eth.account.sign_transaction(
                    transaction, self.private_key
                )
                tx_hash = self.w3.eth.send_raw_transaction(signed_txn.raw_transaction)
                logger.info(f"Sent transaction. Hash: {tx_hash.hex()}")

                receipt = self.w3.eth.wait_for_transaction_receipt(
                    tx_hash, timeout=60, poll_latency=2
                )
                if receipt.status == 1:
                    logger.info(f"Transaction successful. Receipt: {receipt}")
                    return True
                else:
                    logger.error(f"Transaction failed. Receipt: {receipt}")
                    return False

            except Exception as e:
                error_msg = str(e).lower()
                # These errors often require a retry
                if any(term in error_msg for term in [
                    'nonce too low',
                    'replacement transaction underpriced',
                    'transaction not found',
                    'too many requests'
                ]):
                    if attempt < self.MAX_ATTEMPTS - 1:
                        wait_time = self.RETRY_BASE_WAIT * (attempt + 1)
                        logger.warning(
                            f"Transaction attempt {attempt+1} failed with {e}. "
                            f"Retrying in {wait_time} seconds..."
                        )
                        time.sleep(wait_time)
                    else:
                        logger.error("Max retry attempts reached. Giving up.")
                        return False
                else:
                    logger.error(f"Failed to send transaction: {str(e)}")
                    raise

        return False

    def add_hash_mapping(
        self,
        table_name: str,
        partition: str,
        hash_value: str,
        prev_hash: str = "",
        record_type: str = "create",
        flag: str = "initial",
        row_count: int = 0
    ) -> bool:
        """Add a new hash mapping record with retry logic."""
        function_call = self.contract.functions.addHashMapping(
            table_name,
            partition,
            hash_value,
            prev_hash,
            record_type,
            flag,
            row_count
        )
        return self._send_transaction_with_retry(function_call)

    def share_cid(self, user: str, table_name: str, cid: str) -> bool:
        """Share a CID with another user."""
        user_address = self.w3.to_checksum_address(user)
        function_call = self.contract.functions.shareCID(
            user_address, table_name, cid
        )
        return self._send_transaction_with_retry(function_call)

    def revoke_cid_access(self, user: str, table_name: str, cid: str) -> bool:
        """Revoke CID access from a user."""
        user_address = self.w3.to_checksum_address(user)
        function_call = self.contract.functions.revokeCIDAccess(
            user_address, table_name, cid
        )
        return self._send_transaction_with_retry(function_call)

    def get_latest_hash(
        self, user: str, table_name: str, partition: str
    ) -> Optional[str]:
        """Get the latest hash for a table partition."""
        try:
            user_address = self.w3.to_checksum_address(user)
            return self.contract.functions.getLatestHash(
                user_address, table_name, partition
            ).call()
        except Exception as e:
            logger.error(f"Error getting latest hash: {str(e)}")
            return None

    def get_shared_cids(self, user: str, table_name: str) -> Tuple[List[str], List[str]]:
        """Get shared CIDs for a table and user."""
        try:
            user_address = self.w3.to_checksum_address(user)
            cids, owners = self.contract.functions.getSharedCIDs(
                user_address, table_name
            ).call()
            return cids, [self.w3.to_checksum_address(owner) for owner in owners]
        except Exception as e:
            logger.error(f"Error getting shared CIDs: {str(e)}")
            return [], []

    def get_hash_history(
        self, user: str, table_name: str, partition: str
    ) -> List[Dict[str, Union[str, int]]]:
        """Get complete hash history for a table partition."""
        try:
            user_address = self.w3.to_checksum_address(user)
            result = self.contract.functions.getHashHistory(
                user_address, table_name, partition
            ).call()

            history = []
            for i in range(len(result[0])):  # result[0] contains hashes
                record = {
                    "hash": result[0][i],
                    "prevHash": result[1][i],
                    "type": result[2][i],
                    "timestamp": datetime.fromtimestamp(result[3][i]).strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),
                    "flag": result[4][i],
                    "rowCount": result[5][i]
                }
                history.append(record)
            return history

        except Exception as e:
            logger.error(f"Error getting hash history: {str(e)}")
            return []

    def get_user_tables(self, user: str) -> List[str]:
        """Get all tables owned by a user."""
        try:
            user_address = self.w3.to_checksum_address(user)
            return self.contract.functions.getUserTables(user_address).call()
        except Exception as e:
            logger.error(f"Error getting user tables: {str(e)}")
            return []

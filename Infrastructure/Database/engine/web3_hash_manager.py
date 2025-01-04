from web3 import Web3
from web3.exceptions import TransactionNotFound, TimeExhausted
import time
from datetime import datetime
from typing import Dict, List, Optional, Union
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Web3HashManager:
    """
    Manages hash mappings in a smart contract on the Sepolia testnet.
    Handles connecting to the Ethereum network, sending transactions,
    and fetching hash data from the contract.
    """

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
        }
    ]

    SEPOLIA_CHAIN_ID = 11155111
    MAX_ATTEMPTS = 3          # Max attempts for transaction submission
    RETRY_BASE_WAIT = 3       # Base time in seconds to wait before retry
    BASE_MULTIPLIER = 1.5     # Base multiplier for gas price calculations
    MIN_GAS_PRICE = 10_000_000_000  # 10 Gwei as minimum gas price

    def __init__(self):
        """
        Initialize Web3 and the contract connection.
        Raises an exception if connection to the Ethereum network fails.
        """
        try:
            # Connect to Sepolia via Infura
            self.w3 = Web3(Web3.HTTPProvider(
                'https://sepolia.infura.io/v3/eb1d43f1429e49fba50e18fbf5ebd4ab'
            ))
            if not self.w3.is_connected():
                raise ConnectionError("Failed to connect to Ethereum network.")

            # Prepare contract & wallet details
            self.contract_address = self.w3.to_checksum_address(
                "0xfE04e7949311Cc6045AD6DA59eA22bce01b2C55e"
            )
            self.private_key = (
                '34cf59aaa5ef0a24e65b4e4dbe6fb23c2bd23a4d9a6b584d7995a141de719d53'
            )

            # self.private_key = (
            #     '0a13259d86ea6ffefda50f074c0aa4c72db594433585e572b27ffd9b6f60fd12'
            # )
            self.contract = self.w3.eth.contract(
                address=self.contract_address,
                abi=self.contract_abi
            )
            self.account = self.w3.eth.account.from_key(self.private_key)

            # For Sepolia, we'll determine nonce dynamically
            self.last_nonce = self.w3.eth.get_transaction_count(
                self.account.address, 'latest'
            )

            logger.info(
                f"Successfully connected to Ethereum. Account: {self.account.address}"
            )

        except Exception as e:
            logger.error(f"Failed to connect to Ethereum network: {str(e)}")
            raise

    def check_connection(self) -> bool:
        """
        Checks the connection to the Ethereum network.
        Raises an exception if not connected.
        """
        if not self.w3.is_connected():
            error_msg = "Lost connection to Ethereum network."
            logger.error(error_msg)
            raise ConnectionError(error_msg)
        return True

    def _get_next_nonce(self) -> int:
        """
        Get the next valid nonce for the current account on Sepolia.
        Always fetches the latest nonce to handle concurrency or missed transactions.
        """
        try:
            latest_nonce = self.w3.eth.get_transaction_count(
                self.account.address, 'latest'
            )
            # Update & return
            self.last_nonce = latest_nonce
            logger.info(f"Using nonce: {self.last_nonce}")
            return self.last_nonce
        except Exception as e:
            logger.error(f"Error getting next nonce: {str(e)}")
            raise

    def _calculate_gas_price(self, attempt: int = 0) -> int:
        """
        Calculate an appropriate gas price for the Sepolia testnet, 
        factoring in attempts for incremental increases.
        """
        try:
            base_fee = self.w3.eth.gas_price
            # Increase multiplier with each retry
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
        """
        Prepare the transaction parameters including nonce, gas limit, 
        and gas price. Returns a dictionary of parameters to build a transaction.
        """
        try:
            nonce = self._get_next_nonce()
            gas_price = self._calculate_gas_price(attempt)

            # Add a buffer to gas estimation to reduce out-of-gas errors
            estimated_gas = function_call.estimate_gas({'from': self.account.address})
            gas_limit = int(estimated_gas * 1.5)

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

    def add_hash_mapping(
        self,
        user_id: str,
        table_name: str,
        partition: str,
        hash_value: str,
        prev_hash: str = "",
        record_type: str = "create",
        flag: str = "initial",
        row_count: int = 0
    ) -> bool:
        """
        Adds a new hash mapping record on-chain.
        Includes retry logic for nonce or gas-related failures.

        :param user_id:     Ethereum address (as string) of the user
        :param table_name:  Name of the table
        :param partition:   Partition identifier
        :param hash_value:  The new hash to store
        :param prev_hash:   Previous hash (optional)
        :param record_type: Type of record (e.g. create, update, share)
        :param flag:        Arbitrary flag value
        :param row_count:   Row count associated with this table state
        :return:            True if transaction succeeds, False otherwise
        """
        self.check_connection()
        function_call = self.contract.functions.addHashMapping(
            table_name,
            partition,
            hash_value,
            prev_hash,
            record_type,
            flag,
            row_count
        )

        for attempt in range(self.MAX_ATTEMPTS):
            try:
                tx_params = self._prepare_transaction(function_call, attempt)
                transaction = function_call.build_transaction(tx_params)
                signed_txn = self.w3.eth.account.sign_transaction(
                    transaction, self.private_key
                )

                tx_hash = self.w3.eth.send_raw_transaction(signed_txn.raw_transaction)
                logger.info(f"Sent transaction. Hash: {tx_hash.hex()}")

                # Wait for on-chain confirmation
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
                # Retry if nonce or gas-related issues
                if any(term in error_msg for term in [
                    'nonce too low',
                    'replacement transaction underpriced',
                    'transaction not found'
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

    def get_latest_hash(
        self, user_id: str, table_name: str, partition: str
    ) -> Optional[str]:
        """
        Retrieve the most recent hash for a given table partition and user.

        :param user_id:    Ethereum address (as string) of the user
        :param table_name: Name of the table
        :param partition:  Partition identifier
        :return:           Latest IPFS hash if found, otherwise None
        """
        self.check_connection()
        try:
            user_address = self.w3.to_checksum_address(user_id)
            latest_hash = self.contract.functions.getLatestHash(
                user_address, table_name, partition
            ).call()
            logger.info(f"Retrieved latest hash: {latest_hash} for user {user_id}")
            return latest_hash or None
        except ValueError as e:
            logger.error(f"Invalid address format: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error getting latest hash: {str(e)}")
            raise

    def get_hash_history(
        self, user_id: str, table_name: str, partition: str
    ) -> List[Dict[str, Union[str, int]]]:
        """
        Get the complete hash history for a given table partition and user.

        :param user_id:    Ethereum address (as string) of the user
        :param table_name: Name of the table
        :param partition:  Partition identifier
        :return:           List of records with 'hash', 'prevHash', 'type',
                           'timestamp', 'flag', 'rowCount'
        """
        self.check_connection()
        try:
            user_address = self.w3.to_checksum_address(user_id)
            (
                hashes,
                prev_hashes,
                types,
                timestamps,
                flags,
                row_counts
            ) = self.contract.functions.getHashHistory(
                user_address, table_name, partition
            ).call()

            history = []
            for i in range(len(hashes)):
                record = {
                    "hash": hashes[i],
                    "prevHash": prev_hashes[i],
                    "type": types[i],
                    "timestamp": datetime.fromtimestamp(timestamps[i]).strftime(
                        "%Y-%m-%dT%H:%M:%SZ"
                    ),
                    "flag": flags[i],
                    "rowCount": row_counts[i]
                }
                history.append(record)

            logger.info(
                f"Retrieved hash history for user {user_id}, table {table_name}, partition {partition}"
            )
            return history

        except Exception as e:
            logger.error(f"Error getting hash history: {str(e)}")
            return []

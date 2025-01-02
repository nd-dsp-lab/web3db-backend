from web3 import Web3
from web3.exceptions import TransactionNotFound, TimeExhausted
import time
from datetime import datetime
from typing import Dict, List, Optional, Union
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

    def __init__(self):
        """Initialize Web3 and contract connection."""
        try:
            self.w3 = Web3(Web3.HTTPProvider('https://sepolia.infura.io/v3/eb1d43f1429e49fba50e18fbf5ebd4ab'))
            
            if not self.w3.is_connected():
                raise ConnectionError("Failed to connect to Ethereum network")
                
            self.contract_address = self.w3.to_checksum_address("0xfE04e7949311Cc6045AD6DA59eA22bce01b2C55e")
            self.private_key = '34cf59aaa5ef0a24e65b4e4dbe6fb23c2bd23a4d9a6b584d7995a141de719d53'
            
            self.contract = self.w3.eth.contract(
                address=self.contract_address,
                abi=self.contract_abi
            )
            
            self.account = self.w3.eth.account.from_key(self.private_key)
            self.last_nonce = self.w3.eth.get_transaction_count(self.account.address, 'latest')
            logger.info(f"Successfully connected to Ethereum network and contract. Account: {self.account.address}")
            
        except Exception as e:
            logger.error(f"Failed to connect to Ethereum network: {str(e)}")
            raise

    def check_connection(self) -> bool:
        """Check connection to Ethereum network."""
        try:
            if not self.w3.is_connected():
                raise ConnectionError("Lost connection to Ethereum network")
            return True
        except Exception as e:
            logger.error(f"Connection check failed: {str(e)}")
            raise

    def _get_next_nonce(self) -> int:
        """Get the next available nonce for Sepolia testnet."""
        try:
            # Always use 'latest' for more reliable nonce tracking on Sepolia
            latest_nonce = self.w3.eth.get_transaction_count(self.account.address, 'latest')
            
            # Initialize last_nonce if not set
            if not hasattr(self, 'last_nonce'):
                self.last_nonce = latest_nonce - 1
            
            # Increment based on latest nonce
            next_nonce = latest_nonce
            self.last_nonce = next_nonce
            
            logger.info(f"Using nonce: {next_nonce} (latest: {latest_nonce})")
            return next_nonce
        except Exception as e:
            logger.error(f"Error getting next nonce: {str(e)}")
            raise

    def _calculate_gas_price(self, base_fee: int = None, attempt: int = 0) -> int:
        """Calculate appropriate gas price for Sepolia testnet."""
        try:
            if base_fee is None:
                base_fee = self.w3.eth.gas_price
            
            # Higher base multiplier for Sepolia
            base_multiplier = 1.5
            retry_multiplier = 1.5 ** attempt
            
            # Calculate total multiplier
            total_multiplier = base_multiplier * retry_multiplier
            
            # Calculate final gas price
            final_gas_price = int(base_fee * total_multiplier)
            
            # Set minimum gas price for Sepolia (10 Gwei)
            min_gas_price = 10_000_000_000
            final_gas_price = max(final_gas_price, min_gas_price)
            
            logger.info(f"Gas price calculation: base={base_fee}, multiplier={total_multiplier}, final={final_gas_price}")
            return final_gas_price
        except Exception as e:
            logger.error(f"Error calculating gas price: {str(e)}")
            raise

    def _prepare_transaction(self, function_call, attempt: int = 0) -> Dict:
        """Prepare transaction with Sepolia-specific parameters."""
        try:
            nonce = self._get_next_nonce()
            gas_price = self._calculate_gas_price(None, attempt)
            
            # Get gas estimate with buffer
            gas_estimate = function_call.estimate_gas({'from': self.account.address})
            gas_limit = int(gas_estimate * 1.5)  # 50% buffer should be sufficient
            
            transaction_params = {
                'chainId': 11155111,  # Sepolia chain ID
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
        """Add a new hash mapping record with Sepolia-optimized handling."""
        try:
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

            max_attempts = 3  # Reduced number of attempts for faster feedback
            attempt = 0
            retry_base_wait = 3  # Base wait time in seconds
            
            while attempt < max_attempts:
                try:
                    transaction_params = self._prepare_transaction(function_call, attempt)
                    transaction = function_call.build_transaction(transaction_params)
                    
                    signed_txn = self.w3.eth.account.sign_transaction(
                        transaction,
                        self.private_key
                    )
                    
                    tx_hash = self.w3.eth.send_raw_transaction(signed_txn.raw_transaction)
                    logger.info(f"Transaction hash: {tx_hash.hex()}")
                    
                    # Wait for transaction confirmation
                    receipt = self.w3.eth.wait_for_transaction_receipt(
                        tx_hash,
                        timeout=60,  # 1 minute timeout
                        poll_latency=2  # Check every 2 seconds
                    )
                    
                    if receipt['status'] == 1:
                        logger.info(f"Transaction successful: {receipt}")
                        return True
                    else:
                        logger.error(f"Transaction failed: {receipt}")
                        return False
                    
                except Exception as e:
                    if any(error in str(e).lower() for error in ['nonce too low', 'replacement transaction underpriced', 'transaction not found']):
                        attempt += 1
                        if attempt == max_attempts:
                            raise
                        wait_time = retry_base_wait * (attempt + 1)
                        logger.warning(f"Transaction attempt {attempt} failed, retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                    else:
                        raise
                    
        except Exception as e:
            logger.error(f"Error adding hash mapping: {str(e)}")
            logger.exception("Full exception trace:")
            raise

    def get_latest_hash(self, user_id: str, table_name: str, partition: str) -> Optional[str]:
        """Get the most recent hash for a given partition."""
        try:
            self.check_connection()

            user_address = self.w3.to_checksum_address(user_id)
            latest_hash = self.contract.functions.getLatestHash(
                user_address,
                table_name,
                partition
            ).call()
            print("latest hash from blockchain: ", latest_hash)
            return latest_hash if latest_hash else None
            
        except ValueError as e:
            logger.error(f"Invalid address format: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error getting latest hash: {str(e)}")
            raise

    def get_hash_history(
        self,
        user_id: str,
        table_name: str,
        partition: str
    ) -> List[Dict[str, Union[str, int]]]:
        """Get complete hash history for a partition."""
        try:
            self.check_connection()

            user_address = self.w3.to_checksum_address(user_id)
            
            hashes, prev_hashes, types, timestamps, flags, row_counts = \
                self.contract.functions.getHashHistory(
                    user_address,
                    table_name,
                    partition
                ).call()
            
            history = []
            for i in range(len(hashes)):
                record = {
                    "hash": hashes[i],
                    "prevHash": prev_hashes[i],
                    "type": types[i],
                    "timestamp": datetime.fromtimestamp(timestamps[i]).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "flag": flags[i],
                    "rowCount": row_counts[i]
                }
                history.append(record)
                
            return history
            
        except Exception as e:
            logger.error(f"Error getting hash history: {str(e)}")
            return []
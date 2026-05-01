import os
from web3 import Web3
from dotenv import load_dotenv

class Web3dbContract:
    def __init__(self, contract_address=None, infura_api_key=None, private_key=None, rpc_url=None):
        self.infura_api_key = infura_api_key
        self.private_key = private_key
        self.contract_address = contract_address
        self.rpc_url = rpc_url or (
            f"https://sepolia.infura.io/v3/{infura_api_key}" if infura_api_key else None
        )
        print(f"RPC_URL: {self.rpc_url or 'Missing'}")
        print(f"PRIVATE_KEY: {'Present' if self.private_key else 'Missing'}")
        print(f"CONTRACT_ADDRESS: {'Present' if self.contract_address else 'Missing'}")

        if not self.rpc_url:
            raise Exception("Set RPC_URL or INFURA_API_KEY")
        try:
            self.w3 = Web3(Web3.HTTPProvider(self.rpc_url))
        except Exception as e:
            raise Exception(f"Failed to connect to RPC {self.rpc_url}: {e}")
        print(f"Connected to network: {self.w3.is_connected()}")
        
        # Set up account
        self.account = self.w3.eth.account.from_key(self.private_key)
        self.address = self.account.address
        print(f"Connected with address: {self.address}")
        
        # Updated Contract ABI with batch operations and schema management
        self.abi = [
            {
                "anonymous": False,
                "inputs": [
                    {
                        "indexed": False,
                        "internalType": "string",
                        "name": "attribute",
                        "type": "string"
                    },
                    {
                        "indexed": False,
                        "internalType": "string",
                        "name": "oldCID",
                        "type": "string"
                    },
                    {
                        "indexed": False,
                        "internalType": "string",
                        "name": "newCID",
                        "type": "string"
                    }
                ],
                "name": "IndexUpdated",
                "type": "event"
            },
            {
                "anonymous": False,
                "inputs": [
                    {
                        "indexed": False,
                        "internalType": "string[]",
                        "name": "attributes",
                        "type": "string[]"
                    },
                    {
                        "indexed": False,
                        "internalType": "string[]",
                        "name": "newCIDs",
                        "type": "string[]"
                    }
                ],
                "name": "BatchIndexUpdated",
                "type": "event"
            },
            {
                "anonymous": False,
                "inputs": [
                    {
                        "indexed": False,
                        "internalType": "string",
                        "name": "tableName",
                        "type": "string"
                    },
                    {
                        "indexed": False,
                        "internalType": "string",
                        "name": "oldSchema",
                        "type": "string"
                    },
                    {
                        "indexed": False,
                        "internalType": "string",
                        "name": "newSchema",
                        "type": "string"
                    }
                ],
                "name": "SchemaUpdated",
                "type": "event"
            },
            {
                "anonymous": False,
                "inputs": [
                    {
                        "indexed": False,
                        "internalType": "string[]",
                        "name": "attributes",
                        "type": "string[]"
                    },
                    {
                        "indexed": False,
                        "internalType": "string[]",
                        "name": "newCIDs",
                        "type": "string[]"
                    }
                ],
                "name": "BatchIndexUpdated",
                "type": "event"
            },
            {
                "inputs": [
                    {
                        "internalType": "string",
                        "name": "attribute",
                        "type": "string"
                    }
                ],
                "name": "getIndexCID",
                "outputs": [
                    {
                        "internalType": "string",
                        "name": "",
                        "type": "string"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "string[]",
                        "name": "attributes",
                        "type": "string[]"
                    }
                ],
                "name": "batchGetIndexCIDs",
                "outputs": [
                    {
                        "internalType": "string[]",
                        "name": "",
                        "type": "string[]"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "string",
                        "name": "attribute",
                        "type": "string"
                    },
                    {
                        "internalType": "string",
                        "name": "newCID",
                        "type": "string"
                    }
                ],
                "name": "updateIndexCID",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "string[]",
                        "name": "attributes",
                        "type": "string[]"
                    },
                    {
                        "internalType": "string[]",
                        "name": "newCIDs",
                        "type": "string[]"
                    }
                ],
                "name": "batchUpdateIndexCIDs",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "string",
                        "name": "attribute",
                        "type": "string"
                    }
                ],
                "name": "removeIndex",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "string",
                        "name": "tableName",
                        "type": "string"
                    },
                    {
                        "internalType": "string",
                        "name": "schemaJson",
                        "type": "string"
                    }
                ],
                "name": "updateTableSchema",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "string",
                        "name": "tableName",
                        "type": "string"
                    }
                ],
                "name": "getTableSchema",
                "outputs": [
                    {
                        "internalType": "string",
                        "name": "",
                        "type": "string"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "string[]",
                        "name": "tableNamesList",
                        "type": "string[]"
                    }
                ],
                "name": "batchGetTableSchemas",
                "outputs": [
                    {
                        "internalType": "string[]",
                        "name": "",
                        "type": "string[]"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [],
                "name": "getAllTableNames",
                "outputs": [
                    {
                        "internalType": "string[]",
                        "name": "",
                        "type": "string[]"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [],
                "name": "getAllTableSchemas",
                "outputs": [
                    {
                        "components": [
                            {
                                "internalType": "string",
                                "name": "tableName",
                                "type": "string"
                            },
                            {
                                "internalType": "string",
                                "name": "schemaJson",
                                "type": "string"
                            }
                        ],
                        "internalType": "struct Web3dbContract.TableSchema[]",
                        "name": "",
                        "type": "tuple[]"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "string",
                        "name": "tableName",
                        "type": "string"
                    }
                ],
                "name": "removeTableSchema",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "anonymous": False,
                "inputs": [
                    {"indexed": True, "internalType": "address", "name": "object", "type": "address"},
                    {"indexed": True, "internalType": "address", "name": "subject", "type": "address"},
                    {"indexed": False, "internalType": "string", "name": "tableName", "type": "string"},
                    {"indexed": False, "internalType": "string", "name": "policySql", "type": "string"}
                ],
                "name": "AccessPolicyAdded",
                "type": "event"
            },
            {
                "anonymous": False,
                "inputs": [
                    {"indexed": True, "internalType": "address", "name": "object", "type": "address"},
                    {"indexed": True, "internalType": "address", "name": "subject", "type": "address"},
                    {"indexed": False, "internalType": "string", "name": "tableName", "type": "string"},
                    {"indexed": False, "internalType": "string", "name": "policySql", "type": "string"}
                ],
                "name": "AccessPolicyRemoved",
                "type": "event"
            },
            {
                "inputs": [
                    {
                        "internalType": "address",
                        "name": "subject",
                        "type": "address"
                    },
                    {
                        "internalType": "address",
                        "name": "object",
                        "type": "address"
                    },
                    {
                        "internalType": "string",
                        "name": "tableName",
                        "type": "string"
                    },
                    {
                        "internalType": "string",
                        "name": "policySql",
                        "type": "string"
                    }
                ],
                "name": "addAccessPolicy",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "address",
                        "name": "objectAddress",
                        "type": "address"
                    }
                ],
                "name": "getAccessPolicies",
                "outputs": [
                    {
                        "components": [
                            {
                                "internalType": "address",
                                "name": "subject",
                                "type": "address"
                            },
                            {
                                "internalType": "string",
                                "name": "tableName",
                                "type": "string"
                            },
                            {
                                "internalType": "string",
                                "name": "policySql",
                                "type": "string"
                            },
                            {
                                "internalType": "address",
                                "name": "object",
                                "type": "address"
                            }
                        ],
                        "internalType": "struct Web3dbContract.AccessPolicy[]",
                        "name": "",
                        "type": "tuple[]"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "address",
                        "name": "objectAddress",
                        "type": "address"
                    }
                ],
                "name": "getPolicyCount",
                "outputs": [
                    {
                        "internalType": "uint256",
                        "name": "",
                        "type": "uint256"
                    }
                ],
                "stateMutability": "view",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "address",
                        "name": "objectAddress",
                        "type": "address"
                    },
                    {
                        "internalType": "uint256",
                        "name": "policyIndex",
                        "type": "uint256"
                    }
                ],
                "name": "removeAccessPolicy",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [
                    {
                        "internalType": "address",
                        "name": "objectAddress",
                        "type": "address"
                    }
                ],
                "name": "removeAllAccessPolicies",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function"
            },

            # ── CIDBatchLog events ────────────────────────────────────────────
            {
                "anonymous": False,
                "inputs": [
                    {"indexed": True,  "internalType": "bytes32",  "name": "batchId",       "type": "bytes32"},
                    {"indexed": True,  "internalType": "address",  "name": "sender",         "type": "address"},
                    {"indexed": True,  "internalType": "address",  "name": "receiver",       "type": "address"},
                    {"indexed": False, "internalType": "uint256",  "name": "cidCount",       "type": "uint256"},
                    {"indexed": False, "internalType": "bytes32",  "name": "aggregateHash",  "type": "bytes32"},
                    {"indexed": False, "internalType": "bytes32",  "name": "messageHash",    "type": "bytes32"},
                    {"indexed": False, "internalType": "uint256",  "name": "timelock",       "type": "uint256"}
                ],
                "name": "BatchCreated",
                "type": "event"
            },
            {
                "anonymous": False,
                "inputs": [
                    {"indexed": True,  "internalType": "bytes32",   "name": "batchId", "type": "bytes32"},
                    {"indexed": False, "internalType": "bytes32[]", "name": "cids",    "type": "bytes32[]"}
                ],
                "name": "CIDsLogged",
                "type": "event"
            },
            {
                "anonymous": False,
                "inputs": [
                    {"indexed": True,  "internalType": "bytes32", "name": "batchId",       "type": "bytes32"},
                    {"indexed": False, "internalType": "bytes32", "name": "aggregateHash", "type": "bytes32"}
                ],
                "name": "AggregateReleased",
                "type": "event"
            },
            {
                "anonymous": False,
                "inputs": [
                    {"indexed": True, "internalType": "bytes32", "name": "batchId",  "type": "bytes32"},
                    {"indexed": True, "internalType": "address", "name": "verifier", "type": "address"}
                ],
                "name": "BatchVerified",
                "type": "event"
            },

            # ── SecLog events ─────────────────────────────────────────────────
            {
                "anonymous": False,
                "inputs": [
                    {"indexed": True,  "internalType": "bytes32", "name": "logId",       "type": "bytes32"},
                    {"indexed": True,  "internalType": "address", "name": "sender",       "type": "address"},
                    {"indexed": True,  "internalType": "address", "name": "receiver",     "type": "address"},
                    {"indexed": False, "internalType": "uint256", "name": "sk1x",         "type": "uint256"},
                    {"indexed": False, "internalType": "uint256", "name": "sk1y",         "type": "uint256"},
                    {"indexed": False, "internalType": "bytes32", "name": "messageHash",  "type": "bytes32"},
                    {"indexed": False, "internalType": "uint256", "name": "timelock",     "type": "uint256"}
                ],
                "name": "LogEntryNew",
                "type": "event"
            },
            {
                "anonymous": False,
                "inputs": [
                    {"indexed": True, "internalType": "bytes32", "name": "logId", "type": "bytes32"}
                ],
                "name": "LogVerified",
                "type": "event"
            },

            # ── CIDBatchLog functions ─────────────────────────────────────────
            {
                "inputs": [
                    {"internalType": "bytes32[]", "name": "_cids", "type": "bytes32[]"}
                ],
                "name": "computeAggregate",
                "outputs": [
                    {"internalType": "bytes32", "name": "", "type": "bytes32"}
                ],
                "stateMutability": "pure",
                "type": "function"
            },
            {
                "inputs": [
                    {"internalType": "address",   "name": "_receiver",    "type": "address"},
                    {"internalType": "bytes32[]", "name": "_cids",        "type": "bytes32[]"},
                    {"internalType": "bytes32",   "name": "_messageHash", "type": "bytes32"},
                    {"internalType": "uint256",   "name": "_timelock",    "type": "uint256"}
                ],
                "name": "createBatch",
                "outputs": [
                    {"internalType": "bytes32", "name": "batchId",       "type": "bytes32"},
                    {"internalType": "bytes32", "name": "aggregateHash", "type": "bytes32"}
                ],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [
                    {"internalType": "bytes32", "name": "_batchId", "type": "bytes32"}
                ],
                "name": "releaseAggregate",
                "outputs": [
                    {"internalType": "bytes32", "name": "aggregateHash", "type": "bytes32"}
                ],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [
                    {"internalType": "bytes32",   "name": "_batchId", "type": "bytes32"},
                    {"internalType": "bytes32[]", "name": "_cids",    "type": "bytes32[]"}
                ],
                "name": "verifyCIDs",
                "outputs": [
                    {"internalType": "bool", "name": "", "type": "bool"}
                ],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [
                    {"internalType": "bytes32", "name": "_batchId", "type": "bytes32"},
                    {"internalType": "bytes",   "name": "_message", "type": "bytes"}
                ],
                "name": "verifyBatchMessage",
                "outputs": [
                    {"internalType": "bool", "name": "", "type": "bool"}
                ],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [
                    {"internalType": "bytes32", "name": "_batchId", "type": "bytes32"}
                ],
                "name": "getBatch",
                "outputs": [
                    {"internalType": "address", "name": "sender",        "type": "address"},
                    {"internalType": "address", "name": "receiver",      "type": "address"},
                    {"internalType": "uint256", "name": "cidCount",      "type": "uint256"},
                    {"internalType": "bytes32", "name": "aggregateHash", "type": "bytes32"},
                    {"internalType": "bytes32", "name": "messageHash",   "type": "bytes32"},
                    {"internalType": "uint256", "name": "timelock",      "type": "uint256"},
                    {"internalType": "bool",    "name": "released",      "type": "bool"},
                    {"internalType": "bool",    "name": "verified",      "type": "bool"}
                ],
                "stateMutability": "view",
                "type": "function"
            },

            # ── SecLog functions ──────────────────────────────────────────────
            {
                "inputs": [
                    {"internalType": "address", "name": "_receiver",    "type": "address"},
                    {"internalType": "uint256", "name": "_sk1x",        "type": "uint256"},
                    {"internalType": "uint256", "name": "_sk1y",        "type": "uint256"},
                    {"internalType": "bytes32", "name": "_messageHash", "type": "bytes32"},
                    {"internalType": "uint256", "name": "_timelock",    "type": "uint256"}
                ],
                "name": "newLog",
                "outputs": [
                    {"internalType": "bytes32", "name": "logId", "type": "bytes32"}
                ],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [
                    {"internalType": "bytes32", "name": "_logId",   "type": "bytes32"},
                    {"internalType": "uint256", "name": "_sk2",     "type": "uint256"},
                    {"internalType": "bytes",   "name": "_message", "type": "bytes"}
                ],
                "name": "verifyLog",
                "outputs": [
                    {"internalType": "bool", "name": "", "type": "bool"}
                ],
                "stateMutability": "nonpayable",
                "type": "function"
            },
            {
                "inputs": [
                    {"internalType": "bytes32", "name": "_logId", "type": "bytes32"}
                ],
                "name": "getLog",
                "outputs": [
                    {"internalType": "address", "name": "sender",      "type": "address"},
                    {"internalType": "address", "name": "receiver",    "type": "address"},
                    {"internalType": "uint256", "name": "sk1x",        "type": "uint256"},
                    {"internalType": "uint256", "name": "sk1y",        "type": "uint256"},
                    {"internalType": "uint256", "name": "timelock",    "type": "uint256"},
                    {"internalType": "bytes32", "name": "messageHash", "type": "bytes32"},
                    {"internalType": "bool",    "name": "verified",    "type": "bool"}
                ],
                "stateMutability": "view",
                "type": "function"
            }
        ]
        
        # Create contract instance
        self.contract = self.w3.eth.contract(address=self.contract_address, abi=self.abi)
    
    def _get_gas_price(self):
        """
        Get a reasonable gas price for transactions.
        Returns at least 2 gwei to ensure transactions are mined.
        """
        current_gas_price = self.w3.eth.gas_price
        min_gas_price = self.w3.to_wei('2', 'gwei')  # 2 gwei minimum
        return max(current_gas_price, min_gas_price)
        
    def update_index(self, attribute, new_cid):
        """
        Update a single index CID
        
        Args:
            attribute (str): The attribute name
            new_cid (str): The new CID value
            
        Returns:
            tuple: (success, message)
        """
        try:
            # Build transaction
            nonce = self.w3.eth.get_transaction_count(self.address)
            
            tx = self.contract.functions.updateIndexCID(
                attribute,
                new_cid
            ).build_transaction({
                'from': self.address,
                'gas': 2000000,
                'gasPrice': self._get_gas_price(),
                'nonce': nonce,
            })
            
            # Sign and send transaction
            signed_tx = self.w3.eth.account.sign_transaction(tx, self.private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            
            # Wait for transaction receipt
            print(f"Transaction sent: {tx_hash.hex()}")
            tx_receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
            print(f"Transaction confirmed in block {tx_receipt['blockNumber']}")
            
            # Process events
            logs = self.contract.events.IndexUpdated().process_receipt(tx_receipt)
            if logs:
                print(f"Index updated: attribute={logs[0]['args']['attribute']}, "
                      f"oldCID={logs[0]['args']['oldCID']}, "
                      f"newCID={logs[0]['args']['newCID']}")
                return True
            else:
                print("Event not found in transaction receipt")
                return False
        
        except Exception as e:
            print(f"Failed to update index: {e}")
            return False
    
    def batch_update_indices(self, attributes, new_cids):
        """
        Update multiple index CIDs in a single transaction
        
        Args:
            attributes (list): List of attribute names
            new_cids (list): List of new CIDs corresponding to attributes
            
        Returns:
            bool: True if successful, False otherwise
        """
        if len(attributes) != len(new_cids):
            print("Error: Attributes and CIDs lists must have the same length")
            return False
            
        try:
            # Build transaction
            nonce = self.w3.eth.get_transaction_count(self.address)
            
            tx = self.contract.functions.batchUpdateIndexCIDs(
                attributes,
                new_cids
            ).build_transaction({
                'from': self.address,
                'gas': 3000000,  # Increased gas limit for batch operation
                'gasPrice': self._get_gas_price(),
                'nonce': nonce,
            })
            
            # Sign and send transaction
            signed_tx = self.w3.eth.account.sign_transaction(tx, self.private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            
            # Wait for transaction receipt
            print(f"Batch update transaction sent: {tx_hash.hex()}")
            tx_receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
            print(f"Batch update transaction confirmed in block {tx_receipt['blockNumber']}")
            
            # Process batch event
            batch_logs = self.contract.events.BatchIndexUpdated().process_receipt(tx_receipt)
            if batch_logs:
                print(f"Batch update successful for {len(batch_logs[0]['args']['attributes'])} attributes")
                
                # Also process individual update events for detailed logging
                update_logs = self.contract.events.IndexUpdated().process_receipt(tx_receipt)
                if update_logs:
                    for log in update_logs:
                        print(f"Index updated: attribute={log['args']['attribute']}, "
                              f"oldCID={log['args']['oldCID']}, "
                              f"newCID={log['args']['newCID']}")
                
                return True
            else:
                print("BatchIndexUpdated event not found in transaction receipt")
                return False
        
        except Exception as e:
            print(f"Failed to update batch indices: {e}")
            return False
    
    def get_index(self, attribute):
        """
        Get index CID for a single attribute
        
        Args:
            attribute (str): The attribute name
            
        Returns:
            tuple: (success, cid)
        """
        try:
            # Call the smart contract function
            current_cid = self.contract.functions.getIndexCID(attribute).call()
            return True, current_cid
        except Exception as e:
            print(f"Error retrieving index for {attribute}: {e}")
            return False, str(e)
    
    def batch_get_indices(self, attributes):
        """
        Get multiple index CIDs in a single call
        
        Args:
            attributes (list): List of attribute names
            
        Returns:
            tuple: (success, dict of attribute->CID mappings)
        """
        try:
            # Call the smart contract function
            cids = self.contract.functions.batchGetIndexCIDs(attributes).call()
            
            # Create a dictionary of results
            result_dict = {attributes[i]: cids[i] for i in range(len(attributes))}
            return True, result_dict
        except Exception as e:
            print(f"Error retrieving batch indices: {e}")
            return False, {}
    
    def remove_index(self, attribute):
        """
        Remove an index
        
        Args:
            attribute (str): The attribute name to remove
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Build transaction
            nonce = self.w3.eth.get_transaction_count(self.address)
            
            tx = self.contract.functions.removeIndex(attribute).build_transaction({
                'from': self.address,
                'gas': 2000000,
                'gasPrice': self._get_gas_price(),
                'nonce': nonce,
            })
            
            # Sign and send transaction
            signed_tx = self.w3.eth.account.sign_transaction(tx, self.private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            
            # Wait for transaction receipt
            print(f"Remove index transaction sent: {tx_hash.hex()}")
            tx_receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
            print(f"Remove index transaction confirmed in block {tx_receipt['blockNumber']}")
            
            # Process events
            logs = self.contract.events.IndexUpdated().process_receipt(tx_receipt)
            if logs:
                print(f"Index removed: attribute={logs[0]['args']['attribute']}, "
                      f"oldCID={logs[0]['args']['oldCID']}")
                return True
            else:
                print("Event not found in transaction receipt")
                return False
                
        except Exception as e:
            print(f"Failed to remove index: {e}")
            return False
    
    def update_table_schema(self, table_name, schema_json):
        """
        Update a table schema in the smart contract
        
        Args:
            table_name (str): The table name
            schema_json (str): The schema as JSON string
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Build transaction
            nonce = self.w3.eth.get_transaction_count(self.address)
            
            tx = self.contract.functions.updateTableSchema(
                table_name,
                schema_json
            ).build_transaction({
                'from': self.address,
                'gas': 2000000,
                'gasPrice': self._get_gas_price(),
                'nonce': nonce,
            })
            
            # Sign and send transaction
            signed_tx = self.w3.eth.account.sign_transaction(tx, self.private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            
            # Wait for transaction receipt
            print(f"Schema update transaction sent: {tx_hash.hex()}")
            tx_receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
            print(f"Schema update transaction confirmed in block {tx_receipt['blockNumber']}")
            
            # Process events (but don't fail if event parsing fails)
            try:
                logs = self.contract.events.SchemaUpdated().process_receipt(tx_receipt)
                if logs:
                    print(f"Schema updated: table={logs[0]['args']['tableName']}")
                else:
                    print("SchemaUpdated event not found in transaction receipt")
            except Exception as event_error:
                print(f"Warning: Could not process SchemaUpdated event: {event_error}")
            
            # Consider transaction successful if it was mined (status = 1)
            if tx_receipt.get('status') == 1:
                print(f"Schema update transaction successful (status: {tx_receipt.get('status')})")
                return True
            else:
                print(f"Schema update transaction failed (status: {tx_receipt.get('status')})")
                return False
                
        except Exception as e:
            print(f"Failed to update table schema: {e}")
            return False

    def get_table_schema(self, table_name):
        """
        Get a table schema from the smart contract
        
        Args:
            table_name (str): The table name
            
        Returns:
            tuple: (success, schema_json) where success is bool and schema_json is str
        """
        try:
            schema = self.contract.functions.getTableSchema(table_name).call()
            return True, schema
        except Exception as e:
            print(f"Failed to get table schema for {table_name}: {e}")
            return False, None

    def batch_get_table_schemas(self, table_names):
        """
        Get multiple table schemas in one call
        
        Args:
            table_names (list): List of table names
            
        Returns:
            tuple: (success, schemas_dict) where success is bool and schemas_dict maps table names to schemas
        """
        try:
            schemas = self.contract.functions.batchGetTableSchemas(table_names).call()
            schema_dict = {table_names[i]: schemas[i] for i in range(len(table_names))}
            return True, schema_dict
        except Exception as e:
            print(f"Failed to get batch table schemas: {e}")
            return False, {}

    def get_all_table_names(self):
        """
        Get all table names that have schemas stored
        
        Returns:
            tuple: (success, table_names_list) where success is bool and table_names_list is list of strings
        """
        try:
            table_names = self.contract.functions.getAllTableNames().call()
            return True, table_names
        except Exception as e:
            print(f"Failed to get all table names: {e}")
            return False, []

    def get_all_table_schemas(self):
        """
        Get all table schemas with their names
        
        Returns:
            tuple: (success, schemas_dict) where success is bool and schemas_dict maps table names to schemas
        """
        try:
            table_schemas = self.contract.functions.getAllTableSchemas().call()
            schema_dict = {}
            for schema_tuple in table_schemas:
                table_name = schema_tuple[0]  # tableName
                schema_json = schema_tuple[1]  # schemaJson
                if schema_json:  # Only include non-empty schemas
                    schema_dict[table_name] = schema_json
            return True, schema_dict
        except Exception as e:
            print(f"Failed to get all table schemas: {e}")
            return False, {}

    def remove_table_schema(self, table_name):
        """
        Remove a table schema from the smart contract
        
        Args:
            table_name (str): The table name to remove
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Build transaction
            nonce = self.w3.eth.get_transaction_count(self.address)
            
            tx = self.contract.functions.removeTableSchema(table_name).build_transaction({
                'from': self.address,
                'gas': 2000000,
                'gasPrice': self._get_gas_price(),
                'nonce': nonce,
            })
            
            # Sign and send transaction
            signed_tx = self.w3.eth.account.sign_transaction(tx, self.private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            
            # Wait for transaction receipt
            print(f"Schema removal transaction sent: {tx_hash.hex()}")
            tx_receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
            print(f"Schema removal transaction confirmed in block {tx_receipt['blockNumber']}")
            
            # Process events
            logs = self.contract.events.SchemaUpdated().process_receipt(tx_receipt)
            if logs:
                print(f"Schema removed: table={logs[0]['args']['tableName']}")
                return True
            else:
                print("SchemaUpdated event not found in transaction receipt")
                return False
                
        except Exception as e:
            print(f"Failed to remove table schema: {e}")
            return False
    
    # Access Policy Management Methods
    
    def add_access_policy(self, subject_address, object_address, table_name, policy_sql):
        """
        Add an access policy for an object address
        
        Args:
            subject_address (str): The subject address (policy creator/owner)
            object_address (str): The object address (querier)
            table_name (str): The table name
            policy_sql (str): The SQL policy string
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Convert string addresses to checksum addresses
            subject_address = Web3.to_checksum_address(subject_address)
            object_address = Web3.to_checksum_address(object_address)
            
            # Build transaction
            nonce = self.w3.eth.get_transaction_count(self.address)
            
            tx = self.contract.functions.addAccessPolicy(
                subject_address,
                object_address,
                table_name,
                policy_sql
            ).build_transaction({
                'from': self.address,
                'gas': 2000000,
                'gasPrice': self._get_gas_price(),
                'nonce': nonce,
            })
            
            # Sign and send transaction
            signed_tx = self.w3.eth.account.sign_transaction(tx, self.private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            
            # Wait for transaction receipt
            print(f"Add access policy transaction sent: {tx_hash.hex()}")
            tx_receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
            print(f"Add access policy transaction confirmed in block {tx_receipt['blockNumber']}")
            
            # Process events
            try:
                logs = self.contract.events.AccessPolicyAdded().process_receipt(tx_receipt)
                if logs:
                    print(f"Access policy added: wallet={logs[0]['args']['walletAddress']}, "
                          f"table={logs[0]['args']['tableName']}")
                else:
                    print("AccessPolicyAdded event not found in transaction receipt")
            except Exception as event_error:
                print(f"Warning: Could not process AccessPolicyAdded event: {event_error}")
            
            # Consider transaction successful if it was mined (status = 1)
            if tx_receipt.get('status') == 1:
                return True
            else:
                print(f"Add access policy transaction failed (status: {tx_receipt.get('status')})")
                return False
                
        except Exception as e:
            print(f"Failed to add access policy: {e}")
            return False
    
    def get_access_policies(self, wallet_address):
        """
        Get all access policies for a wallet address
        
        Args:
            wallet_address (str): The wallet address (e.g., "0x123...")
            
        Returns:
            tuple: (success, policies_list) where success is bool and policies_list is list of dicts
        """
        try:
            # Convert string address to checksum address
            wallet_address = Web3.to_checksum_address(wallet_address)
            
            # Call the smart contract function
            policies = self.contract.functions.getAccessPolicies(wallet_address).call()
            
            # Convert tuple results to list of dictionaries
            policy_list = []
            for policy in policies:
                policy_dict = {
                    'subject': policy[0],
                    'tableName': policy[1],
                    'policySql': policy[2],
                    'object': policy[3]
                }
                policy_list.append(policy_dict)
                
            return True, policy_list
        except Exception as e:
            print(f"Failed to get access policies for {wallet_address}: {e}")
            return False, []
    
    def get_policies_granted_by(self, subject_address, from_block=0):
        """
        Return policies where subject_address is the grantor (subject).

        Uses indexed event logs. Computes (added - removed) by matching on
        (object, tableName, policySql). Removed events are also filtered by
        subject so the comparison set stays small.
        """
        try:
            subject_address = Web3.to_checksum_address(subject_address)
            added = self.contract.events.AccessPolicyAdded.get_logs(
                from_block=from_block, to_block='latest',
                argument_filters={'subject': subject_address},
            )
            removed = self.contract.events.AccessPolicyRemoved.get_logs(
                from_block=from_block, to_block='latest',
                argument_filters={'subject': subject_address},
            )

            def key(log):
                a = log['args']
                return (
                    Web3.to_checksum_address(a['object']),
                    a['tableName'],
                    a['policySql'],
                )

            removed_counts = {}
            for log in removed:
                k = key(log)
                removed_counts[k] = removed_counts.get(k, 0) + 1

            granted = []
            object_policy_cache = {}
            for log in added:
                k = key(log)
                if removed_counts.get(k, 0) > 0:
                    removed_counts[k] -= 1
                    continue
                obj = k[0]
                if obj not in object_policy_cache:
                    object_policy_cache[obj] = self.contract.functions.getAccessPolicies(obj).call()
                idx = next(
                    (i for i, p in enumerate(object_policy_cache[obj])
                     if Web3.to_checksum_address(p[0]) == subject_address
                     and p[1] == k[1] and p[2] == k[2]),
                    None,
                )
                granted.append({
                    'subject': subject_address,
                    'object': obj,
                    'tableName': k[1],
                    'policySql': k[2],
                    'object_policy_index': idx,
                })
            return True, granted
        except Exception as e:
            print(f"Failed to get policies granted by {subject_address}: {e}")
            return False, []

    def get_policy_count(self, wallet_address):
        """
        Get the count of policies for a wallet address
        
        Args:
            wallet_address (str): The wallet address
            
        Returns:
            tuple: (success, count) where success is bool and count is int
        """
        try:
            # Convert string address to checksum address
            wallet_address = Web3.to_checksum_address(wallet_address)
            
            # Call the smart contract function
            count = self.contract.functions.getPolicyCount(wallet_address).call()
            return True, count
        except Exception as e:
            print(f"Failed to get policy count for {wallet_address}: {e}")
            return False, 0
    
    def remove_access_policy(self, wallet_address, policy_index):
        """
        Remove a specific access policy by index
        
        Args:
            wallet_address (str): The wallet address
            policy_index (int): The index of the policy to remove
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Convert string address to checksum address
            wallet_address = Web3.to_checksum_address(wallet_address)
            
            # Build transaction
            nonce = self.w3.eth.get_transaction_count(self.address)
            
            tx = self.contract.functions.removeAccessPolicy(
                wallet_address,
                policy_index
            ).build_transaction({
                'from': self.address,
                'gas': 2000000,
                'gasPrice': self._get_gas_price(),
                'nonce': nonce,
            })
            
            # Sign and send transaction
            signed_tx = self.w3.eth.account.sign_transaction(tx, self.private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            
            # Wait for transaction receipt
            print(f"Remove access policy transaction sent: {tx_hash.hex()}")
            tx_receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
            print(f"Remove access policy transaction confirmed in block {tx_receipt['blockNumber']}")
            
            # Process events
            try:
                logs = self.contract.events.AccessPolicyRemoved().process_receipt(tx_receipt)
                if logs:
                    print(f"Access policy removed: wallet={logs[0]['args']['walletAddress']}, "
                          f"table={logs[0]['args']['tableName']}")
                else:
                    print("AccessPolicyRemoved event not found in transaction receipt")
            except Exception as event_error:
                print(f"Warning: Could not process AccessPolicyRemoved event: {event_error}")
            
            # Consider transaction successful if it was mined (status = 1)
            if tx_receipt.get('status') == 1:
                return True
            else:
                print(f"Remove access policy transaction failed (status: {tx_receipt.get('status')})")
                return False
                
        except Exception as e:
            print(f"Failed to remove access policy: {e}")
            return False
    
    def remove_all_access_policies(self, wallet_address):
        """
        Remove all access policies for a wallet address
        
        Args:
            wallet_address (str): The wallet address
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Convert string address to checksum address
            wallet_address = Web3.to_checksum_address(wallet_address)
            
            # Build transaction
            nonce = self.w3.eth.get_transaction_count(self.address)
            
            tx = self.contract.functions.removeAllAccessPolicies(
                wallet_address
            ).build_transaction({
                'from': self.address,
                'gas': 2000000,
                'gasPrice': self._get_gas_price(),
                'nonce': nonce,
            })
            
            # Sign and send transaction
            signed_tx = self.w3.eth.account.sign_transaction(tx, self.private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            
            # Wait for transaction receipt
            print(f"Remove all access policies transaction sent: {tx_hash.hex()}")
            tx_receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
            print(f"Remove all access policies transaction confirmed in block {tx_receipt['blockNumber']}")
            
            # Process events
            try:
                logs = self.contract.events.AccessPolicyRemoved().process_receipt(tx_receipt)
                if logs:
                    print(f"All access policies removed: wallet={logs[0]['args']['walletAddress']}")
                else:
                    print("AccessPolicyRemoved event not found in transaction receipt")
            except Exception as event_error:
                print(f"Warning: Could not process AccessPolicyRemoved event: {event_error}")
            
            # Consider transaction successful if it was mined (status = 1)
            if tx_receipt.get('status') == 1:
                return True
            else:
                print(f"Remove all access policies transaction failed (status: {tx_receipt.get('status')})")
                return False
                
        except Exception as e:
            print(f"Failed to remove all access policies: {e}")
            return False

    # =========================================================================
    # CIDBatchLog Methods
    # =========================================================================

    def compute_aggregate(self, cids):
        """
        Compute the sha256 aggregate hash for a list of CIDs (pure, no gas).

        Args:
            cids (list[bytes]): List of CID digests, each exactly 32 bytes.

        Returns:
            tuple: (success, aggregate_hash_bytes) where aggregate_hash_bytes is bytes32.
        """
        try:
            cids_bytes32 = [c if isinstance(c, bytes) else bytes.fromhex(c.replace('0x', '')) for c in cids]
            result = self.contract.functions.computeAggregate(cids_bytes32).call()
            return True, result
        except Exception as e:
            print(f"Failed to compute aggregate: {e}")
            return False, None

    def create_batch(self, receiver_address, cids, message_hash=None, timelock=None):
        """
        Commit to an ordered list of CIDs via their aggregate hash.
        The full CID list is emitted in the CIDsLogged event (not stored on-chain).

        Args:
            receiver_address (str): Ethereum address authorised to verify the batch.
            cids (list[bytes]):     Ordered CID list, each element exactly 32 bytes.
            message_hash (bytes):   Optional 32-byte application message hash.
                                    Pass None or bytes(32) to omit.
            timelock (int):         Unix timestamp (must be in the future).

        Returns:
            tuple: (success, batch_id, aggregate_hash)
                   batch_id and aggregate_hash are bytes32 values (bytes).
        """
        try:
            receiver_address = Web3.to_checksum_address(receiver_address)
            cids_bytes32 = [c if isinstance(c, bytes) else bytes.fromhex(c.replace('0x', '')) for c in cids]
            msg_hash = message_hash if message_hash is not None else bytes(32)

            nonce = self.w3.eth.get_transaction_count(self.address)
            tx = self.contract.functions.createBatch(
                receiver_address,
                cids_bytes32,
                msg_hash,
                timelock
            ).build_transaction({
                'from': self.address,
                'gas': 2000000,
                'gasPrice': self._get_gas_price(),
                'nonce': nonce,
            })

            signed_tx = self.w3.eth.account.sign_transaction(tx, self.private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            print(f"createBatch transaction sent: {tx_hash.hex()}")
            tx_receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
            print(f"createBatch confirmed in block {tx_receipt['blockNumber']}")

            if tx_receipt.get('status') != 1:
                print(f"createBatch transaction failed (status: {tx_receipt.get('status')})")
                return False, None, None

            logs = self.contract.events.BatchCreated().process_receipt(tx_receipt)
            if logs:
                batch_id = logs[0]['args']['batchId']
                agg_hash = logs[0]['args']['aggregateHash']
                print(f"Batch created: batchId={batch_id.hex()}, aggregateHash={agg_hash.hex()}")
                return True, batch_id, agg_hash

            print("BatchCreated event not found in receipt")
            return False, None, None

        except Exception as e:
            print(f"Failed to create batch: {e}")
            return False, None, None

    def release_aggregate(self, batch_id):
        """
        Sender explicitly releases the aggregate hash on-chain as a delivery signal.

        Args:
            batch_id (bytes): The 32-byte batch ID returned by create_batch.

        Returns:
            tuple: (success, aggregate_hash_bytes)
        """
        try:
            nonce = self.w3.eth.get_transaction_count(self.address)
            tx = self.contract.functions.releaseAggregate(batch_id).build_transaction({
                'from': self.address,
                'gas': 2000000,
                'gasPrice': self._get_gas_price(),
                'nonce': nonce,
            })

            signed_tx = self.w3.eth.account.sign_transaction(tx, self.private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            print(f"releaseAggregate transaction sent: {tx_hash.hex()}")
            tx_receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
            print(f"releaseAggregate confirmed in block {tx_receipt['blockNumber']}")

            if tx_receipt.get('status') != 1:
                return False, None

            logs = self.contract.events.AggregateReleased().process_receipt(tx_receipt)
            if logs:
                agg_hash = logs[0]['args']['aggregateHash']
                print(f"Aggregate released: {agg_hash.hex()}")
                return True, agg_hash

            return False, None

        except Exception as e:
            print(f"Failed to release aggregate: {e}")
            return False, None

    def verify_cids(self, batch_id, cids):
        """
        Verify that a CID list matches the aggregate commitment stored in the batch.

        Args:
            batch_id (bytes):   The 32-byte batch ID to verify against.
            cids (list[bytes]): The ordered CID list to verify (each 32 bytes).

        Returns:
            bool: True if verification succeeded, False otherwise.
        """
        try:
            cids_bytes32 = [c if isinstance(c, bytes) else bytes.fromhex(c.replace('0x', '')) for c in cids]

            nonce = self.w3.eth.get_transaction_count(self.address)
            tx = self.contract.functions.verifyCIDs(
                batch_id,
                cids_bytes32
            ).build_transaction({
                'from': self.address,
                'gas': 2000000,
                'gasPrice': self._get_gas_price(),
                'nonce': nonce,
            })

            signed_tx = self.w3.eth.account.sign_transaction(tx, self.private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            print(f"verifyCIDs transaction sent: {tx_hash.hex()}")
            tx_receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
            print(f"verifyCIDs confirmed in block {tx_receipt['blockNumber']}")

            if tx_receipt.get('status') == 1:
                print(f"CIDs verified for batchId={batch_id.hex()}")
                return True

            print(f"verifyCIDs transaction failed (status: {tx_receipt.get('status')})")
            return False

        except Exception as e:
            print(f"Failed to verify CIDs: {e}")
            return False

    def verify_batch_message(self, batch_id, message):
        """
        Verify that a raw message matches the messageHash stored in a batch.

        Args:
            batch_id (bytes): The 32-byte batch ID.
            message (bytes):  The raw message bytes to verify.

        Returns:
            bool: True if the message hash matches, False otherwise.
        """
        try:
            nonce = self.w3.eth.get_transaction_count(self.address)
            tx = self.contract.functions.verifyBatchMessage(
                batch_id,
                message
            ).build_transaction({
                'from': self.address,
                'gas': 2000000,
                'gasPrice': self._get_gas_price(),
                'nonce': nonce,
            })

            signed_tx = self.w3.eth.account.sign_transaction(tx, self.private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            tx_receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)

            return tx_receipt.get('status') == 1

        except Exception as e:
            print(f"Failed to verify batch message: {e}")
            return False

    def get_batch(self, batch_id):
        """
        Retrieve full batch metadata by ID.

        Args:
            batch_id (bytes): The 32-byte batch ID.

        Returns:
            tuple: (success, dict) where dict has keys:
                   sender, receiver, cid_count, aggregate_hash,
                   message_hash, timelock, released, verified.
        """
        try:
            result = self.contract.functions.getBatch(batch_id).call()
            return True, {
                'sender':         result[0],
                'receiver':       result[1],
                'cid_count':      result[2],
                'aggregate_hash': result[3],
                'message_hash':   result[4],
                'timelock':       result[5],
                'released':       result[6],
                'verified':       result[7],
            }
        except Exception as e:
            print(f"Failed to get batch {batch_id}: {e}")
            return False, {}

    # =========================================================================
    # SecLog Methods
    # =========================================================================

    def new_log(self, receiver_address, sk1x, sk1y, message_hash, timelock):
        """
        Create a new log entry committing to an EC public key point (sk1x, sk1y).
        The point must equal sk2 * G where sk2 is the sender's private scalar.
        The receiver proves knowledge of sk2 later via verify_log().

        Args:
            receiver_address (str): Ethereum address of the log receiver.
            sk1x (int):             x-coordinate of the committed EC point.
            sk1y (int):             y-coordinate of the committed EC point.
            message_hash (bytes):   keccak256 hash of the message to log (32 bytes).
            timelock (int):         Unix timestamp — must be in the future.

        Returns:
            tuple: (success, log_id) where log_id is a bytes32 value (bytes).
        """
        try:
            receiver_address = Web3.to_checksum_address(receiver_address)

            nonce = self.w3.eth.get_transaction_count(self.address)
            tx = self.contract.functions.newLog(
                receiver_address,
                sk1x,
                sk1y,
                message_hash,
                timelock
            ).build_transaction({
                'from': self.address,
                'gas': 2000000,
                'gasPrice': self._get_gas_price(),
                'nonce': nonce,
            })

            signed_tx = self.w3.eth.account.sign_transaction(tx, self.private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            print(f"newLog transaction sent: {tx_hash.hex()}")
            tx_receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
            print(f"newLog confirmed in block {tx_receipt['blockNumber']}")

            if tx_receipt.get('status') != 1:
                print(f"newLog transaction failed (status: {tx_receipt.get('status')})")
                return False, None

            logs = self.contract.events.LogEntryNew().process_receipt(tx_receipt)
            if logs:
                log_id = logs[0]['args']['logId']
                print(f"Log created: logId={log_id.hex()}")
                return True, log_id

            print("LogEntryNew event not found in receipt")
            return False, None

        except Exception as e:
            print(f"Failed to create log: {e}")
            return False, None

    def verify_log(self, log_id, sk2, message):
        """
        Receiver verifies the log by supplying the private scalar sk2 and original message.
        Two proofs are checked on-chain:
          1. EC proof:      sk2 * G == (sk1x, sk1y)
          2. Message proof: keccak256(message) == messageHash

        NOTE: EllipticCurve.ecMul() is compute-intensive on secp256k1 in pure Solidity.
              Uses gas=3000000 to account for the EC multiplication cost.

        Args:
            log_id  (bytes): The 32-byte log ID returned by new_log.
            sk2     (int):   The private scalar whose public key matches (sk1x, sk1y).
            message (bytes): The original plaintext message (not its hash).

        Returns:
            bool: True if both proofs pass and the log is marked verified, False otherwise.
        """
        try:
            nonce = self.w3.eth.get_transaction_count(self.address)
            tx = self.contract.functions.verifyLog(
                log_id,
                sk2,
                message
            ).build_transaction({
                'from': self.address,
                'gas': 3000000,  # Increased: EllipticCurve.ecMul is compute-intensive
                'gasPrice': self._get_gas_price(),
                'nonce': nonce,
            })

            signed_tx = self.w3.eth.account.sign_transaction(tx, self.private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.raw_transaction)
            print(f"verifyLog transaction sent: {tx_hash.hex()}")
            tx_receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)
            print(f"verifyLog confirmed in block {tx_receipt['blockNumber']}")

            if tx_receipt.get('status') != 1:
                print(f"verifyLog transaction failed (status: {tx_receipt.get('status')})")
                return False

            try:
                event_logs = self.contract.events.LogVerified().process_receipt(tx_receipt)
                if event_logs:
                    print(f"Log verified: logId={event_logs[0]['args']['logId'].hex()}")
                else:
                    print("LogVerified event not found in receipt")
            except Exception as event_error:
                print(f"Warning: Could not process LogVerified event: {event_error}")

            return True

        except Exception as e:
            print(f"Failed to verify log: {e}")
            return False

    def get_log(self, log_id):
        """
        Retrieve full log metadata by ID.

        Args:
            log_id (bytes): The 32-byte log ID.

        Returns:
            tuple: (success, dict) where dict has keys:
                   sender, receiver, sk1x, sk1y, timelock, message_hash, verified.
        """
        try:
            result = self.contract.functions.getLog(log_id).call()
            return True, {
                'sender':       result[0],
                'receiver':     result[1],
                'sk1x':         result[2],
                'sk1y':         result[3],
                'timelock':     result[4],
                'message_hash': result[5],
                'verified':     result[6],
            }
        except Exception as e:
            print(f"Failed to get log {log_id}: {e}")
            return False, {}

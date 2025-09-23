import os
from web3 import Web3
from dotenv import load_dotenv

class Web3dbContract:
    def __init__(self, contract_address=None, infura_api_key=None, private_key=None):
        self.infura_api_key = infura_api_key
        self.private_key = private_key
        self.contract_address = contract_address
        print(f"INFURA_API_KEY: {'Present' if self.infura_api_key else 'Missing'}")
        print(f"PRIVATE_KEY: {'Present' if self.private_key else 'Missing'}")
        print(f"CONTRACT_ADDRESS: {'Present' if self.contract_address else 'Missing'}")

        # Connect to Sepolia network
        try:
            self.w3 = Web3(Web3.HTTPProvider(f"https://sepolia.infura.io/v3/{self.infura_api_key}"))
        except Exception as e:
            raise Exception(f"Failed to connect to Sepolia network: {e}")
        print(f"Connected to Sepolia network: {self.w3.is_connected()}")
        
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
                    {
                        "indexed": True,
                        "internalType": "address",
                        "name": "walletAddress",
                        "type": "address"
                    },
                    {
                        "indexed": False,
                        "internalType": "string",
                        "name": "tableName",
                        "type": "string"
                    },
                    {
                        "indexed": False,
                        "internalType": "string",
                        "name": "policySql",
                        "type": "string"
                    }
                ],
                "name": "AccessPolicyAdded",
                "type": "event"
            },
            {
                "anonymous": False,
                "inputs": [
                    {
                        "indexed": True,
                        "internalType": "address",
                        "name": "walletAddress",
                        "type": "address"
                    },
                    {
                        "indexed": False,
                        "internalType": "string",
                        "name": "tableName",
                        "type": "string"
                    }
                ],
                "name": "AccessPolicyRemoved",
                "type": "event"
            },
            {
                "inputs": [
                    {
                        "internalType": "address",
                        "name": "walletAddress",
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
                        "name": "walletAddress",
                        "type": "address"
                    }
                ],
                "name": "getAccessPolicies",
                "outputs": [
                    {
                        "components": [
                            {
                                "internalType": "address",
                                "name": "ownerAddress",
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
                        "name": "walletAddress",
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
                        "name": "walletAddress",
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
                        "name": "walletAddress",
                        "type": "address"
                    }
                ],
                "name": "removeAllAccessPolicies",
                "outputs": [],
                "stateMutability": "nonpayable",
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
    
    def add_access_policy(self, wallet_address, table_name, policy_sql):
        """
        Add an access policy for a wallet address
        
        Args:
            wallet_address (str): The wallet address (e.g., "0x123...")
            table_name (str): The table name
            policy_sql (str): The SQL policy string
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Convert string address to checksum address
            wallet_address = Web3.to_checksum_address(wallet_address)
            
            # Build transaction
            nonce = self.w3.eth.get_transaction_count(self.address)
            
            tx = self.contract.functions.addAccessPolicy(
                wallet_address,
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
                    'ownerAddress': policy[0],
                    'tableName': policy[1],
                    'policySql': policy[2]
                }
                policy_list.append(policy_dict)
                
            return True, policy_list
        except Exception as e:
            print(f"Failed to get access policies for {wallet_address}: {e}")
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


if __name__ == "__main__":
    try:
        # Initialize the Web3dbContract instance
        index_storage = Web3dbContract(
            contract_address="0x041da68BD3F1bf13C5d75E3bA80ab6bB8B136BFd",
            infura_api_key="eb1d43f1429e49fba50e18fbf5ebd4ab",
            private_key="34cf59aaa5ef0a24e65b4e4dbe6fb23c2bd23a4d9a6b584d7995a141de719d53"
        )
        
        # Test single operations first
        # print("\n--- Testing Single Operations ---")
        # attribute = "PatientID"
        # success, current_cid = index_storage.get_index(attribute)
        # print(f"Current {attribute} CID: {current_cid if success else 'Error retrieving'}")
        
        # Update a single index
        # single_new_cid = "QmSingleTestCID123456789abcdefghijklmnopqrstuvwxyz"
        # print(f"\nUpdating {attribute} index to: {single_new_cid}")
        # success = index_storage.update_index(attribute, single_new_cid)
        
        # if success:
        #     # Verify the update
        #     success, updated_cid = index_storage.get_index(attribute)
        #     print(f"Updated {attribute} Index CID: {updated_cid if success else 'Error retrieving'}")
        
        # Test batch operations
        print("\n--- Testing Batch Operations ---")
        attributes = ["PatientID", "HospitalID", "Age"]

        # Test batch update
        new_cids = [
            "QmBatchTest1CIDPatientID123456789abcdefghijklmnopqrst",
            "QmBatchTest2CIDHospitalID123456789abcdefghijklmnopqrs",
            "QmBatchTest3CIDAge123456789abcdefghijklmnopqrstuvwxyz"
        ]
        
        print("\nTesting batch update_indices...")
        print(f"Updating {len(attributes)} indices in a single transaction")
        for i, (attr, cid) in enumerate(zip(attributes, new_cids)):
            print(f"  {attr} -> {cid}")
        
        success = index_storage.batch_update_indices(attributes, new_cids)
        
        if success:
            # Verify the batch update
            print("\nVerifying batch update...")
            success, updated_cids = index_storage.batch_get_indices(attributes)
            if success:
                print("Updated CIDs:")
                for attr, cid in updated_cids.items():
                    print(f"  {attr}: {cid}")
            else:
                print("Failed to retrieve updated batch indices")
        
        # Test access policy management
        print("\n--- Testing Access Policy Management ---")
        test_wallet = "0x68ef100cC9dAdE0bb67a0aE99A02CDd1eaE54A2f"
        test_table = "patient_data"
        test_policy = "SELECT * FROM patient_data WHERE PatientID = '38'"
        
        # Add an access policy
        print(f"Adding access policy for wallet: {test_wallet}")
        print(f"Table: {test_table}")
        print(f"Policy: {test_policy}")
        
        success = index_storage.add_access_policy(test_wallet, test_table, test_policy)
        if success:
            print("Access policy added successfully!")
            
            # Get policy count
            success, count = index_storage.get_policy_count(test_wallet)
            if success:
                print(f"Policy count for wallet: {count}")
            
            # Get all policies
            success, policies = index_storage.get_access_policies(test_wallet)
            if success:
                print("Retrieved access policies:")
                for i, policy in enumerate(policies):
                    print(f"  Policy {i}:")
                    print(f"    Owner: {policy['ownerAddress']}")
                    print(f"    Table: {policy['tableName']}")
                    print(f"    SQL: {policy['policySql']}")
            else:
                print("Failed to retrieve access policies")
                
        else:
            print("Failed to add access policy")
        
        # Optional: test remove index
        # print("\n--- Testing Remove Index ---")
        # test_attr = "TestAttribute"
        # print(f"Adding and then removing test attribute: {test_attr}")
        # 
        # # First add it
        # test_cid = "QmTestCIDForRemovalTest123456789abcdefghijklmnopqr"
        # success = index_storage.update_index(test_attr, test_cid)
        # if success:
        #     # Verify it was added
        #     success, added_cid = index_storage.get_index(test_attr)
        #     print(f"Added test attribute CID: {added_cid if success else 'Error retrieving'}")
        #     
        #     # Now remove it
        #     success = index_storage.remove_index(test_attr)
        #     print(f"Removed test attribute: {'Success' if success else 'Failed'}")
        #     
        #     # Verify it was removed
        #     success, removed_cid = index_storage.get_index(test_attr)
        #     print(f"After removal CID (should be empty): '{removed_cid if success else 'Error retrieving'}'")
        
    except Exception as e:
        print(f"Error in test: {e}")
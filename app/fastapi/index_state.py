import os
from web3 import Web3
from dotenv import load_dotenv

class IndexState:
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
        
        # Updated Contract ABI with batch operations
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
            }
        ]
        
        # Create contract instance
        self.contract = self.w3.eth.contract(address=self.contract_address, abi=self.abi)
        
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
                'gasPrice': self.w3.eth.gas_price,
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
                'gasPrice': self.w3.eth.gas_price,
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
                'gasPrice': self.w3.eth.gas_price,
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
    


if __name__ == "__main__":
    try:
        # Initialize the IndexState instance
        index_storage = IndexState(
            contract_address="0xe4B4B17AA1Fe9f90fA1521ed87FfcC0f85452F91",
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
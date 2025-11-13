import os, sys
from web3 import Web3
from dotenv import load_dotenv
import json

class Web3dbContract:
    def __init__(self, contract_address=None, infura_api_key=None, private_key=None, abi_path="../../contracts/artifacts/contracts/Web3dbContract.sol/Web3dbContract.json", network_url="http://localhost:8545"):
        self.infura_api_key = infura_api_key
        self.private_key = private_key
        self.contract_address = contract_address
        print(f"INFURA_API_KEY: {'Present' if self.infura_api_key else 'Missing'}")
        print(f"PRIVATE_KEY: {'Present' if self.private_key else 'Missing'}")
        print(f"CONTRACT_ADDRESS: {'Present' if self.contract_address else 'Missing'}")
        print(f"ABI_PATH: {'Present' if abi_path else 'Missing'}")
        # Connect to Sepolia network
        try:
            # self.w3 = Web3(Web3.HTTPProvider(f"http://localhost:8545"))
            self.w3 = Web3(Web3.HTTPProvider(network_url))
            # self.w3 = Web3(Web3.HTTPProvider(f"https://sepolia.infura.io/v3/{self.infura_api_key}"))
        except Exception as e:
            raise Exception(f"Failed to connect to Sepolia network: {e}")
        print(f"Connected to Sepolia network: {self.w3.is_connected()}")
        
        # Set up account
        self.account = self.w3.eth.account.from_key(self.private_key)
        self.address = self.account.address
        print(f"Connected with address: {self.address}")
        
        # Updated Contract ABI with batch operations and schema management
        with open(abi_path, 'r') as f:
            self.abi = json.load(f)['abi']

        
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
        # try:
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
            print(f"Index updated: attribute={logs[0]['args']['attribute']}", 
                  f"newCID={logs[0]['args']['newCID']}")
            return True
        else:
            print("Event not found in transaction receipt")
            return False
        
        # except Exception as e:
        #     print(f"Failed to update index: {e}")
        #     return False
    
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
                # update_logs = self.contract.events.IndexUpdated().process_receipt(tx_receipt)
                # if update_logs:
                #     for log in update_logs:
                #         print(f"Index updated: attribute={log['args']['attribute']}, "
                #               f"newCID={log['args']['newCID']}")
                
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
                print(f"Index removed: attribute={logs[0]['args']['attribute']}")
                return True
            else:
                print("Event not found in transaction receipt")
                return False
                
        except Exception as e:
            print(f"Failed to remove index: {e}")
            return False
     

def test_batch_get(index_storage):
    print("\nTesting batch get_indices...")
    attributes = ["PatientID", "HospitalID", "Age"]
    success, updated_cids = index_storage.batch_get_indices(attributes)
    if success:
        print("Updated CIDs:")
        for attr, cids in updated_cids.items():
            print(f"{attr}: {cids}\n")
    else:
        print("Failed to retrieve updated batch indices")

if __name__ == "__main__":
    # try:
    # Initialize the Web3dbContract instance
    index_storage = Web3dbContract(
        contract_address="0x5FbDB2315678afecb367f032d93F642f64180aa3",
        infura_api_key="eb1d43f1429e49fba50e18fbf5ebd4ab",
        private_key="0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
    )
    test_batch_get(index_storage)
    
    sys.exit(0)
    # Test single operations first
    print("\n--- Testing Single Operations ---")
    print("\nTesting update_index...")
    attribute = "PatientID"
    success, current_cid = index_storage.get_index(attribute)
    print(f"Current {attribute} CID: {current_cid if success else 'Error retrieving'}")
    
    # Update a single index
    single_new_cid = "test1"
    print(f"Updating {attribute} index to: {single_new_cid}")
    success = index_storage.update_index(attribute, single_new_cid)
    
    if success:
        # Verify the update
        print("\nTesting get_index...")
        success, updated_cid = index_storage.get_index(attribute)
        print(f"Updated {attribute} Index CID: {updated_cid if success else 'Error retrieving'}")
        
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
        print("\nTesting batch get_indices...")
        success, updated_cids = index_storage.batch_get_indices(attributes)
        if success:
            print("Updated CIDs:")
            for attr, cids in updated_cids.items():
                print(f"{attr}: {cids}\n")
        else:
            print("Failed to retrieve updated batch indices")

    print("\n--- Testing Remove ---")

    attributes = ["PatientID", "HospitalID", "Age"]
    for attr in attributes:
        success = index_storage.remove_index(attr)
    
    if success:
        # Verify the batch update
        print("\nTesting batch get_indices...")
        success, updated_cids = index_storage.batch_get_indices(attributes)
        if success:
            print("Updated CIDs:")
            for attr, cids in updated_cids.items():
                print(f"{attr}: {cids}\n")
        else:
            print("Failed to retrieve updated batch indices")

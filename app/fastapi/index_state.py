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
        
        # Updated Contract ABI (generated from your IndexState.sol contract)
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
            }
        ]
        
        # Create contract instance
        self.contract = self.w3.eth.contract(address=self.contract_address, abi=self.abi)
        
    def update_index(self, attribute, new_cid):
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
                return True, "success"
            else:
                return False, "Event not found in transaction receipt"
        
        except Exception as e:
            print(f"Failed to update index: {e}")
            return False, str(e)
    
    def get_index(self, attribute):
        try:
            # Call the smart contract function
            current_cid = self.contract.functions.getIndexCID(attribute).call()
            return True, current_cid
        except Exception as e:
            print(f"Error retrieving index for {attribute}: {e}")
            return False, str(e)
    


# if __name__ == "__main__":
#     try:
#         index_storage = IndexState(
#     contract_address="0xb4f39C949FFC18ca9AD4449fc0f7c328f646d87C",
#     infura_api_key="eb1d43f1429e49fba50e18fbf5ebd4ab",
#     private_key="34cf59aaa5ef0a24e65b4e4dbe6fb23c2bd23a4d9a6b584d7995a141de719d53"
# )
#         attribute = "PatientID"
#         success, current_cid = index_storage.get_index(attribute)
#         print(success, current_cid == "")
        # Update index
        # new_cid = "QmXg9Pp2ytZ14xgmQjYEiHjVjMFXzCVVEcRTWJBmLgR39V"
        # print(f"Updating {attribute} index to: {new_cid}")
        # index_storage.update_index(attribute, new_cid)
        
        # Get updated index
        # success, updated_cid = index_storage.get_index(attribute)
        # if success:
        #     print(f"Updated {attribute} Index CID: {updated_cid}")
            
    # except Exception as e:
    #     print(f"Error: {e}")
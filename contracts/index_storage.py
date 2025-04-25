import os
from web3 import Web3
from dotenv import load_dotenv

class IndexStorage:
    def __init__(self, contract_address=None, infura_api_key=None, private_key=None):
        
        # Load environment variables from .env in the same directory
        load_dotenv(".env")
        
        self.infura_api_key = infura_api_key or os.getenv("INFURA_API_KEY")
        self.private_key = private_key or os.getenv("PRIVATE_KEY")
        self.contract_address = contract_address or os.getenv("CONTRACT_ADDRESS")
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
        
        # Contract ABI (generated from your IndexStorage.sol contract)
        self.abi = [
            {
                "anonymous": False,
                "inputs": [
                    {
                        "indexed": False,
                        "internalType": "string",
                        "name": "previousCID",
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
                "inputs": [],
                "name": "getCurrentIndex",
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
                        "name": "_newCID",
                        "type": "string"
                    }
                ],
                "name": "updateCurrentIndex",
                "outputs": [],
                "stateMutability": "nonpayable",
                "type": "function"
            }
        ]
        
        # Create contract instance
        self.contract = self.w3.eth.contract(address=self.contract_address, abi=self.abi)
        
    def update_current_index(self, new_cid):
        try:
            # Build transaction
            nonce = self.w3.eth.get_transaction_count(self.address)
            
            tx = self.contract.functions.updateCurrentIndex(
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
                print(f"Index updated: {logs[0]['args']}")
                return True, "success"
            else:
                return False, "failed"
        
        except Exception as e:
            print(f"Failed to update index: {e}")
            return False, str(e)
    
    def get_current_index(self):
        try:
            # Call the smart contract function
            current_cid = self.contract.functions.getCurrentIndex().call()
            return True, current_cid
        except Exception as e:
            print(f"Error retrieving current index: {e}")
            return False, str(e)


if __name__ == "__main__":
    try:
        index_storage = IndexStorage()
        
        # Get current index
        success, current_cid = index_storage.get_current_index()
        if success:
            print(f"Current Index CID: {current_cid}")
        
        # Update index
        new_cid = "QmXg9Pp2ytZ14xgmQjYEiHjVjMFXzCVVEcRTWJBmLgR39V"
        print(f"Updating index to: {new_cid}")
        index_storage.update_current_index(new_cid)
        
        # Get updated index
        success, updated_cid = index_storage.get_current_index()
        if success:
            print(f"Updated Index CID: {updated_cid}")
            
    except Exception as e:
        print(f"Error: {e}")
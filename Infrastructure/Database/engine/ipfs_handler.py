import requests
import datetime
from typing import Dict, Optional, Any
import os
from cryptography.fernet import Fernet

class IPFSHandler:
    # Default encryption key (base64 encoded 32-byte key)
    DEFAULT_KEY = b'YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXoxMjM0NTY='
    
    def __init__(self):
        host = os.getenv('IPFS_HOST', 'localhost')
        port = os.getenv('IPFS_PORT', '5001')
        self.api_url = f'http://{host}:{port}/api/v0'
        self.connected = self._test_connection()
        
        # Initialize encryption
        encryption_key = os.getenv('AES_ENCRYPTION_KEY')
        if not encryption_key:
            encryption_key = self.DEFAULT_KEY
            print("Using default encryption key")
            
        self.cipher_suite = Fernet(encryption_key)
        
        if self.connected:
            print("Connected to IPFS node")
        else:
            print("Warning: Could not connect to IPFS node")

    def _test_connection(self) -> bool:
        try:
            response = requests.post(f'{self.api_url}/id')
            return response.status_code == 200
        except Exception as e:
            print(f"IPFS connection error: {str(e)}")
            return False
            
    def _encrypt(self, data: str) -> bytes:
        """Encrypt string data"""
        return self.cipher_suite.encrypt(data.encode())
        
    def _decrypt(self, encrypted_data: bytes) -> str:
        """Decrypt bytes to string"""
        return self.cipher_suite.decrypt(encrypted_data).decode()

    def save_state(self, sql_dump: str, metadata: Dict[str, Any] = None) -> Optional[Dict[str, Any]]:
        """
        Save a new state and return its metadata
        Args:
            sql_dump: Complete SQL dump of current state
            metadata: Additional metadata including previous_hash if any
        """
        print("\n=== IPFS Save ===")
        print(f"Saving SQL dump: {sql_dump}")
        print(f"With metadata: {metadata}")
        
        if not self.connected:
            print("Warning: IPFS not connected, cannot save")
            return None

        try:
            # Encrypt the SQL dump before saving
            encrypted_data = self._encrypt(sql_dump)
            files = {'file': ('filename', encrypted_data)}
            response = requests.post(f'{self.api_url}/add', files=files)
            print(f"IPFS response: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                print(f"IPFS result: {result}")
                return {
                    'hash': result['Hash'],
                    'timestamp': datetime.datetime.now().isoformat(),
                    'table_mappings': metadata.get('table_mappings', {}),
                    'previous_hash': metadata.get('previous_hash')
                }
            else:
                print(f"Failed to save to IPFS: {response.status_code}")
                return None

        except Exception as e:
            print(f"Error saving to IPFS: {str(e)}")
            return None

    def load_state(self, hash: str) -> Optional[Dict[str, Any]]:
        """
        Load content and metadata from IPFS using hash
        Returns dict with 'content' and 'metadata' keys
        """
        if not self.connected:
            print("Warning: IPFS not connected, cannot load")
            return None

        try:
            # Get encrypted SQL content
            response = requests.post(
                f'{self.api_url}/cat',
                params={'arg': hash}
            )
            
            if response.status_code != 200:
                print(f"Failed to load from IPFS: {response.status_code}")
                return None

            # Decrypt the content
            decrypted_content = self._decrypt(response.content)

            # Get metadata if exists
            try:
                meta_response = requests.post(
                    f'{self.api_url}/object/get',
                    params={'arg': hash}
                )
                metadata = meta_response.json() if meta_response.status_code == 200 else {}
            except:
                metadata = {}

            return {
                'content': decrypted_content,
                'hash': hash,
                'table_mappings': metadata.get('table_mappings', {}),
                'previous_hash': metadata.get('previous_hash')
            }

        except Exception as e:
            print(f"Error loading from IPFS: {str(e)}")
            return None
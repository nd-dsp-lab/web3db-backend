#fastapi/bplustree.py
from math import inf
import bplustree
import trie
import io
import pickle
import numpy as np

# A universal index interface for both bplustree and trie

class CIDIndex:
    def __init__(self, data=None, block_size=10):
        """
        Initialize the CIDIndex. Automatically determines the index type based on the data type.
        
        Args:
            data (list): A list of tuples (key, value) to build the index.
            block_size (int): Block size for BPlusTree (only used for integer keys).
        """
        self.index = None
        self.index_type = None
        if data:
            # key_type = type(data[0][0])
            # print(f"Key type: {key_type}", data[0:5])
            if isinstance(data[0][0], (int, np.integer)):
                self.index = bplustree.buildIndex(data, block_size)
                self.index_type = "bplustree"
            elif isinstance(data[0][0], str):
                self.index = trie.buildIndex(data)
                self.index_type = "trie"
            else:
                raise TypeError("Unsupported key type. Only int and str are supported.")

    def update(self, data):
        """
        Update the index with new data.
         
        Args:
            data (list): A list of tuples (key, value) to update the index.
        """
        if not self.index:
            raise ValueError("Index is not initialized. Provide data during initialization.")
        
        if self.index_type == "bplustree":
            self.index = bplustree.updateIndex(self.index, data)
        elif self.index_type == "trie":
            self.index = trie.updateIndex(self.index, data)

    def query(self, key):
        """
        Perform a point query on the index.
        
        Args:
            key: The key to query.
        
        Returns:
            list: A list of values matching the key.
        """
        if not self.index:
            raise ValueError("Index is not initialized.")
        
        if self.index_type == "bplustree":
            return bplustree.query(self.index, key)
        elif self.index_type == "trie":
            return trie.query(self.index, key)

    def query_range(self, start_key=-inf, end_key=inf):
        """
        Perform a range query on the index.
        
        Args:
            start_key: The start key of the range.
            end_key: The end key of the range.
        
        Returns:
            list: A list of values matching the range.
        """
        if not self.index:
            raise ValueError("Index is not initialized.")
        
        if self.index_type == "bplustree":
            return bplustree.queryRange(self.index, start_key, end_key)
        elif self.index_type == "trie":
            return trie.queryRange(self.index, start_key, end_key)

    def dump(self):
        """
        Serialize the index to a binary stream.
        
        Returns:
            io.BytesIO: A binary stream containing the serialized index.
        """
        if not self.index:
            raise ValueError("Index is not initialized.")
        
        if self.index_type == "bplustree":
            return bplustree.dumpIndex(self.index)
        elif self.index_type == "trie":
            return trie.dumpIndex(self.index)

    def load(self, stream):
        """
        Load the index from a binary stream.
        
        Args:
            stream (io.BytesIO): A binary stream containing the serialized index.
        """
        if not stream:
            raise ValueError("Stream is empty.")
        
        try:
            index = pickle.load(stream)
            if isinstance(index, bplustree.BPlusTree):
                self.index = index
                self.index_type = "bplustree"
            elif isinstance(index, trie.CharTrie):
                self.index = index
                self.index_type = "trie"
            else:
                raise TypeError("Unsupported index type in the stream.")
        except Exception as e:
            raise ValueError(f"Failed to load index: {str(e)}")
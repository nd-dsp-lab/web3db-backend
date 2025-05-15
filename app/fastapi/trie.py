#fastapi/trie.py
from pygtrie import CharTrie
import pickle
import io

# build index based on input, -> index 
def buildIndex(data = []) -> CharTrie:
    index = CharTrie()
    if data:
        index = updateIndex(index, data)
    return index

# update index -> index
def updateIndex(index: CharTrie, data) -> CharTrie:
    for key, value in data:
        if key not in index:
            index[key] = [value]
        else:
            index[key].append(value)
    return index

# point query -> list of all that match that query
def query(index: CharTrie, key) -> list:
    if key not in index:
        return []
    return index[key]

# range query -> Not sure if this should be like a prefix thing or smth. dunno rlly
def queryRange(index: CharTrie, startKey=None, endKey=None) -> list:
    vals = set()
    for key in index.keys():
        vals.update(index[key])
    return list(vals)
    
    # pass

def dumpIndex(index: CharTrie):
    stream = io.BytesIO()
    pickle.dump(index, stream)
    stream.seek(0)
    return stream
        

def readIndex(stream: io.BytesIO) -> CharTrie:
    return pickle.load(stream)



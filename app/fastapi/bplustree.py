import bisect
import pickle
import io
'''
2024-02-24

Implement a simple function that builds an index over a given set of data, and query function.

Input: [(timestamp, CID), (timestamp, CID), [(timestamp, CID) … ]
CID: 256-bit string
flexible; reuse structure for other attributes
index over timestamp
'''

'''
For reusable structure, need to right a greater than and an equal to function
'''


class BPlusTree:
    class Node:
        def __init__(self, leaf):
            self.leaf = leaf
            self.keys = []
            self.children = [None] if leaf else []
        
        def info(self):
            a = "Node: " + str(id(self))
            b = "Leaf: " + str(self.leaf)
            c = f'keys: {self.keys}'
            d = f'data: {self.children}'
            return f'{a}\n{b}\n{c}\n{d}\n'

    def create_node(self, leaf, keys, data):
        node = self.Node(leaf)
        node.keys = keys
        node.children = data
        return node
    
    def __init__(self, data, BLOCK_SIZE: int, key_type: type, value_type: type):
        self.BLOCK_SIZE = BLOCK_SIZE
        self.root = BPlusTree.Node(leaf=True)
        self.key_type = key_type
        self.value_type = value_type
        for timestamp, cid in data:
            self.insert((timestamp, cid))

    def insert_inorder(self, node: Node, key, child):        
        if node.leaf:
            # In the case of leaf: find where key should be indexed, insert key and child there
            index = bisect.bisect_right(node.keys, key)
            if index > 0 and node.keys[index-1] == key:
                node.children[index-1].append(child)
            else:
                node.keys.insert(index, key)
                node.children.insert(index, [child])
        else:
            # In the case of internal: find where key should be indexed, insert key and add right child after; left child was edited as the node before
            index = bisect.bisect_right(node.keys, key)
            node.keys.insert(index, key)
            node.children.insert(index+1, child)


    def split_node(self, node: Node) -> tuple[Node, Node, int]:
        # turn node into left node, create new right node, split up info. if internal node lift up the middle node to be divider
        keys = node.keys
        children = node.children
        left = node
        left.keys = []
        left.children = []
        n = len(keys)

        if left.leaf:
            right = BPlusTree.Node(leaf=True)
            left.keys = keys[:(n+1)//2]
            right.keys = keys[(n+1)//2:]
            left.children = children[:(n+1)//2]
            left.children.append(right)
            right.children = children[(n+1)//2:]
            return (left, right, keys[(n+1)//2])
        else:
            right = BPlusTree.Node(leaf=False)
            left.keys = keys[:(n+1)//2]
            right.keys = keys[(n+1)//2+1:]
            left.children = children[:(n+1)//2+1]
            right.children = children[(n+1)//2+1:]
            return (left, right, keys[(n+1)//2])

    # TODO: def remove(self, data):
    def remove(self, data):
        
        stack = []
        node = self.root
        while not node.leaf:
            i = 0
            for key in node.keys:
                if data[0] < key:
                    break
                i+=1
            stack.append(node)
            node = node.children[i]
        key, value = data[0], data[1]
        i = 0
        while i < len(node.keys) and key > node.keys[i]:
            i += 1
        if i == len(node.keys):
            return False # no data with that key found
        if node.keys[i] != key:
            return False # no data with that key found
        if value not in node.children[i]:
            return False # no data with that value found
        if len(node.children[i]) == 1:
            node.children = node.children[:i] + node.children[i+1:]
            node.keys = node.keys[:i] + node.keys[i+1:]
        else:
            node.children[i] = list(filter(lambda a: a != value, node.children[i]))
        # TODO: need to remove node if node is empty
        return True

    def insert(self, data):
        if self.key_type == None and self.value_type == None:
            self.key_type, self.value_type = type(data[0]), type(data[1])
        if type(data[0])!= self.key_type:
            raise TypeError(f"Expected {self.key_type} for key and got {type(data[0])}")
        if type(data[1])!= self.value_type:
            raise TypeError(f"Expected {self.value_type} for value and got {type(data[1])}")
        stack = []
        node = self.root

        # search for where it's supposed to go
        while not node.leaf:
            i = 0
            for key in node.keys:
                if data[0] < key:
                    break
                i+=1
            stack.append(node)
            node = node.children[i]

        # insert data in order
        self.insert_inorder(node, data[0], data[1])

        # split node if necessary, all the way back up to the top
        interceptor = None
        if len(node.keys) > self.BLOCK_SIZE:
            left, right, interceptor = self.split_node(node)
        while interceptor != None:
            if not stack:
                node = BPlusTree.Node(False)
                node.keys.append(interceptor)
                node.children.append(left)
                node.children.append(right)
                interceptor = None
                self.root = node
            else:
                node = stack.pop()
                self.insert_inorder(node, interceptor, right)
                interceptor = None
                if len(node.keys) > self.BLOCK_SIZE:
                    left, right, interceptor = self.split_node(node)

    def query(self, query):
        return self.queryRange(query, query)

    def queryRange(self, querystart, queryend):
        node = self.root
        while not node.leaf:
            i = 0
            for key in node.keys:
                if querystart <= key:
                    break
                i+=1
            node = node.children[i]
        i = 0

        # handle edge case of empty tree
        if not node.keys:
            return []
        
        # iterate till past start
        while node and querystart > node.keys[i]:
            i+=1
            if i == len(node.keys):
                node = node.children[-1]
                i = 0
            if not node:
                break

        res = [] 
        # iterate till past end
        while node and queryend >= node.keys[i]:
            res += node.children[i]
            i+=1
            if i == len(node.keys):
                node = node.children[-1]
                i = 0
            if not node:
                break
        return res

    def __str__(self):
        return 'BLOCK_SIZE: ' + str(self.BLOCK_SIZE) + '\n' + BPlusTree.dfs(self.root)
    
    @staticmethod
    def dfs(node: Node, tabs=0) -> str:
        res = ''
        if node.leaf:
            for _ in range(tabs):
                res += '        '
            res += str(node) + '\n'
            for i in range(len(node.keys)):
                for _ in range(tabs+1):
                    res += '        '
                res += str(node.keys[i])
                res += ' '
                res += str(node.children[i])
                res += '\n'
            for _ in range(tabs):
                res += '        '
            res += 'next: '
            res += str(node.children[-1])
            res += '\n'
        else:
            for _ in range(tabs):
                res += '        '
            res+=str(node) +'\n'
            res += BPlusTree.dfs(node.children[0], tabs+1)
            for i in range(len(node.keys)):
                for _ in range(tabs):
                    res += '        '
                res += str(node.keys[i])
                res +='\n'
                res += BPlusTree.dfs(node.children[i+1], tabs+1)
        return res

# build index based on input, -> index 
def buildIndex(data=[], block_size=10) -> BPlusTree:
    if not data:
        return BPlusTree(data, block_size, None, None)
    key_type, value_type = type(data[0][0]), type(data[0][1])
    index = BPlusTree(data, block_size, key_type, value_type)
    return index

# update index -> index
def updateIndex(index: BPlusTree, data) -> BPlusTree:
    for datum in data:
        index.insert(datum)
    return index

# point query -> list of all that match that query
def query(index: BPlusTree, time) -> list:
    return index.query(time)

# range query -> list of all that match query range
def queryRange(index: BPlusTree, startTime, endTime) -> list:
    return index.queryRange(startTime,endTime)

def dumpIndex(index: BPlusTree):
    stream = io.BytesIO()
    pickle.dump(index, stream)
    stream.seek(0)
    return stream
        

def readIndex(stream: io.BytesIO) -> BPlusTree:
    return pickle.load(stream)
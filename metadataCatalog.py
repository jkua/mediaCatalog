import os
import json

class HashDirectoryTree(object):
    def __init__(self, rootPath, hashLength=32, segmentLength=3):
        self.rootPath = rootPath
        self.hashLength = hashLength
        self.segmentLength = segmentLength

    def getPath(self, hash_):
        segments = [hash_[self.segmentLength*n:self.segmentLength*n+self.segmentLength] for n in range(self.hashLength//self.segmentLength)]
        return os.path.join(self.rootPath, *segments)

    def exist(self, hash_):
        path = self.getPath(hash_)
        return os.path.exists(path)


class MetadataCatalog(object):
    def __init__(self, path):
        self.path = path
        self.hashTree = HashDirectoryTree(path, hashLength=32, segmentLength=3)

    def write(self, metadata):
        metadataPath = self.hashTree.getPath(metadata['File:MD5Sum'])
        metadataDirectory, _ = os.path.split(metadataPath)
        if not os.path.exists(metadataDirectory):
            os.makedirs(metadataDirectory)
        with open(metadataPath, 'wt') as f:
            f.write(json.dumps(metadata) + '\n')

    def read(self, hash_):
        metadataPath = self.hashTree.getPath(hash_)
        if not os.path.exists(metadataPath):
            return None
        with open(metadataPath, 'rt') as f:
            metadata = json.loads(f.read())
        return metadata
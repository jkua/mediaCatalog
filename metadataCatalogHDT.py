import os
import json

from metadataCatalog import MetadataCatalog

class HashDirectoryTree(object):
    def __init__(self, rootPath, hashLength=32, segmentLength=2, depth=2):
        self.rootPath = rootPath
        self.hashLength = hashLength
        self.segmentLength = segmentLength
        self.depth = depth

    def getPath(self, hash_):
        segments = [hash_[self.segmentLength*n:self.segmentLength*n+self.segmentLength] for n in range(self.hashLength//self.segmentLength)]
        return os.path.join(self.rootPath, *segments[:self.depth])

    def exist(self, hash_):
        path = self.getPath(hash_)
        return os.path.exists(path)


class MetadataCatalogHDT(MetadataCatalog):
    def __init__(self, path, hashMode):
        super().__init__()

        self.path = path
        self.hashMode = hashMode
        self.hashKey = f'File:{hashMode}Sum'
        self.hashTree = HashDirectoryTree(path, hashLength=32, segmentLength=2, depth=2)

    def write(self, metadata):
        hash_ = metadata[self.hashKey]
        metadataPath = self.getMetadataPath(hash_)
        metadataDirectory, _ = os.path.split(metadataPath)
        if not os.path.exists(metadataDirectory):
            os.makedirs(metadataDirectory)

        # Check for existing file
        if os.path.exists(metadataPath):
            print(f'Metadata exists for this hash! {hash_}')
            existingMetadata = json.loads(open(metadataPath, 'rt').read())
            if existingMetadata['SourceFile'] == metadata['SourceFile']:
                print(f'    Same file: {metadata["SourceFile"]}')
                print(f'        updating metadata...')
            else:
                print(f'    Different filenames! Is this a duplicate!')
                print(f'        Existing: {existingMetadata["SourceFile"]}')
                print(f'             New: {metadata["SourceFile"]}')
                raise Exception('Hash collision! Do not know what to do!')

        with open(metadataPath, 'wt') as f:
            f.write(json.dumps(metadata) + '\n')

        return metadataPath

    def read(self, hash_):
        metadataPath = self.getMetadataPath(hash_)
        if not os.path.exists(metadataPath):
            return None
        with open(metadataPath, 'rt') as f:
            metadata = json.loads(f.read())
        return metadata

    def getMetadataPath(self, hash_):
        return os.path.join(self.hashTree.getPath(hash_), hash_ + '.json')

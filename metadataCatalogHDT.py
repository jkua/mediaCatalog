import os
import json

from metadataCatalog import MetadataCatalog

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


class MetadataCatalogHDT(MetadataCatalog):
    def __init__(self, path):
        super().__init__()

        self.path = path
        self.hashTree = HashDirectoryTree(path, hashLength=32, segmentLength=3)

    def write(self, metadata):
        metadataPath = self.hashTree.getPath(metadata['File:MD5Sum']) + '.json'
        metadataDirectory, _ = os.path.split(metadataPath)
        if not os.path.exists(metadataDirectory):
            os.makedirs(metadataDirectory)

        # Check for existing file
        if os.path.exists(metadataPath):
            print(f'Metadata exists for this hash! {metadata["File:MD5Sum"]}')
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

    def read(self, hash_):
        metadataPath = self.hashTree.getPath(hash_) + '.json'
        if not os.path.exists(metadataPath):
            return None
        with open(metadataPath, 'rt') as f:
            metadata = json.loads(f.read())
        return metadata
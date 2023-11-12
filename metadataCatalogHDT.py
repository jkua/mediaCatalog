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
        return os.path.exists(os.path.join(self.rootPath, path))


class MetadataCatalogHDT(MetadataCatalog):
    def __init__(self, path, hashMode):
        super().__init__()

        self.path = path
        self.hashMode = hashMode
        self.hashKey = f'File:{hashMode}Sum'
        self.hashTree = HashDirectoryTree(path, hashLength=32, segmentLength=2, depth=2)

        if not os.path.exists(self.path):
            raise Exception('Missing metadata folder!')

    def write(self, metadata, updateMode):
        hash_ = metadata[self.hashKey]
        metadataPath = self.getMetadataPath(hash_)
        metadataDirectory, _ = os.path.split(metadataPath)
        if not os.path.exists(metadataDirectory):
            os.makedirs(metadataDirectory)

        # Check for existing file
        write_ = True
        if os.path.exists(metadataPath):
            print(f'Metadata exists for this hash! {hash_}')
            if updateMode:
                existingMetadata = json.loads(open(metadataPath, 'rt').read())
                if existingMetadata['File:FileSize'] == metadata['File:FileSize']:
                    print(f'    Same file: {metadata["SourceFile"]}, Size: {metadata["File:FileSize"]}')
                    print(f'        updating metadata...')
                else:
                    print(f'    Different filesizes!')
                    print(f'        Existing: {existingMetadata["File:FileSize"]}')
                    print(f'             New: {metadata["File:FileSize"]}')
                    raise Exception('Hash collision! Do not know what to do!')
            else:
                print('    Will not update metadata.')
                write_ = False

        if write_:
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

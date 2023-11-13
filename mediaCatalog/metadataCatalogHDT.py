import os
import json
import glob
import logging

from .metadataCatalog import MetadataCatalog

class HashDirectoryTree(object):
    def __init__(self, rootPath, hashLength=32, segmentLength=2, depth=2):
        self.rootPath = rootPath
        self.hashLength = hashLength
        self.segmentLength = segmentLength
        self.depth = depth

    def getPath(self, hash_):
        segments = [hash_[self.segmentLength*n:self.segmentLength*n+self.segmentLength] for n in range(self.hashLength//self.segmentLength)]
        return os.path.join(self.rootPath, *segments[:self.depth])


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

        metadataPath = self.getMetadataPath(hash_, new=True)
        metadataDirectory, _ = os.path.split(metadataPath)
        if not os.path.exists(metadataDirectory):
            os.makedirs(metadataDirectory)

        # Check for existing metadata
        existingMetadata, existingMdPath = self.read(hash_, 
                                                    filename=metadata['File:FileName'],
                                                    directory=metadata['File:Directory'],
                                                    hostname=metadata['HostName'])

        write_ = True
        if existingMetadata:
            print(f'Metadata exists for this file! {hash_}')
            if updateMode:
                print(f'    Updating metadata...')
            else:
                print('    Will not update metadata.')
            
        if write_:
            with open(metadataPath, 'wt') as f:
                f.write(json.dumps(metadata) + '\n')

        return metadataPath

    def read(self, hash_, filename=None, directory=None, hostname=None, all=False):
        if filename is None and directory is None and hostname is None:
            noFilters = False
        else:
            noFilters = True

        metadataAll, pathsAll = self.getAllMetadataForHash(hash_)
        if not metadataAll:
            return None, None

        outputMetadata = []
        for metadata, path in zip(metadataAll, pathsAll):
            if filename is not None and metadata['File:FileName'] != filename:
                continue
            if directory is not None and metadata['File:Directory'] != directory:
                continue
            if hostname is not None and metadata['HostName'] != hostname:
                continue
            outputMetadata.append((metadata, path))

        if not outputMetadata:
            return None, None
            
        if all:
            return outputMetadata

        if len(outputMetadata) > 1:
            if noFilters:
                logging.warning(f'Multiple ({len(outputMetadata)}) metadata entries found for hash! Set filters to narrow down results.')
            else:
                logging.warning(f'Multiple ({len(outputMetadata)}) metadata entries found for criteria! Returning first')

        return outputMetadata[0]

    def getAllMetadataForHash(self, hash_):
        basePath = os.path.join(self.hashTree.getPath(hash_), hash_)
        paths = glob.glob(basePath + '*.json')
        metadata = [json.loads(open(p, 'rt').read()) for p in paths]
        return metadata, paths

    def exists(self, hash_):
        path = self.hashTree.getPath(hash_)
        paths = glob.glob(path + '*')
        return len(paths)

    def getMetadataPath(self, hash_, new=True):
        basePath = os.path.join(self.hashTree.getPath(hash_), hash_)
        existingPaths = sorted(glob.glob(basePath + '*.json'))
        if not new or not existingPaths:
            return basePath + '.json'
        
        head, ext = os.path.splitext(existingPaths[-1])
        directory, baseName = os.path.split(head)
        if len(baseName) == len(hash_):
            lastIndex = 0
        else:
            lastIndex = int(baseName.split('-')[-1])

        return os.path.join(directory, f'{baseName}-{lastIndex+1:02d}.json')

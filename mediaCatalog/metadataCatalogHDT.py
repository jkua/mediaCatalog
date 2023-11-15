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
    VALID_HASH_MODES = ['SHA256', 'MD5']
    def __init__(self, path, hashMode, createPath=False):
        super().__init__()

        if not os.path.exists(path):
            if createPath:
                os.makedirs(path)
            else:
                raise ValueError('Missing metadata folder!')
        self.path = path
        if hashMode not in self.VALID_HASH_MODES:
            raise ValueError(f'Invalid hash mode: {hashMode}! Must be one of {self.VALID_HASH_MODES}')
        self.hashMode = hashMode
        self.hashKey = f'File:{hashMode}Sum'
        self.filenameKey = 'File:FileName'
        self.directoryKey = 'File:Directory'
        self.hostnameKey = 'HostName'
        self.hashTree = HashDirectoryTree(path, hashLength=32, segmentLength=2, depth=2)

    def write(self, metadata, updateMode=False):
        hash_ = metadata[self.hashKey]      

        metadataPath = self.getMetadataPath(hash_, new=True)
        metadataDirectory, _ = os.path.split(metadataPath)
        if not os.path.exists(metadataDirectory):
            os.makedirs(metadataDirectory)

        # Check for existing metadata
        existingMetadata, existingMetadataPath = self.read(hash_, 
                                                        filename=metadata[self.filenameKey],
                                                        directory=metadata[self.directoryKey],
                                                        hostname=metadata[self.hostnameKey])

        if existingMetadata:
            if updateMode:
                metadataPath = existingMetadataPath
                logging.info(f'Updating metadata at: {metadataPath}')
            else:
                logging.warning(f'Metadata exists for this file! {hash_} Will not update - set updateMode to update.')
                return None            

        with open(metadataPath, 'wt') as f:
            f.write(json.dumps(metadata) + '\n')

        return metadataPath

    def read(self, hash_, filename=None, directory=None, hostname=None, all=False):
        directory = os.path.normpath(directory) if directory else None
        if filename is None and directory is None and hostname is None:
            noFilters = False
        else:
            noFilters = True

        metadataAll, pathsAll = self.getAllMetadataForHash(hash_)
        
        outputMetadata = []
        for metadata, path in zip(metadataAll, pathsAll):
            if filename is not None and metadata[self.filenameKey] != filename:
                continue
            if directory is not None and metadata[self.directoryKey] != directory:
                continue
            if hostname is not None and metadata[self.hostnameKey] != hostname:
                continue
            outputMetadata.append((metadata, path))

        if not outputMetadata and not all:
            return None, None
            
        if all:
            return outputMetadata

        if len(outputMetadata) > 1:
            if noFilters:
                logging.warning(f'Multiple ({len(outputMetadata)}) metadata entries found for hash! Set filters to narrow down results.')
            else:
                logging.warning(f'Multiple ({len(outputMetadata)}) metadata entries found for criteria! Returning first')

        return outputMetadata[0]

    def delete(self, hash_, filename=None, directory=None, hostname=None, all=False):
        ''' Deletes metadata for a file. If all is True, all metadata matching the query is deleted.

            :param hash_: (str) The hash of the file to delete metadata for
            :param filename: (str) The filename of the file to delete metadata for
            :param directory: (str) The directory of the file to delete metadata for
            :param hostname: (str) The hostname of the file to delete metadata for
            :param all: (bool) If True, all metadata matching the query is deleted
                               If False, metadata is only deleted if there is a single match
            :returns: (int) number of metadata entries deleted
        '''
        output = self.read(hash_, filename=filename, directory=directory, hostname=hostname, all=True)
        if not all and len(output) > 1:
            raise Exception('Multiple metadata entries found for query! Set all=True to delete all.')
        
        for metadata, path in output:
            os.remove(path)
            for i in self.hashTree.depth:
                path = os.path.split(path)[0]
                if os.path.abspath(path) == os.path.abspath(self.hashTree.rootPath):
                    raise Exception('Cannot delete root path!')
                try:
                    os.rmdir(path)
                except OSError:
                    break

        return len(output)

    def getAllMetadataForHash(self, hash_):
        basePath = os.path.join(self.hashTree.getPath(hash_), hash_)
        paths = glob.glob(basePath + '*.json')
        metadata = [json.loads(open(p, 'rt').read()) for p in paths]
        return metadata, paths

    def exists(self, hash_, filename=None, directory=None, hostname=None):
        hashExists = self._existsHash(hash_)
        noFilters = filename is None and directory is None and hostname is None
        if noFilters or not hashExists:
            return hashExists
        
        metadataAll, pathsAll = self.getAllMetadataForHash(hash_)
        if not metadataAll:
            raise Exception('Hash exists but no metadata found!')

        count = 0
        for metadata, path in zip(metadataAll, pathsAll):
            if filename is not None and metadata[self.filenameKey] != filename:
                continue
            if directory is not None and metadata[self.directoryKey] != directory:
                continue
            if hostname is not None and metadata[self.hostnameKey] != hostname:
                continue
            count += 1

        return count


    def _existsHash(self, hash_):
        basePath = os.path.join(self.hashTree.getPath(hash_), hash_)        
        paths = glob.glob(basePath + '*.json')
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

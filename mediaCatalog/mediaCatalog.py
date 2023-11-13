import os
import logging
import socket
import yaml

import jsonlines

from .metadataCatalogHDT import MetadataCatalogHDT
from .catalogDatabase import CatalogDatabase
from .utils import md5sum, sha256sum, getMimeTypes, getMetadata, getAcoustid


class MediaCatalog(object):
    CONFIG_FILENAME = 'config.yaml'
    CATALOG_DB_FILENAME = 'catalog.db'
    METADATA_FOLDERNAME = 'metadata'
    HASH_TABLE_FILENAME = 'hashTable.jsonl'
    MEDIA_MIME_TYPES = ['image', 'video', 'audio', 'text']
    CHECKSUM_MODE = 'SHA256'

    def __init__(self, catalogPath, create=False, update=False, verbose=False):
        self.catalogPath = catalogPath
        self.createMode = create
        self.updateMode = update
        self.verbose = verbose
        self.hashFilePath = os.path.join(self.catalogPath, self.HASH_TABLE_FILENAME)
        self.configPath = os.path.join(self.catalogPath, self.CONFIG_FILENAME)
        self.catalogDbPath = os.path.join(self.catalogPath, self.CATALOG_DB_FILENAME)
        self.metadataCatalogPath = os.path.join(self.catalogPath, self.METADATA_FOLDERNAME)
        self.hashChunkSize = 1024*1024*16
        self.hashDict = {}
        self.config = None
        self.catalogDb = None
        self.metadataCatalog = None
        self.checksumKey = f'File:{self.CHECKSUM_MODE}Sum'

        self.open()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
        
    def open(self):
        print(f'Opening catalog at {self.catalogPath}')
        if self.createMode:
            self._createCatalog()
            self.createMode = False
        elif not os.path.exists(self.catalogPath):
            raise RuntimeError(f'No catalog at {self.catalogPath}! May create with -n!')
        else:
            self._loadCatalog()

    def close(self):
        self.catalogDb.commit()
        self.catalogDb.close()
        self.catalogDb = None

    def _loadConfig(self):
        try:
            with open(self.configPath, 'r') as f:
                self.config = yaml.safe_load(f)
        except FileNotFoundError as e:
            raise FileNotFoundError('Could not load config file at: {self.configPath}! Broken catalog!')

    def _createConfig(self):
        self.config = {'project': '', 'defaultBucket': ''}
        with open(self.configPath, 'w') as f:
            yaml.dump(self.config, f)

    def _createCatalog(self):
        # Create the catalog folder
        try:
            os.mkdir(self.catalogPath)
        except FileExistsError as e:
            raise FileExistsError(f'Catalog already exists at {self.catalogPath}!')

        self._createConfig()

        # Create the metadata folder - subfolders are two character segments of the md5sum
        os.mkdir(self.metadataCatalogPath)
        self.catalogDb = CatalogDatabase(self.catalogDbPath)
        self.metadataCatalog = MetadataCatalogHDT(self.metadataCatalogPath, self.CHECKSUM_MODE)

    def _loadCatalog(self):
        self._loadConfig()

        self.catalogDb = CatalogDatabase(self.catalogDbPath)
        self.metadataCatalog = MetadataCatalogHDT(self.metadataCatalogPath, self.CHECKSUM_MODE)

    def _loadHashTable(self):
        with jsonlines.open(self.hashFilePath) as reader:
            for obj in reader:
                self.hashDict[obj['hash']] = obj

    def catalog(self, path):
        newFiles = []
        updatedFiles = []
        skippedFiles = []
        failedFiles = []
        hostname = socket.gethostname()
        for dirName, subdirList, fileList in os.walk(path):
            subdirList.sort()
            print(f'\nProcessing directory: {dirName}')
            if not fileList:
                continue
            filesToProcess = []
            fullFilePaths = [os.path.join(dirName, fname) for fname in fileList]
            mimeTypes = getMimeTypes(fullFilePaths)
            for filePath, mimeType in sorted(zip(fullFilePaths, mimeTypes)):
                if mimeType and mimeType.split('/')[0] in self.MEDIA_MIME_TYPES:
                    checksum = self._checksum(filePath, self.CHECKSUM_MODE)
                    # TODO Extend the database check to also look for same capture device and filename/time in case of corruption
                    if self.updateMode or not self.catalogDb.exists(checksum):
                        filesToProcess.append((filePath, checksum))
                    else:
                        skippedFiles.append((filePath, mimeType, checksum))
                        logging.warning(f'File already in catalog! Skipping! {filePath}, Hash: {checksum}')
                else:
                    skippedFiles.append((filePath, mimeType, None))
                    logging.warning(f'Skipping non-media/corrupt file: {filePath} ({mimeType})')

            if not filesToProcess:
                continue

            files, checksums = zip(*filesToProcess)
            if len(filesToProcess) > 0:
                metadata = self._getMetadata(files)        

                for file, md, checksum in zip(files, metadata, checksums):
                    if md is None:
                        failedFiles.append((file, checksum))
                        continue
                    try:
                        md['HostName'] = hostname
                    except:
                        import pdb; pdb.set_trace()
                    
                    md[self.checksumKey] = checksum
                    print(f'{file} -> {checksum}')

                    if self.catalogDb.exists(checksum):
                        updatedFiles.append((file, checksum))
                        logging.info('File already in catalog! Updating!')
                    else:
                        newFiles.append((file, checksum))

                    if md['File:MIMEType'].startswith('audio'):
                        logging.warning('Audio fingerprinting disabled!')
                        # md['Acoustid:MatchResults'] = getAcoustid(file)

                    metadataPath = self.metadataCatalog.write(md, self.updateMode)
                    self.catalogDb.write(md, self.updateMode)

                    # Readback test - TODO Move this to a test case
                    md_readback, _ = self.metadataCatalog.read(md[self.checksumKey], 
                                                    filename=md['File:FileName'],
                                                    directory=md['File:Directory'],
                                                    hostname=md['HostName'])
                    assert self._compareMetadata(md, md_readback)

                    if self.verbose:
                        self.catalogDb.printFileRecord(md[self.checksumKey])

            self.catalogDb.commit()

        numProcessedFiles = len(newFiles) + len(updatedFiles) + len(skippedFiles) + len(failedFiles)
        print(f'\nCataloging complete!')
        print('====================')
        print(f'Files processed: {numProcessedFiles}')
        print(f'New files: {len(newFiles)}')
        print(f'Updated files: {len(updatedFiles)}')
        print(f'Skipped files: {len(skippedFiles)}')
        for file, mimeType, checksum in skippedFiles:
            if checksum:
                print(f'    {file} -> already in catalog: {checksum}')
            elif mimeType:
                print(f'    {file} -> not media - type: {mimeType}')
            else:
                print(f'    {file} -> unable to determine file type')

        print(f'Failed files: {len(failedFiles)}')
        for file, checksum in failedFiles:
            print(f'    {file} -> unable to read metadata!')

    def query(self, checksum=None, filename=None, directory=None, hostname=None):
        dbRecords = self.catalogDb.read(checksum=checksum, filename=filename, directory=directory, hostname=hostname)
        if checksum is not None:
            metadataAndPaths = self.metadataCatalog.read(checksum, filename=filename, directory=directory, hostname=hostname, all=True)
        else:
            metadataAndPaths = []
            for record in dbRecords:
                currentMetadataAndPaths = self.metadataCatalog.read(record['checksum'], filename=record['file_name'], directory=record['directory'], hostname=hostname, all=True)
                metadataAndPaths.extend(currentMetadataAndPaths)
        return dbRecords, metadataAndPaths

    def checksum(self, filename: str) -> str:
        return self._checksum(filename, self.CHECKSUM_MODE)

    def _getMetadata(self, filenames: list) -> list:
        return getMetadata(filenames)
    
    def _compareMetadata(self, metadata1: dict, metadata2: dict) -> bool:
        if len(metadata1.keys()) != len(metadata2.keys()):
            print('Different number of keys!')
            return False
        for key, value in metadata1.items():
            if key == 'File:FileAccessDate':
                continue
            if value != metadata2[key]:
                print(f'Mismatch for [{key}]: ({value} != {metadata2[key]}')
                return False
            
        return True

    def _checksum(self, filename: str, mode: str) -> str:
        if mode == 'MD5':
            return md5sum(filename, self.hashChunkSize)
        elif mode == 'SHA256':
            return sha256sum(filename, self.hashChunkSize)
        else:
            raise ValueError(f'Invalid checksum mode ({mode})!')
        

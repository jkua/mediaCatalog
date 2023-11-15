import os
import logging
import socket
import yaml
from typing import Optional

import jsonlines

from .metadataCatalogHDT import MetadataCatalogHDT
from .catalogDatabase import CatalogDatabase
from .cloudStorage import CloudStorage
from .utils import md5sum, sha256sum, getMimeTypes, getMetadata, getAcoustid


class MediaCatalog(object):
    CONFIG_FILENAME = 'config.yaml'
    CATALOG_DB_FILENAME = 'catalog.db'
    METADATA_FOLDERNAME = 'metadata'
    HASH_TABLE_FILENAME = 'hashTable.jsonl'
    MEDIA_MIME_TYPES = ['image', 'video', 'audio', 'text']
    CHECKSUM_MODE = 'SHA256'
    DEFAULT_CLOUD_OBJECT_PREFIX = 'file'
    HASH_CHUNK_SIZE = 1024*1024*16

    def __init__(self, catalogPath, create=False, update=False, verbose=False):
        self.catalogPath = catalogPath
        self.createMode = create
        self.updateMode = update
        self.verbose = verbose
        self.hashFilePath = os.path.join(self.catalogPath, self.HASH_TABLE_FILENAME)
        self.configPath = os.path.join(self.catalogPath, self.CONFIG_FILENAME)
        self.catalogDbPath = os.path.join(self.catalogPath, self.CATALOG_DB_FILENAME)
        self.metadataCatalogPath = os.path.join(self.catalogPath, self.METADATA_FOLDERNAME)
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
        self.config = {'cloudProject': '', 'defaultCloudBucket': '', 'cloudObjectPrefix': self.DEFAULT_CLOUD_OBJECT_PREFIX}
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
            fullFilePaths = [os.path.abspath(os.path.join(dirName, fname)) for fname in fileList]
            mimeTypes = getMimeTypes(fullFilePaths)
            for filePath, mimeType in sorted(zip(fullFilePaths, mimeTypes)):
                if mimeType and mimeType.split('/')[0] in self.MEDIA_MIME_TYPES:
                    checksum = self._checksum(filePath, self.CHECKSUM_MODE)
                    # TODO Extend the database check to also look for same capture device and filename/time in case of corruption
                    if self.updateMode or not self.catalogDb.existsPath(filename=os.path.basename(filePath), directory=os.path.dirname(filePath)):
                        filesToProcess.append((filePath, checksum))
                    else:
                        skippedFiles.append((filePath, mimeType, checksum))
                        print(f' -> {filePath} -> [SKIP] File already in catalog')
                else:
                    skippedFiles.append((filePath, mimeType, None))
                    mimeTypeOutput = mimeType if mimeType else 'Unknown'
                    print(f' -> {filePath} -> [SKIP] Non-media/corrupt file of type: {mimeTypeOutput}')

            if not filesToProcess:
                continue

            files, checksums = zip(*filesToProcess)
            if len(filesToProcess) > 0:
                metadata = self._getMetadata(files)        

                for file, md, checksum in zip(files, metadata, checksums):
                    if md is None:
                        failedFiles.append((file, checksum))
                        continue
                    
                    md['HostName'] = hostname
                    md[self.checksumKey] = checksum

                    output = f' -> {file}'

                    if self.catalogDb.existsPath(filename=md['File:FileName'],
                                                 directory=md['File:Directory']):
                        if self.updateMode:
                            updatedFiles.append((file, checksum))
                            output += ' -> [UPDATE] File in catalog'
                        else:
                            raise RuntimeError('An existing file should not be in the main processing loop if not in update mode!')
                    else:
                        newFiles.append((file, checksum))
                        output += f' -> [NEW] {checksum}'

                    print(output)

                    if md['File:MIMEType'].startswith('audio'):
                        logging.warning('Audio fingerprinting disabled!')
                        # md['Acoustid:MatchResults'] = getAcoustid(file)

                    self.metadataCatalog.write(md, self.updateMode)
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

        self._printCatalogProcessResults(newFiles, updatedFiles, skippedFiles, failedFiles)

        return newFiles, updatedFiles, skippedFiles, failedFiles
    
    def _printCatalogProcessResults(self, newFiles, updatedFiles, skippedFiles, failedFiles):
        numProcessedFiles = len(newFiles) + len(updatedFiles) + len(skippedFiles) + len(failedFiles)
        print(f'\nCataloging complete!')
        print('====================')
        print(f'Files processed: {numProcessedFiles}')
        print(f'New files: {len(newFiles)}')
        print(f'Updated files: {len(updatedFiles)}')
        print(f'Skipped files: {len(skippedFiles)}')
        noUpdateFlag = False
        for file, mimeType, checksum in skippedFiles:
            if checksum:
                print(f'    {file} -> already in catalog: {checksum}')
                noUpdateFlag = True
            elif mimeType:
                print(f'    {file} -> not media - type: {mimeType}')
            else:
                print(f'    {file} -> unable to determine file type')

        print(f'Failed files: {len(failedFiles)}')
        for file, checksum in failedFiles:
            print(f'    {file} -> unable to read metadata!')

        if noUpdateFlag:
            print(f'\nSome files were skipped because they are already in the catalog. Use the -u flag to update them.')

        return numProcessedFiles

    def verify(self, 
                path: Optional[str]=None, 
                local: bool=False, 
                cloudStorage: Optional[CloudStorage]=None, 
                verifyChecksum: bool=False) -> bool:
        ''' Verify the catalog against the local files and in the cloud. If path is specified, only verify files in that path and subpaths.
        
            :param path: (str) Only verify files in this path and subpaths
            :param local: (bool) Verify local files
            :param cloudStorage: CloudStorage object to use for cloud verification
            :param verifyChecksum: (bool) Verify checksums of files
            :returns: (bool) True if all files are found and optionally matches checksums, False otherwise
        '''
        if path:
            path = os.path.abspath(path)
            directory = self.catalogDb._normalizeDirectory(path)
            if not directory.endswith('*'):
                directory += '*'
            records = self.catalogDb.read(directory=directory)
        else:
            records = self.catalogDb.read(all=True)
        if not local and not cloudStorage:
            raise ValueError('At least one of local or cloudStorage must be set!')

        numFiles = len(records)
        print(f'{numFiles} files in catalog')

        foundLocalFiles = []
        missingLocalFiles = []
        changedLocalFiles = []
        foundCloudFiles = []
        missingCloudFiles = []
        changedCloudFiles = []
        
        for record in records:
            filePath = os.path.join(record['directory'], record['file_name'])
            if local:
                if os.path.exists(filePath):
                    if verifyChecksum:
                        checksum = self._checksum(filePath, self.CHECKSUM_MODE)
                        if checksum == record['checksum']:
                            foundLocalFiles.append(record)
                        else:
                            changedLocalFiles.append(record)
                            print(f'[WARNING] Local file changed: {filePath}')
                    else:
                        foundLocalFiles.append(record)
                else:
                    missingLocalFiles.append(record)
                    print(f'Local file not found: {filePath}')
            if cloudStorage:
                if cloudStorage.fileExists(record['cloud_object_name']):
                    if verifyChecksum:
                        if cloudStorage.validateFile(record['cloud_object_name'], checksum=record['cloud_object_checksum']):
                            foundCloudFiles.append(record)
                        else:
                            changedCloudFiles.append(record)
                            print(f'[WARNING] Cloud file changed: {record["cloud_object_name"]} ({filePath})')
                    else:
                        foundCloudFiles.append(record)
                else:
                    missingCloudFiles.append(record)
                    print(f'Cloud file not found: {record["cloud_object_name"]} ({filePath})')

        print('\nVerification complete!')
        print('======================')
        if path:
            print(f'For path: {path}')
        print(f'Files in catalog: {numFiles}')
        if local:
            print(f'\nLocal files found: {len(foundLocalFiles)}')
            print(f'Local files missing: {len(missingLocalFiles)}')
            print(f'Local files changed: {len(changedLocalFiles)}')
        if cloudStorage:
            print(f'\nCloud files found: {len(foundCloudFiles)}')
            print(f'Cloud files missing: {len(missingCloudFiles)}')
            print(f'Cloud files changed: {len(changedCloudFiles)}')

        if missingLocalFiles or changedLocalFiles or missingCloudFiles or changedCloudFiles:
            return False
        
        return True

    def query(self, checksum=None, filename=None, directory=None, hostname=None):
        directory = os.path.abspath(directory) if directory else None
        dbRecords = self.catalogDb.read(checksum=checksum, filename=filename, directory=directory, hostname=hostname)
        if checksum is not None:
            metadataAndPaths = self.metadataCatalog.read(checksum, filename=filename, directory=directory, hostname=hostname, all=True)
        else:
            metadataAndPaths = []
            for record in dbRecords:
                currentMetadataAndPaths = self.metadataCatalog.read(record['checksum'], filename=record['file_name'], directory=record['directory'], hostname=hostname, all=True)
                metadataAndPaths.extend(currentMetadataAndPaths)
        return dbRecords, metadataAndPaths

    def remove(self, records, cloudStorage=None):
        ''' Removes files from the catalog and deletes the metadata files. 
            Typically called with the output of query(). Checks to see if
            the file has been removed from the cloud first. Then removes
            from the metadata catalog and then the finally the database.
        
            :param records: (dict) Remove these records from the catalog
            :returns: (int) Number of files removed
        '''
        recordsRemoved = []
        for i, record in enumerate(records, 1):
            checksum = record['checksum']
            filename = record['file_name']
            directory = record['directory']
            hostname = record['host_name']
            objectName = record['cloud_object_name']
            fullFilePath = os.path.join(directory, filename)
            percentComplete = i/len(records)*100
            print(f'[{i}/{len(records)} ({percentComplete:.3f %})] Removing {fullFilePath} ({checksum})...')
            
            if objectName:
                if cloudStorage:
                    cloudStorage.deleteFile(objectName)
                else:
                    raise RuntimeError('Cannot remove files from the catalog that are in the cloud! Remove from the cloud first.')

            self.metadataCatalog.delete(checksum, filename, directory, hostname)
            self.catalogDb.delete(checksum, filename, directory, hostname)
            recordsRemoved.append(record)

        self.catalogDb.commit()

        return recordsRemoved

    def checksum(self, filename: str) -> str:
        return self._checksum(filename, self.CHECKSUM_MODE)

    @classmethod
    def _getMetadata(self, filenames: list) -> list:
        return getMetadata(filenames)

    @classmethod
    def _addAdditionalMetadata(self, metadata: dict) -> dict:
        metadata['HostName'] = socket.gethostname()
        fullPath = os.path.join(metadata['File:Directory'], metadata['File:FileName'])
        metadata['File:SHA256Sum'] = self._checksum(fullPath, self.CHECKSUM_MODE)
        
        return metadata
    
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

    @classmethod
    def _checksum(self, filename: str, mode: str) -> str:
        if mode == 'MD5':
            return md5sum(filename, self.HASH_CHUNK_SIZE)
        elif mode == 'SHA256':
            return sha256sum(filename, self.HASH_CHUNK_SIZE)
        else:
            raise ValueError(f'Invalid checksum mode ({mode})!')
        

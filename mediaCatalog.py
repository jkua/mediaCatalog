import os
import glob
import logging
import socket
import json
import yaml

import jsonlines

from metadataCatalogHDT import MetadataCatalogHDT
from catalogDatabase import CatalogDatabase
from utils import md5sum, sha256sum, getMetadata, getAcoustid


class MediaCatalog(object):
    CONFIG_FILENAME = 'config.yaml'
    CATALOG_DB_FILENAME = 'catalog.db'
    METADATA_FOLDERNAME = 'metadata'
    HASH_TABLE_FILENAME = 'hashTable.jsonl'
    MEDIA_EXTENSIONS = ['cr2', 'cr3', 'jpg', 'jpeg', 'heif', 'mov', 'mp4', 'mp3', 'm4a']
    CHECKSUM_MODE = 'SHA256'

    def __init__(self, catalogPath, create=False, update=False):
        self.catalogPath = catalogPath
        self.createMode = create
        self.updateMode = update
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
        if not os.path.exists(self.catalogPath):
            if self.createMode:
                logging.info(f'No catalog at {self.catalogPath} - creating catalog')
                self._createCatalog()
            else:
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
            if self.createMode:
                self._createConfig()
            else:
                raise FileNotFoundError('Could not load config file at: {self.configPath}! Broken catalog!')

    def _createConfig(self):
        self.config = {'project': '', 'defaultBucket': ''}
        with open(self.configPath, 'w') as f:
            yaml.dump(self.config, f)

    def _createCatalog(self):
        # Create the catalog folder
        os.mkdir(self.catalogPath)

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
        hostname = socket.gethostname()
        for dirName, subdirList, fileList in os.walk(path):
            print('Found directory: %s' % dirName)
            filesToProcess = []
            for fname in sorted(fileList):
                head, ext = os.path.splitext(fname)
                if len(ext) > 1:
                    ext = ext[1:].lower()
                    if ext in self.MEDIA_EXTENSIONS:
                        filePath = os.path.join(dirName, fname)
                        checksum = self._checksum(filePath, self.CHECKSUM_MODE)
                        # TODO Extend the database check to also look for same capture device and filename/time in case of corruption
                        if self.updateMode or not self.catalogDb.exists(checksum):
                            filesToProcess.append((filePath, checksum))
                        else:
                            print(f'File already in catalog! Skipping! {filePath}, Hash: {checksum}')

            if not filesToProcess:
                continue

            files, checksums = zip(*filesToProcess)
            if len(filesToProcess) > 0:
                print('Extracting metadata')
                metadata = self._getMetadata(files)

                for file, md, checksum in zip(files, metadata, checksums):
                    # TODO This probably needs to be more robust to the range of bad files
                    # And should this be here or upstream in the filesToProcess generation?
                    if md['File:FileSize'] == 0:
                        raise Exception('File size is zero!')
                    md['HostName'] = hostname
                    
                    md[self.checksumKey] = checksum
                    print(f'\n{file} -> {checksum}')

                    if md['File:MIMEType'].startswith('audio'):
                        md['Acoustid:MatchResults'] = getAcoustid(file)

                    metadataPath = self.metadataCatalog.write(md, self.updateMode)
                    self.catalogDb.write(md, self.updateMode)

                    # Readback test - TODO Move this to a test case
                    md_readback = self.metadataCatalog.read(md[self.checksumKey])
                    for key, value in md.items():
                        if value != md_readback[key]:
                            print(f'Mismatch for [{key}]: ({value} != {md_readback[key]}')
                    # assert md_readback == md

                    self.catalogDb.printFileRecord(md[self.checksumKey])

            self.catalogDb.commit()

    def query(self, checksum):
        dbRecord = self.catalogDb.read(checksum)
        metadata = self.metadataCatalog.read(checksum)
        return dbRecord, metadata

    def checksum(self, filename: str) -> str:
        return self._checksum(filename, self.CHECKSUM_MODE)

    def _getMetadata(self, filenames: list) -> list:
        return getMetadata(filenames)

    def _checksum(self, filename: str, mode: str) -> str:
        if mode == 'MD5':
            return md5sum(filename, self.hashChunkSize)
        elif mode == 'SHA256':
            return sha256sum(filename, self.hashChunkSize)
        else:
            raise ValueError(f'Invalid checksum mode ({mode})!')
        

import os
import glob
import logging
import socket
import json

import jsonlines

from metadataCatalogHDT import MetadataCatalogHDT
from catalogDatabase import CatalogDatabase
from utils import md5sum, sha256sum, getMetadata, getAcoustid


class MediaCataloger(object):
    METADATA_FOLDERNAME = 'metadata'
    CATALOG_DB_FILENAME = 'catalog.db'
    HASH_TABLE_FILENAME = 'hashTable.jsonl'
    MEDIA_EXTENSIONS = ['cr2', 'cr3', 'jpg', 'jpeg', 'heif', 'mov', 'mp4', 'mp3', 'm4a']
    CHECKSUM_MODE = 'SHA256'

    def __init__(self, catalogPath):
        self.catalogPath = catalogPath
        self.hashFilePath = os.path.join(self.catalogPath, self.HASH_TABLE_FILENAME)
        self.catalogDbPath = os.path.join(self.catalogPath, self.CATALOG_DB_FILENAME)
        self.metadataCatalogPath = os.path.join(self.catalogPath, self.METADATA_FOLDERNAME)
        self.hashChunkSize = 1024*1024*16
        self.hashDict = {}
        self.catalogDb = None
        self.metadataCatalog = None
        self.checksumKey = f'File:{self.CHECKSUM_MODE}Sum'

        self.open()

    def open(self):
        if not os.path.exists(self.catalogPath):
            logging.info(f'No catalog at {self.catalogPath} - creating catalog')
            self.createCatalog()
        else:
            self.loadCatalog()

    def close(self):
        self.catalogDb.close()

    def createCatalog(self):
        # Create the catalog folder
        os.mkdir(self.catalogPath)

        # Create the metadata folder - subfolders are two character segments of the md5sum
        os.mkdir(self.metadataCatalogPath)
        self.catalogDb = CatalogDatabase(self.catalogDbPath)
        self.metadataCatalog = MetadataCatalogHDT(self.metadataCatalogPath, self.CHECKSUM_MODE)

    def loadCatalog(self):
        if not os.path.exists(self.metadataCatalogPath):
            raise Exception('Missing metadata folder!')

        self.catalogDb = CatalogDatabase(self.catalogDbPath)
        self.metadataCatalog = MetadataCatalogHDT(self.metadataCatalogPath, self.CHECKSUM_MODE)

    def loadHashTable(self):
        with jsonlines.open(self.hashFilePath) as reader:
            for obj in reader:
                self.hashDict[obj['hash']] = obj

    def catalog(self, path):
        hostname = socket.gethostname()
        for dirName, subdirList, fileList in os.walk(path):
            print('Found directory: %s' % dirName)
            filesToProcess = []
            for fname in fileList:
                head, ext = os.path.splitext(fname)
                if len(ext) > 1:
                    ext = ext[1:].lower()
                    if ext in self.MEDIA_EXTENSIONS:
                        filesToProcess.append(os.path.join(dirName, fname))

            filesToProcess.sort()
            if len(filesToProcess) > 0:
                print('Extracting metadata')
                metadata = self._getMetadata(filesToProcess)

                for file, md in zip(filesToProcess, metadata):
                    md['HostName'] = hostname
                    checksum = self._checksum(file, self.CHECKSUM_MODE)
                    md[self.checksumKey] = checksum
                    print(f'\n{file} -> {checksum}')
                    if md['File:MIMEType'].startswith('audio'):
                        md['Acoustid:MatchResults'] = getAcoustid(file)

                    metadataPath = self.metadataCatalog.write(md)
                    self.catalogDb.write(md, metadataPath)

                    # Readback test - TODO Move this to a test case
                    md_readback = self.metadataCatalog.read(md[self.checksumKey])
                    assert md_readback == md

                    record = self.catalogDb.read(md[self.checksumKey])
                    for key, value in zip(record.keys(), record):
                        print(f'{key}: {value}')

        self.catalogDb.commit()

    def _getMetadata(self, filenames: list) -> list:
        return getMetadata(filenames)

    def _checksum(self, filename: str, mode: str) -> str:
        if mode == 'MD5':
            return md5sum(filename, self.hashChunkSize)
        elif mode == 'SHA256':
            return sha256sum(filename, self.hashChunkSize)
        else:
            raise ValueError(f'Invalid checksum mode ({mode})!')
        

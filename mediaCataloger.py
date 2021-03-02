import os
import glob
import logging

import jsonlines

from metadataCatalog import MetadataCatalog
from utils import md5sum, getMetadata


class MediaCataloger(object):
    METADATA_FOLDERNAME = 'metadata'
    HASH_TABLE_FILENAME = 'hashTable.jsonl'
    MEDIA_EXTENSIONS = ['cr2', 'cr3', 'jpg', 'jpeg', 'heif', 'mov', 'mp4', 'mp3', 'm4a']

    def __init__(self, catalogPath):
        self.catalogPath = catalogPath
        self.hashFilePath = os.path.join(self.catalogPath, self.HASH_TABLE_FILENAME)
        self.metadataCatalogPath = os.path.join(self.catalogPath, self.METADATA_FOLDERNAME)
        self.md5ChunkSize = 1024*1024*16
        self.hashDict = {}
        self.metadataCatalog = None

        self.open()

    def open(self):
        if not os.path.exists(self.catalogPath):
            logging.info(f'No catalog at {self.catalogPath} - creating catalog')
            self.createCatalog()
        else:
            self.loadCatalog()

    def createCatalog(self):
        # Create the catalog folder
        os.mkdir(self.catalogPath)

        # Create the metadata folder - subfolders are two character segments of the md5sum
        os.mkdir(self.metadataCatalogPath)
        self.metadataCatalog = MetadataCatalog(self.metadataCatalogPath)

        # Create the hash table - single JSON per line
        with open(self.hashFilePath, 'wt') as f:
            pass

    def loadCatalog(self):
        if not os.path.exists(self.metadataCatalogPath):
            raise Exception('Missing metadata folder!')
        if not os.path.exists(self.hashFilePath):
            raise Exception('Missing hash table!')

        self.loadHashTable(self)
        self.metadataCatalog = MetadataCatalog(self.metadataCatalogPath)

    def loadHashTable(self):
        with jsonlines.open(self.hashFilePath) as reader:
            for obj in reader:
                self.hashDict[obj['hash']] = obj

    def catalog(self, path):
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
                    checksum = self._md5sum(file)
                    md['File:MD5Sum'] = checksum
                    # print(f'{file} -> {checksum}')

                    self.metadataCatalog.write(md)

                # Test readback
                test = [self.metadataCatalog.read(md['File:MD5Sum']) for md in metadata]
                assert metadata == test

    def _getMetadata(self, filenames: list) -> list:
        return getMetadata(filenames)

    def _md5sum(self, filename: str) -> str:
        return md5sum(filename, self.md5ChunkSize)

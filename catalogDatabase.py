import os
import sqlite3
import json
from packaging.version import Version

import utils

class CatalogDatabase(object):
    SCHEMA_VERSION = Version('0.1.0')
    MIN_SCHEMA_VERSION = Version('0.1.0')

    def __init__(self, dbPath):
        self.dbPath = dbPath
        print(self.dbPath)
        self._open_db()

    def _open_db(self):
        self.connection = sqlite3.connect(self.dbPath)
        self.connection.row_factory = sqlite3.Row
        
        if self.connection:
            self.cursor = self.connection.cursor()

            initialized = self._check_db_schema()

            if not initialized:
                self._init_db()

    def _check_db_schema(self):
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'")
        
        if self.cursor.fetchone() is None:
            return False

        self.cursor.execute('SELECT major, minor, patch FROM schema_version WHERE valid_to IS NULL')
        rows = self.cursor.fetchall()
        if len(rows) == 0:
            raise Exception('Database missing schema version!')
        elif len(rows) > 1:
            for row in rows:
                logging.debug(print(row))
            raise Exception('Multiple schema versions!')

        row = rows[0]
        db_version = Version(f'{row[0]}.{row[1]}.{row[2]}')

        print(f'Database schema version: {db_version}')

        if db_version < self.MIN_SCHEMA_VERSION:
            raise Exception(f'Database schema version ({db_version}) is too old! Need {self.MIN_SCHEMA_VERSION} to {self.SCHEMA_VERSION}!')

        if db_version > self.SCHEMA_VERSION:
            raise Exception(f'Database schema version ({db_version}) is too new! Need {self.MIN_self.SCHEMA_VERSION} to {SCHEMA_VERSION}!')

        return True

    def _init_db(self):
        self.cursor.execute(
            '''CREATE TABLE schema_version 
                (
                    schema_version_id INTEGER PRIMARY KEY,
                    valid_from DATETIME NOT NULL,
                    valid_to DATETIME NULL,
                    major INTEGER NOT NULL,
                    minor INTEGER NOT NULL,
                    patch INTEGER NOT NULL
                )
            '''
            )

        self.cursor.execute(
            '''INSERT INTO schema_version
                (valid_from, valid_to, major, minor, patch)
                VALUES
                (DATETIME('now'), NULL, ?, ?, ?)
            ''',
            (self.SCHEMA_VERSION.major, self.SCHEMA_VERSION.minor, self.SCHEMA_VERSION.micro)
            )

        self.cursor.execute(
            '''CREATE TABLE photos 
                (
                    checksum BLOB PRIMARY KEY,
                    file_name TEXT NOT NULL,
                    directory TEXT NULL,
                    host_name TEXT NULL,
                    file_size INTEGER NOT NULL,
                    file_modify_datetime DATETIME NOT NULL,
                    file_mime_type TEXT NOT NULL,
                    camera_model TEXT NULL,
                    camera_serial_number TEXT NULL,
                    capture_datetime DATETIME NULL,
                    metadata_path TEXT NOT NULL,
                    cloud_bucket_name TEXT NULL,
                    cloud_object_name TEXT NULL
                )
            '''
            )

        self.connection.commit()

    def write(self, metadata, metadataPath, updateMode=False):
        try:
            self.cursor.execute(
                '''INSERT INTO photos
                    (
                        checksum, 
                        file_name,
                        directory,
                        host_name, 
                        file_size, 
                        file_modify_datetime,
                        file_mime_type,
                        camera_model,
                        camera_serial_number,
                        capture_datetime, 
                        metadata_path
                    )
                    VALUES
                    (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                ''',
                (
                    metadata['File:SHA256Sum'],
                    metadata['File:FileName'],
                    metadata['File:Directory'],
                    metadata['HostName'],
                    metadata['File:FileSize'],
                    metadata['File:FileModifyDate'],
                    metadata['File:MIMEType'],
                    metadata.get('EXIF:Model'),
                    metadata.get('EXIF:SerialNumber'),
                    utils.getPreciseCaptureTimeFromExif(metadata),
                    metadataPath
                )
            )
        except sqlite3.IntegrityError as e:
            if updateMode:
                self.cursor.execute(
                    '''UPDATE photos
                        SET file_name = ?,
                            directory = ?,
                            host_name = ?, 
                            file_size = ?, 
                            file_modify_datetime = ?,
                            file_mime_type = ?,
                            camera_model = ?,
                            camera_serial_number = ?,
                            capture_datetime = ?, 
                            metadata_path = ?
                        WHERE
                            checksum = ?
                    ''',
                    (
                        metadata['File:FileName'],
                        metadata['File:Directory'],
                        metadata['HostName'],
                        metadata['File:FileSize'],
                        metadata['File:FileModifyDate'],
                        metadata['File:MIMEType'],
                        metadata.get('EXIF:Model'),
                        metadata.get('EXIF:SerialNumber'),
                        utils.getPreciseCaptureTimeFromExif(metadata),
                        metadataPath,
                        metadata['File:SHA256Sum']
                    )
                )
            else:
                raise e

    def read(self, checksum):
        self.cursor.execute(
            'SELECT * FROM photos WHERE checksum = ?',
            (checksum,)
        )
        records = self.cursor.fetchall()
        if len(records) == 0:
            raise KeyError(f'Checksum {checksum} not found in database!')
        elif len(records) > 1:
            print(f'WARNING: Multiple ({len(records)}) entries with checksum {checksum} in database! Returning first!')

        return records[0]

    def commit(self):
        self.connection.commit()

    def close(self):
        self.connection.close()


if __name__=='__main__':
    catalog = CatalogDatabase('test.db')


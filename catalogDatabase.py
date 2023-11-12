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
        print(f'Opening catalog database at: {self.dbPath}')
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
            # TODO add mac address support
            '''CREATE TABLE host
                (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    UNIQUE(name)
                )
            '''
            )

        self.cursor.execute(
            '''CREATE TABLE mime_type
                (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    type TEXT UNIQUE NOT NULL
                )
            '''
            )

        self.cursor.execute(
            '''CREATE TABLE cloud_storage 
                (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    bucket TEXT NOT NULL,
                    UNIQUE(name, bucket)
                )
            '''
            )

        self.cursor.execute(
            '''CREATE TABLE capture_device
                (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    make TEXT NULL,
                    model TEXT NULL,
                    serial_number TEXT NULL,
                    UNIQUE(make, model, serial_number)
                )
            '''
            )

        self.cursor.execute(
            '''CREATE TABLE file
                (
                    checksum BLOB PRIMARY KEY,
                    file_name TEXT NOT NULL,
                    directory TEXT NULL,
                    host_id INTEGER NULL,
                    file_size INTEGER NOT NULL,
                    file_modify_datetime DATETIME NOT NULL,
                    file_mime_type_id INTEGER NULL,
                    capture_device_id INTEGER NULL,
                    capture_datetime DATETIME NULL,
                    metadata_path TEXT NOT NULL,
                    cloud_storage_id INTEGER NULL,
                    cloud_object_name TEXT NULL,
                    FOREIGN KEY(host_id) REFERENCES host(id) ON DELETE SET NULL,
                    FOREIGN KEY(file_mime_type_id) REFERENCES mime_type(id) ON DELETE SET NULL,
                    FOREIGN KEY(capture_device_id) REFERENCES capture_device(id) ON DELETE SET NULL,
                    FOREIGN KEY(cloud_storage_id) REFERENCES cloud_storage(id) ON DELETE SET NULL
                )
            '''
            )        

        self.connection.commit()

    def write(self, metadata, metadataPath, updateMode=False):
        hostId = self.getHostId(metadata['HostName'], insert=True)
        mimeTypeId = self.getMimeTypeId(metadata['File:MIMEType'], insert=True)
        captureDeviceId = self.getCaptureDeviceId(metadata['EXIF:Make'], metadata['EXIF:Model'], metadata['EXIF:SerialNumber'], insert=True)

        command = '''INSERT INTO file
                    (
                        checksum, 
                        file_name,
                        directory,
                        host_id, 
                        file_size, 
                        file_modify_datetime,
                        file_mime_type_id,
                        capture_device_id,
                        capture_datetime, 
                        metadata_path
                    )
                    VALUES
                    (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                '''
        values = (
                    metadata['File:SHA256Sum'],
                    metadata['File:FileName'],
                    metadata['File:Directory'],
                    hostId,
                    metadata['File:FileSize'],
                    metadata['File:FileModifyDate'],
                    mimeTypeId,
                    captureDeviceId,
                    utils.getPreciseCaptureTimeFromExif(metadata),
                    metadataPath
                )

        if updateMode:
            command += ''' ON CONFLICT(checksum) DO UPDATE
                        SET file_name = ?,
                            directory = ?,
                            host_id = ?, 
                            file_size = ?, 
                            file_modify_datetime = ?,
                            file_mime_type_id = ?,
                            capture_device_id = ?,
                            capture_datetime = ?, 
                            metadata_path = ?
                        WHERE
                            checksum = ?
                '''
            values += (
                        metadata['File:FileName'],
                        metadata['File:Directory'],
                        hostId,
                        metadata['File:FileSize'],
                        metadata['File:FileModifyDate'],
                        mimeTypeId,
                        captureDeviceId,
                        utils.getPreciseCaptureTimeFromExif(metadata),
                        metadataPath,
                        metadata['File:SHA256Sum']
                    )
        try:
            self.cursor.execute(command, values)
        except sqlite3.IntegrityError as e:
            if not updateMode:
                print(f'WARNING: File already in database! Ignoring.')

    def read(self, checksum):
        self.cursor.execute(
            '''SELECT checksum,
                    file_name,
                    directory,
                    host.name as host_name,
                    file_size,
                    file_modify_datetime,
                    mime_type.type as file_mime_type,
                    capture_device.make as capture_device_make,
                    capture_device.model as capture_device_model,
                    capture_device.serial_number as capture_device_serial_number,
                    capture_datetime,
                    metadata_path,
                    cloud_storage.name as cloud_name,
                    cloud_storage.bucket as cloud_bucket,
                    cloud_object_name
                FROM file
                JOIN host on file.host_id = host.id
                JOIN mime_type on file.file_mime_type_id = mime_type.id
                JOIN capture_device on file.capture_device_id = capture_device.id
                JOIN cloud_storage on file.cloud_storage_id = cloud_storage.id
                WHERE checksum = ?
            ''',
            (checksum,)
        )
        records = self.cursor.fetchall()
        if len(records) == 0:
            raise KeyError(f'Checksum {checksum} not found in database!')
        elif len(records) > 1:
            print(f'WARNING: Multiple ({len(records)}) entries with checksum {checksum} in database! Returning first!')

        return records[0]

    def exists(self, checksum):
        self.cursor.execute(
            'SELECT EXISTS(SELECT 1 FROM file WHERE checksum = ?)',
            (checksum,)
        )
        records = self.cursor.fetchall()
        return records[0] == 1

    def setCloudStorage(self, checksum, projectId, bucketName, objectName):
        cloudStorageId = self.getCloudStorageId(projectId, bucketName, insert=True)
        self.cursor.execute(
            '''UPDATE file
                SET cloud_storage_id=?,
                    cloud_object_name=?
                WHERE checksum=?
            ''',
            (cloudStorageId, objectName, checksum)
        )

    def commit(self):
        self.connection.commit()

    def close(self):
        self.connection.close()

    def getHostId(self, hostName, insert=False):
        # TODO: Add MAC address support
        if insert:
            self.cursor.execute(
                'INSERT INTO host(name) VALUES (?) ON CONFLICT(name) DO NOTHING',
                (hostName,)
            )

        self.cursor.execute(
            'SELECT id FROM host WHERE name = ?',
            (hostName,)
        )
        records = self.cursor.fetchall()
        if len(records) > 1:
            import pdb; pdb.set_trace()
            raise RuntimeError(f'Duplicate hostnames ({hostName}) in database!')

        return records[0][0]

    def getMimeTypeId(self, mimeType, insert=False):
        if insert:
            self.cursor.execute(
                'INSERT INTO mime_type(type) VALUES (?) ON CONFLICT(type) DO NOTHING',
                (mimeType,)
            )

        self.cursor.execute(
            'SELECT id FROM mime_type WHERE type = ?',
            (mimeType,)
        )
        records = self.cursor.fetchall()
        if len(records) > 1:
            raise RuntimeError(f'Duplicate mime types ({mimeType}) in database!')

        return records[0][0]

    def getCaptureDeviceId(self, make, model, serialNumber, insert=False):
        if insert:
            self.cursor.execute(
                'INSERT INTO capture_device(make, model, serial_number) VALUES (?, ?, ?) ON CONFLICT(make, model, serial_number) DO NOTHING',
                (make, model, serialNumber)
            )

        self.cursor.execute(
            'SELECT id FROM capture_device WHERE make = ? AND model = ? AND serial_number = ?',
            (make, model, serialNumber)
        )
        records = self.cursor.fetchall()
        if len(records) > 1:
            raise RuntimeError(f'Duplicate capture devices ({make}, {model}, {serialNumber}) in database!')

        return records[0][0]

    def getCloudStorageId(self, projectId, bucket, insert=False):
        if insert:
            self.cursor.execute(
                'INSERT INTO cloud_storage(name, bucket) VALUES (?, ?) ON CONFLICT(name, bucket) DO NOTHING',
                (projectId, bucket)
            )

        self.cursor.execute(
            'SELECT id FROM cloud_storage WHERE name = ? AND bucket = ?',
            (projectId, bucket)
        )
        records = self.cursor.fetchall()
        if len(records) > 1:
            raise RuntimeError(f'Duplicate cloud storage entries ({projectId}, {bucket}) in database!')

        return records[0][0]

    def getFileCount(self):
        self.cursor.execute('SELECT COUNT(checksum) FROM file')
        records = self.cursor.fetchall()
        return records[0][0]

    def getFilesNotInCloud(self):
        self.cursor.execute('SELECT checksum, file_name, directory FROM file WHERE cloud_storage_id IS NULL')
        records = self.cursor.fetchall()
        return records

    def printFileRecord(self, checksum):
        record = self.read(checksum)
        print('')
        for key, value in zip(record.keys(), record):
            print(f'{key}: {value}')


if __name__=='__main__':
    catalog = CatalogDatabase('test.db')


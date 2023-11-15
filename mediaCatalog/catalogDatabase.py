import sqlite3
from packaging.version import Version
import logging
import os

from .utils import getPreciseCaptureTimeFromExif

class CatalogDatabase(object):
    SCHEMA_VERSION = Version('0.1.0')
    MIN_SCHEMA_VERSION = Version('0.1.0')

    def __init__(self, dbPath):
        self.dbPath = os.path.abspath(dbPath)
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

        # TODO: Add checksum index and maybe a path index
        self.cursor.execute(
            '''CREATE TABLE file
                (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    checksum BLOB NOT NULL,
                    file_name TEXT NOT NULL,
                    directory TEXT NULL,
                    host_id INTEGER NULL,
                    file_size INTEGER NOT NULL,
                    file_modify_datetime DATETIME NOT NULL,
                    file_mime_type_id INTEGER NULL,
                    capture_device_id INTEGER NULL,
                    capture_datetime DATETIME NULL,
                    cloud_storage_id INTEGER NULL,
                    cloud_object_name TEXT NULL,
                    cloud_object_checksum BLOB NULL,
                    FOREIGN KEY(host_id) REFERENCES host(id) ON DELETE SET NULL,
                    FOREIGN KEY(file_mime_type_id) REFERENCES mime_type(id) ON DELETE SET NULL,
                    FOREIGN KEY(capture_device_id) REFERENCES capture_device(id) ON DELETE SET NULL,
                    FOREIGN KEY(cloud_storage_id) REFERENCES cloud_storage(id) ON DELETE SET NULL,
                    UNIQUE(file_name, directory, host_id)
                )
            '''
            )        

        self.connection.commit()

    def write(self, metadata, updateMode=False):
        hostId = self.getHostId(metadata['HostName'], insert=True)
        mimeTypeId = self.getMimeTypeId(metadata['File:MIMEType'], insert=True)
        captureDeviceId = self.getCaptureDeviceId(metadata.get('EXIF:Make'), metadata.get('EXIF:Model'), metadata.get('EXIF:SerialNumber'), insert=True)

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
                        capture_datetime
                    )
                    VALUES
                    (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?
                    )
                '''
        values = (
                    metadata['File:SHA256Sum'],
                    metadata['File:FileName'],
                    self._normalizeDirectory(metadata['File:Directory']),
                    hostId,
                    metadata['File:FileSize'],
                    metadata['File:FileModifyDate'],
                    mimeTypeId,
                    captureDeviceId,
                    getPreciseCaptureTimeFromExif(metadata),
                )

        if updateMode:
            command += ''' ON CONFLICT(file_name, directory, host_id) DO UPDATE
                        SET file_size = ?, 
                            file_modify_datetime = ?,
                            file_mime_type_id = ?,
                            capture_device_id = ?,
                            capture_datetime = ?
                        WHERE
                            checksum = ?
                '''
            values += (
                        metadata['File:FileSize'],
                        metadata['File:FileModifyDate'],
                        mimeTypeId,
                        captureDeviceId,
                        getPreciseCaptureTimeFromExif(metadata),
                        metadata['File:SHA256Sum']
                    )
        try:
            self.cursor.execute(command, values)
        except sqlite3.IntegrityError as e:
            if not updateMode:
                print(f'WARNING: File already in database! Ignoring.')

    def read(self, checksum=None, filename=None, directory=None, hostname=None, all=False):
        '''Returns a list of records matching the query
            
            :param checksum: Checksum of the file
            :param filename: Filename of the file - accepts wildcards (*, ?)
            :param directory: Directory of the file - accepts wildcards (*, ?)
            :param hostname: Hostname of the file - accepts wildcards (*, ?)
            :param all: Return all records in the database - ignores all other query filters
            :returns list: List of records matching the query
        '''
        if not all and checksum is None and filename is None and directory is None and hostname is None:
            raise Exception('Must supply at least one of checksum, filename, directory, or hostname!')

        directory = self._normalizeDirectory(directory)

        command = '''SELECT file.id,
                        checksum,
                        file_name,
                        directory,
                        host.id as host_id,
                        host.name as host_name,
                        file_size,
                        file_modify_datetime,
                        mime_type.type as file_mime_type,
                        capture_device.id as capture_device_id,
                        capture_device.make as capture_device_make,
                        capture_device.model as capture_device_model,
                        capture_device.serial_number as capture_device_serial_number,
                        capture_datetime,
                        cloud_storage.id as cloud_storage_id,
                        cloud_storage.name as cloud_name,
                        cloud_storage.bucket as cloud_bucket,
                        cloud_object_name,
                        cloud_object_checksum
                    FROM file
                    LEFT JOIN host on file.host_id = host.id
                    LEFT JOIN mime_type on file.file_mime_type_id = mime_type.id
                    LEFT JOIN capture_device on file.capture_device_id = capture_device.id
                    LEFT JOIN cloud_storage on file.cloud_storage_id = cloud_storage.id
                '''
        tokens = []
        values = []
        if checksum is not None:
            tokens.append('checksum = ?')
            values.append(checksum)
        if filename is not None:
            if '*' in filename or '?' in filename:
                tokens.append('file_name LIKE ?')
                values.append(filename.replace('*', '%').replace('?', '_'))
            else:
                tokens.append('file_name = ?')
                values.append(filename)
        if directory is not None:
            if '*' in directory or '?' in directory:
                tokens.append('directory LIKE ?')
                values.append(directory.replace('*', '%').replace('?', '_'))
            else:
                tokens.append('directory = ?')
                values.append(directory)
        if hostname is not None:
            if '*' in hostname or '?' in hostname:
                tokens.append('host_name LIKE ?')
                values.append(hostname.replace('*', '%').replace('?', '_'))
            else:
                tokens.append('host_name = ?')
                values.append(hostname)
        if not all:
            command += 'WHERE ' + ' AND '.join(tokens)
        else:
            values = []
        self.cursor.execute(command, values)

        records = self.cursor.fetchall()
        if len(records) == 0 and not all:
            errorMessage = 'File not found in database with '
            tokens = []
            if checksum is not None:
                tokens.append(f'checksum: {checksum}')
            if filename is not None:
                tokens.append(f'filename: {filename}')
            if directory is not None:
                tokens.append(f'directory: {directory}')
            if hostname is not None:
                tokens.append(f'hostname: {hostname}')
            errorMessage += ', '.join(tokens) + ' !'
            raise KeyError(errorMessage)

        return records

    def existsChecksum(self, checksum):
        self.cursor.execute(
            'SELECT EXISTS(SELECT 1 FROM file WHERE checksum = ?)',
            (checksum,)
        )
        records = self.cursor.fetchall()
        return records[0][0] == 1

    def existsPath(self, filename, directory=None, hostname=None):
        directory = self._normalizeDirectory(directory)

        command = 'SELECT EXISTS(SELECT 1 FROM file '
        values = [filename]
        tokens = ['file_name = ?']
        if directory is not None:
            tokens.append('directory = ?')
            values.append(directory)
        if hostname is not None:
            tokens.append('host_name = ?')
            values.append(hostname)
        command += 'WHERE ' + ' AND '.join(tokens)
        command += ')'
        self.cursor.execute(command, values)
        records = self.cursor.fetchall()
        return records[0][0] == 1

    def setCloudStorage(self, checksum, projectId, bucketName, objectName, objectChecksum):
        cloudStorageId = self.getCloudStorageId(projectId, bucketName, insert=True)
        self.cursor.execute(
            '''UPDATE file
                SET cloud_storage_id=?,
                    cloud_object_name=?,
                    cloud_object_checksum=?
                WHERE checksum=?
            ''',
            (cloudStorageId, objectName, objectChecksum, checksum)
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
        if make is None and model is None and serialNumber is None:
            return None

        if insert:
            try:
                self._getCaptureDeviceId(make, model, serialNumber)
            except LookupError as e:
                self.cursor.execute(
                    'INSERT INTO capture_device(make, model, serial_number) VALUES (?, ?, ?) ON CONFLICT(make, model, serial_number) DO NOTHING',
                    (make, model, serialNumber)
                )    

        return self._getCaptureDeviceId(make, model, serialNumber)

    def _getCaptureDeviceId(self, make, model, serialNumber):
        command = 'SELECT id FROM capture_device WHERE '
        values = []
        commandTokens = []
        if make is not None:
            commandTokens.append('make = ?')
            values.append(make)
        else:
            commandTokens.append('make IS NULL')
        if model is not None:
            commandTokens.append('model = ?')
            values.append(model)
        else:
            commandTokens.append('model IS NULL')
        if serialNumber is not None:
            commandTokens.append('serial_number = ?')
            values.append(serialNumber)
        else:
            commandTokens.append('serial_number IS NULL')

        command += ' AND '.join(commandTokens)
        self.cursor.execute(command, values)
        records = self.cursor.fetchall()
        if len(records) > 1:
            raise RuntimeError(f'Duplicate ({len(records)}) capture devices ({make}, {model}, {serialNumber}) in database!')
        elif len(records) == 0:
            raise LookupError(f'Failed to find capture device ({make}, {model}, {serialNumber}) in database!')

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
        self.cursor.execute(
            '''SELECT checksum, 
                    file_name, 
                    directory, 
                    mime_type.type as file_mime_type 
                FROM file 
                JOIN mime_type ON file.file_mime_type_id = mime_type.id
                WHERE cloud_storage_id IS NULL
            '''
        )
        records = self.cursor.fetchall()
        return records

    def printFileRecord(self, checksum):
        record = self.read(checksum)[0]
        print('')
        for key, value in zip(record.keys(), record):
            print(f'{key}: {value}')

    def _normalizeDirectory(self, directory):
        if directory is None:
            return None

        directory = os.path.normpath(directory)
        if not directory.endswith('*'):
            directory += '/'
        return directory


if __name__=='__main__':
    catalog = CatalogDatabase('test.db')


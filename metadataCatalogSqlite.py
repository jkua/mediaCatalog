import sqlite3
from packaging.version import Version

from metadataCatalog import MetadataCatalog

class MetadataCatalogSqlite(MetadataCatalog):
    SCHEMA_VERSION = Version('0.1.0')
    MIN_SCHEMA_VERSION = Version('0.1.0')

    def __init__(self, dbPath):
        self.dbPath = dbPath
        self._open_db()

        self.connection.close()

    def _open_db(self):
        self.connection = sqlite3.connect(self.dbPath)
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
                    photo_id BLOB PRIMARY KEY,
                    directory TEXT NULL,
                    file_name TEXT NOT NULL,
                    file_size INTEGER NOT NULL,
                    file_creation_datetime DATETIME NOT NULL,
                    capture_datetime DATETIME NULL,
                    checksum BLOB NOT NULL,
                    metadata BLOB NULL,
                    thumbnail BLOB NULL
                )
            '''
            )

        self.connection.commit()


if __name__=='__main__':
    catalog = MetadataCatalogSqlite('test.db')


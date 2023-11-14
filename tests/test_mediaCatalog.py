import pytest
import os
import yaml
from mediaCatalog.mediaCatalog import MediaCatalog

class TestMediaCatalog:
    @pytest.fixture
    def catalog_dir(self, tmp_path):
        return tmp_path / 'catalog'

    @pytest.fixture
    def sample_data_dir(self):
        return os.path.join(os.path.dirname(__file__), '..', 'data/sample')
    
    @pytest.fixture
    def new_catalog(self, catalog_dir):
        catalog = MediaCatalog(catalog_dir, create=True)
        return catalog

    @pytest.fixture
    def sample_catalog(self, catalog_dir, sample_data_dir):
        catalog = MediaCatalog(catalog_dir, create=True)
        catalog.catalog(sample_data_dir)
        return catalog    

    def test_init(self, new_catalog, catalog_dir):
        assert new_catalog.catalogPath == catalog_dir
        assert new_catalog.createMode == False
        assert new_catalog.updateMode == False
        assert new_catalog.configPath == os.path.join(catalog_dir, 'config.yaml')
        assert new_catalog.catalogDbPath == os.path.join(catalog_dir, 'catalog.db')
        assert new_catalog.metadataCatalogPath == os.path.join(catalog_dir, 'metadata')
        assert new_catalog.checksumKey == 'File:SHA256Sum'

    def test_create_config(self, new_catalog):
        new_catalog._createConfig()
        assert os.path.exists(new_catalog.configPath)
        assert new_catalog.config == {'cloudProject': '', 'defaultCloudBucket': '', 'cloudObjectPrefix': 'file'}
        assert new_catalog.config == yaml.safe_load(open(new_catalog.configPath, 'r'))

    def test_load_config(self, new_catalog):
        new_catalog._createConfig()
        new_catalog._loadConfig()
        assert new_catalog.config == {'cloudProject': '', 'defaultCloudBucket': '', 'cloudObjectPrefix': 'file'}

    def test_create_catalog(self, new_catalog):
        assert os.path.exists(new_catalog.catalogPath)
        assert os.path.exists(new_catalog.configPath)
        assert os.path.exists(new_catalog.catalogDbPath)
        assert os.path.exists(new_catalog.metadataCatalogPath)

    def test_load_catalog(self, tmp_path):
        catalog_dir = tmp_path / 'catalog_load_test'
        catalog = MediaCatalog(catalog_dir, create=True)
        config_path = catalog.configPath
        catalog.close()
        os.rename(config_path, config_path + '.bak')
        with pytest.raises(FileNotFoundError):
            catalog = MediaCatalog(catalog_dir)
        os.rename(config_path + '.bak', config_path)
        catalog = MediaCatalog(catalog_dir)

    def test_generate_catalog(self, sample_data_dir, tmp_path):
        catalog_dir = tmp_path / 'catalog_generate_test'
        catalog = MediaCatalog(catalog_dir, create=True)
        catalog.catalog(sample_data_dir)

        # Check that every file has been added to the catalog
        expectedMediaFiles = 12
        for albumDir in ['album1', 'album1_duplicate']:
            for root, dirs, files in os.walk(os.path.join(sample_data_dir, albumDir)):
                dirs.sort()
                for file in sorted(files):
                    if file == 'document_bad_extension.jpg':
                        continue
                    dbRecords, metadataAndPaths = catalog.query(filename=file, directory=root)
                    assert len(dbRecords) == 1
                    assert len(metadataAndPaths) == 1
        
        # Check that the catalog database has the expected number of records
        dbRecordCount = catalog.catalogDb.getFileCount()
        assert dbRecordCount == expectedMediaFiles

    def test_verify_local(self, sample_catalog):
        catalog = sample_catalog
        assert catalog.verify(local=True)
        assert catalog.verify(local=True, verifyChecksum=True)

    def test_query(self, sample_catalog, sample_data_dir):
        catalog = sample_catalog
        filename = 'IMG_0731.JPG'
        albumDir = os.path.join(sample_data_dir, 'album1')
        checksum = '69be5bae269d2142dc8b37e178af3fcad22ed4ca2c50c16d9fb44484e10d1723'

        # Query by checksum - there are two files with the same checksum
        dbRecords, metadataAndPaths = catalog.query(checksum=checksum)
        assert len(dbRecords) == 2
        assert len(metadataAndPaths) == 2
        for dbRecord, metadataAndPath in zip(dbRecords, metadataAndPaths):
            metadata, metadataPath = metadataAndPath
            assert dbRecord['file_name'] == filename
            assert dbRecord['checksum'] == checksum
            assert dbRecord['file_mime_type'] == 'image/jpeg'
            assert metadata['File:FileName'] == filename
            assert metadata['File:SHA256Sum'] == checksum
            assert metadata['File:MIMEType'] == 'image/jpeg'

        # Query by checksum and filename - there are two files with the same checksum and filename
        dbRecords, metadataAndPaths = catalog.query(checksum=checksum, filename=filename)
        assert len(dbRecords) == 2
        assert len(metadataAndPaths) == 2
        for dbRecord, metadataAndPath in zip(dbRecords, metadataAndPaths):
            metadata, metadataPath = metadataAndPath
            assert dbRecord['file_name'] == filename
            assert dbRecord['checksum'] == checksum
            assert dbRecord['file_mime_type'] == 'image/jpeg'
            assert metadata['File:FileName'] == filename
            assert metadata['File:SHA256Sum'] == checksum
            assert metadata['File:MIMEType'] == 'image/jpeg'

        # Query by checksum, filename, and directory
        dbRecords, metadataAndPaths = catalog.query(checksum=checksum, filename=filename, directory=albumDir)
        assert len(dbRecords) == 1
        assert len(metadataAndPaths) == 1
        dbRecord = dbRecords[0]
        metadata, metadataPath = metadataAndPaths[0]
        assert dbRecord['file_name'] == filename
        assert dbRecord['directory'] == albumDir
        assert dbRecord['checksum'] == checksum
        assert dbRecord['file_mime_type'] == 'image/jpeg'
        assert metadata['File:FileName'] == filename
        assert metadata['File:Directory'] == albumDir
        assert metadata['File:SHA256Sum'] == checksum
        assert metadata['File:MIMEType'] == 'image/jpeg'

        # Query by filename - there are two files with the same filename
        dbRecords, metadataAndPaths = catalog.query(filename=filename)
        assert len(dbRecords) == 2
        # assert len(metadataAndPaths) == 2
        for dbRecord, metadataAndPath in zip(dbRecords, metadataAndPaths):
            metadata, metadataPath = metadataAndPath
            assert dbRecord['file_name'] == filename
            assert dbRecord['checksum'] == checksum
            assert dbRecord['file_mime_type'] == 'image/jpeg'
            assert metadata['File:FileName'] == filename
            assert metadata['File:SHA256Sum'] == checksum
            assert metadata['File:MIMEType'] == 'image/jpeg'
        
        # Query by filename and directory
        dbRecords, metadataAndPaths = catalog.query(filename=filename, directory=albumDir)
        assert len(dbRecords) == 1
        assert len(metadataAndPaths) == 1
        dbRecord = dbRecords[0]
        metadata, metadataPath = metadataAndPaths[0]
        assert dbRecord['file_name'] == filename
        assert dbRecord['directory'] == albumDir
        assert dbRecord['checksum'] == checksum
        assert dbRecord['file_mime_type'] == 'image/jpeg'
        assert metadata['File:FileName'] == filename
        assert metadata['File:Directory'] == albumDir
        assert metadata['File:SHA256Sum'] == checksum
        assert metadata['File:MIMEType'] == 'image/jpeg'

    def test_checksum(self, new_catalog, tmp_path):
        test_file = tmp_path / 'test.txt'
        with open(test_file, 'w') as f:
            f.write('test\n')
        checksum = new_catalog.checksum(test_file)
        assert len(checksum) == 64  # SHA256 checksum should be 64 characters long
        assert checksum == 'f2ca1bb6c7e907d06dafe4687e579fce76b37e4e93b7605022da52e6ccc26fd2'

    def test_invalid_checksum_mode(self, new_catalog):
        with pytest.raises(ValueError):
            new_catalog._checksum('test.txt', 'INVALID')

import pytest
import os
import yaml
from mediaCatalog.mediaCatalog import MediaCatalog

class TestMediaCatalog:
    @pytest.fixture
    def catalog_dir(self, tmp_path):
        return tmp_path / 'catalog'
    
    @pytest.fixture
    def catalog(self, catalog_dir):
        catalog = MediaCatalog(catalog_dir, create=True)
        return catalog

    def test_init(self, catalog, catalog_dir):
        assert catalog.catalogPath == catalog_dir
        assert catalog.createMode == False
        assert catalog.updateMode == False
        assert catalog.configPath == os.path.join(catalog_dir, 'config.yaml')
        assert catalog.catalogDbPath == os.path.join(catalog_dir, 'catalog.db')
        assert catalog.metadataCatalogPath == os.path.join(catalog_dir, 'metadata')
        assert catalog.checksumKey == 'File:SHA256Sum'

    def test_create_config(self, catalog):
        catalog._createConfig()
        assert os.path.exists(catalog.configPath)
        assert catalog.config == {'project': '', 'defaultBucket': ''}
        assert catalog.config == yaml.safe_load(open(catalog.configPath, 'r'))

    def test_load_config(self, catalog):
        catalog._createConfig()
        catalog._loadConfig()
        assert catalog.config == {'project': '', 'defaultBucket': ''}

    def test_create_catalog(self, catalog):
        assert os.path.exists(catalog.catalogPath)
        assert os.path.exists(catalog.configPath)
        assert os.path.exists(catalog.catalogDbPath)
        assert os.path.exists(catalog.metadataCatalogPath)

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

    def test_checksum(self, catalog, tmp_path):
        test_file = tmp_path / 'test.txt'
        with open(test_file, 'w') as f:
            f.write('test\n')
        checksum = catalog.checksum(test_file)
        checksum = catalog._checksum(test_file, 'SHA256')
        assert len(checksum) == 64  # SHA256 checksum should be 64 characters long
        assert checksum == 'f2ca1bb6c7e907d06dafe4687e579fce76b37e4e93b7605022da52e6ccc26fd2'

    def test_invalid_checksum_mode(self, catalog):
        with pytest.raises(ValueError):
            catalog._checksum('test.txt', 'INVALID')

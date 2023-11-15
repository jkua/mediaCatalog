import pytest
import os
import shutil
import yaml
import json
from mediaCatalog.metadataCatalogHDT import MetadataCatalogHDT
from mediaCatalog.mediaCatalog import MediaCatalog

class TestMetadataCatalogHDT:
    @pytest.fixture
    def sample_data_dir(self, tmp_path):
        temp_sample_dir = tmp_path / 'sample'
        shutil.copytree(os.path.join(os.path.dirname(__file__), '..', 'data/sample'), temp_sample_dir)
        return temp_sample_dir
    
    def test_init(self, tmp_path):
        path = tmp_path / 'metadata_init_test'
        
        for hashMode in MetadataCatalogHDT.VALID_HASH_MODES:
            shutil.rmtree(path, ignore_errors=True)
            metadataCatalog = MetadataCatalogHDT(path, hashMode, createPath=True)

        # Pass an invalid hash mode - should raise an exception
        with pytest.raises(ValueError):
            shutil.rmtree(path, ignore_errors=True)
            metadataCatalog = MetadataCatalogHDT(path, 'INVALID', createPath=True)

        # Path does not exist - should raise an exception
        shutil.rmtree(path, ignore_errors=True)
        with pytest.raises(ValueError):
            metadataCatalog = MetadataCatalogHDT(path, hashMode)

    def test_write(self, tmp_path, sample_data_dir):
        path = tmp_path / 'metadata_write_test'
        hashMode = 'SHA256'

        shutil.rmtree(path, ignore_errors=True)
        metadataCatalog = MetadataCatalogHDT(path, hashMode, createPath=True)
        
        dataRootPath = os.path.join(sample_data_dir, 'album1')
        paths = os.listdir(dataRootPath)
        paths.sort()

        for path in paths:
            if os.path.isfile(os.path.join(dataRootPath, path)):
                metadata = MediaCatalog._getMetadata(os.path.join(dataRootPath, path))[0]
                metadata = MediaCatalog._addAdditionalMetadata(metadata)
                metadataPath = metadataCatalog.write(metadata)

                # Check that the metadata was written correctly
                assert os.path.exists(metadataPath)
                assert metadata == json.loads(open(metadataPath, 'rt').read())

                # Writing the same metadata again should fail
                assert not metadataCatalog.write(metadata)

                # Writing the same metadata again with updateMode=True should succeed and return the same path
                assert metadataCatalog.write(metadata, updateMode=True) == metadataPath

                # Only test one file
                break

    def test_exists(self, tmp_path, sample_data_dir):
        catalogPath = tmp_path / 'metadata_exists_test'
        hashMode = 'SHA256'

        shutil.rmtree(catalogPath, ignore_errors=True)
        metadataCatalog = MetadataCatalogHDT(catalogPath, hashMode, createPath=True)
        
        dataRootPath1 = os.path.join(sample_data_dir, 'album1')
        paths = os.listdir(dataRootPath1)
        paths.sort()

        for path in paths:
            fullPath = os.path.join(dataRootPath1, path)
            if os.path.isfile(fullPath):
                metadata1 = MediaCatalog._getMetadata(fullPath)[0]
                metadata1 = MediaCatalog._addAdditionalMetadata(metadata1)
                metadataPath1 = metadataCatalog.write(metadata1)
                print(f'MetadataPath1: {metadataPath1}')

                # Only test one file
                break

        # Add duplicate file in a different directory
        dataRootPath2 = os.path.join(sample_data_dir, 'album1_duplicate')
        path = os.path.join(dataRootPath2, path)
        metadata2 = MediaCatalog._getMetadata(os.path.join(dataRootPath2, path))[0]
        metadata2 = MediaCatalog._addAdditionalMetadata(metadata2)
        metadataPath2 = metadataCatalog.write(metadata2)
        print(f'MetadataPath2: {metadataPath2}')

        assert metadata1[metadataCatalog.hashKey] == metadata2[metadataCatalog.hashKey]
        assert metadata1[metadataCatalog.filenameKey] == metadata2[metadataCatalog.filenameKey]
        assert metadata1[metadataCatalog.directoryKey] != metadata2[metadataCatalog.directoryKey]
        assert metadata1[metadataCatalog.hostnameKey] == metadata2[metadataCatalog.hostnameKey]

        checksum = metadata1[metadataCatalog.hashKey]
        filename = metadata1[metadataCatalog.filenameKey]
        directory1 = metadata1[metadataCatalog.directoryKey]
        directory2 = metadata2[metadataCatalog.directoryKey]
        hostname = metadata1[metadataCatalog.hostnameKey]
        print(f'Checksum: {checksum}')
        print(f'Filename: {filename}')
        print(f'Directory1: {directory1}')
        print(f'Directory2: {directory2}')
        print(f'Hostname: {hostname}')
        assert metadataCatalog.exists(checksum) == 2
        assert metadataCatalog.exists(checksum, filename=filename) == 2
        assert metadataCatalog.exists(checksum, filename=filename, directory=directory1) == 1
        assert metadataCatalog.exists(checksum, filename=filename, directory=directory2) == 1
        assert metadataCatalog.exists(checksum, filename=filename, directory=directory1, hostname=hostname) == 1
        assert metadataCatalog.exists(checksum, filename=filename, directory=directory2, hostname=hostname) == 1
        assert metadataCatalog.exists(checksum, filename=filename, hostname=hostname) == 2
        assert metadataCatalog.exists(checksum, directory=directory1) == 1
        assert metadataCatalog.exists(checksum, directory=directory2) == 1
        assert metadataCatalog.exists(checksum, directory=directory1, hostname=hostname) == 1
        assert metadataCatalog.exists(checksum, directory=directory2, hostname=hostname) == 1
        assert metadataCatalog.exists(checksum, hostname=hostname) == 2

    def test_getMetadataPath(self, tmp_path, sample_data_dir):
        pass

    def test_read(self, tmp_path, sample_data_dir):
        pass

    def test_delete(self, tmp_path, sample_data_dir):
        pass


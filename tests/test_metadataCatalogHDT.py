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
    
    @pytest.fixture
    def new_catalog(self, tmp_path):
        catalogPath = tmp_path / 'metadata'
        hashMode = 'SHA256'

        metadataCatalog = MetadataCatalogHDT(catalogPath, hashMode, createPath=True)
        return metadataCatalog

    def test_init(self, tmp_path):
        path = tmp_path / 'metadata'
        
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

    def test_write(self, new_catalog, sample_data_dir):
        metadataCatalog = new_catalog
        
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

    def test_write_duplicate(self, new_catalog, sample_data_dir):
        metadataCatalog = new_catalog
        
        dataRootPath1 = os.path.join(sample_data_dir, 'album1')
        paths = os.listdir(dataRootPath1)
        paths.sort()

        for path in paths:
            fullPath = os.path.join(dataRootPath1, path)
            if os.path.isfile(fullPath):
                metadata1 = MediaCatalog._getMetadata(fullPath)[0]
                metadata1 = MediaCatalog._addAdditionalMetadata(metadata1)
                metadataPath1 = metadataCatalog.write(metadata1)

                # Check that the metadata was written correctly
                assert os.path.exists(metadataPath1)
                metadata1_rb = json.loads(open(metadataPath1, 'rt').read())
                assert metadata1_rb == metadata1

                # Only test one file
                break

        # Add duplicate file in a different directory
        dataRootPath2 = os.path.join(sample_data_dir, 'album1_duplicate')
        path = os.path.join(dataRootPath2, path)
        metadata2 = MediaCatalog._getMetadata(os.path.join(dataRootPath2, path))[0]
        metadata2 = MediaCatalog._addAdditionalMetadata(metadata2)
        metadataPath2 = metadataCatalog.write(metadata2)
        # Check that the metadata was written correctly
        assert os.path.exists(metadataPath2)
        metadata2_rb = json.loads(open(metadataPath2, 'rt').read())
        assert metadata2_rb == metadata2

        assert metadata1_rb != metadata2_rb
        assert metadataPath1 != metadataPath2

    def test_exists(self, new_catalog, sample_data_dir):
        metadataCatalog = new_catalog
        
        dataRootPath1 = os.path.join(sample_data_dir, 'album1')
        paths = os.listdir(dataRootPath1)
        paths.sort()

        for path in paths:
            fullPath = os.path.join(dataRootPath1, path)
            if os.path.isfile(fullPath):
                metadata1 = MediaCatalog._getMetadata(fullPath)[0]
                metadata1 = MediaCatalog._addAdditionalMetadata(metadata1)
                metadataPath1 = metadataCatalog.write(metadata1)

                # Only test one file
                break

        # Add duplicate file in a different directory
        dataRootPath2 = os.path.join(sample_data_dir, 'album1_duplicate')
        path = os.path.join(dataRootPath2, path)
        metadata2 = MediaCatalog._getMetadata(os.path.join(dataRootPath2, path))[0]
        metadata2 = MediaCatalog._addAdditionalMetadata(metadata2)
        metadataPath2 = metadataCatalog.write(metadata2)

        checksum = metadata1[metadataCatalog.hashKey]
        filename = metadata1[metadataCatalog.filenameKey]
        directory1 = metadata1[metadataCatalog.directoryKey]
        directory2 = metadata2[metadataCatalog.directoryKey]
        hostname = metadata1[metadataCatalog.hostnameKey]
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

    def test_getMetadataPath(self, new_catalog, sample_data_dir):
        metadataCatalog = new_catalog
        
        dataRootPath1 = os.path.join(sample_data_dir, 'album1')
        paths = os.listdir(dataRootPath1)
        paths.sort()

        for path in paths:
            fullPath = os.path.join(dataRootPath1, path)
            if os.path.isfile(fullPath):
                metadata1 = MediaCatalog._getMetadata(fullPath)[0]
                metadata1 = MediaCatalog._addAdditionalMetadata(metadata1)
                metadataPath1 = metadataCatalog.write(metadata1)

                # Only test one file
                break

        assert metadataCatalog.getMetadataPath(metadata1[metadataCatalog.hashKey], new=False) == metadataPath1
        head, ext = os.path.splitext(metadataPath1)
        newPath = head + '-01.json'
        assert metadataCatalog.getMetadataPath(metadata1[metadataCatalog.hashKey]) == newPath

        # Add duplicate file in a different directory
        dataRootPath2 = os.path.join(sample_data_dir, 'album1_duplicate')
        path = os.path.join(dataRootPath2, path)
        metadata2 = MediaCatalog._getMetadata(os.path.join(dataRootPath2, path))[0]
        metadata2 = MediaCatalog._addAdditionalMetadata(metadata2)
        metadataPath2 = metadataCatalog.write(metadata2)

        assert metadataCatalog.getMetadataPath(metadata2[metadataCatalog.hashKey], new=False) == metadataPath1
        assert metadataPath2 == newPath
        newPath2 = head + '-02.json'
        assert metadataCatalog.getMetadataPath(metadata2[metadataCatalog.hashKey]) == newPath2

    def test_read(self, new_catalog, sample_data_dir):
        metadataCatalog = new_catalog
        
        dataRootPath1 = os.path.join(sample_data_dir, 'album1')
        paths = os.listdir(dataRootPath1)
        paths.sort()

        for path in paths:
            fullPath = os.path.join(dataRootPath1, path)
            if os.path.isfile(fullPath):
                metadata1 = MediaCatalog._getMetadata(fullPath)[0]
                metadata1 = MediaCatalog._addAdditionalMetadata(metadata1)
                metadataPath1 = metadataCatalog.write(metadata1)

                # Only test one file
                break

        # Add duplicate file in a different directory
        dataRootPath2 = os.path.join(sample_data_dir, 'album1_duplicate')
        path = os.path.join(dataRootPath2, path)
        metadata2 = MediaCatalog._getMetadata(os.path.join(dataRootPath2, path))[0]
        metadata2 = MediaCatalog._addAdditionalMetadata(metadata2)
        metadataPath2 = metadataCatalog.write(metadata2)

        checksum = metadata1[metadataCatalog.hashKey]
        filename = metadata1[metadataCatalog.filenameKey]
        directory1 = metadata1[metadataCatalog.directoryKey]
        directory2 = metadata2[metadataCatalog.directoryKey]
        hostname = metadata1[metadataCatalog.hostnameKey]

        # Read by checksum with one result
        output = metadataCatalog.read(checksum)
        print(type(output))
        assert type(output) is tuple and len(output) == 2
        assert type(output[0]) is dict and type(output[1]) is str
        assert (output[0] == metadata1) ^ (output[0] == metadata2)
        assert (output[1] == metadataPath1) ^ (output[1] == metadataPath2)

        # Read with invalid checksum
        output = metadataCatalog.read('INVALID')
        assert type(output) is tuple and len(output) == 2
        assert output[0] is None and output[1] is None

        # Read by checksum with multiple results
        output = metadataCatalog.read(checksum, all=True)
        assert type(output) is list and len(output) == 2
        for md, mdPath in output:
            assert type(md) is dict and type(mdPath) is str
            assert (md == metadata1) ^ (md == metadata2)
            assert (mdPath == metadataPath1) ^ (mdPath == metadataPath2)

        # Read by checksum and filename with one result
        output = metadataCatalog.read(checksum, filename=filename)
        assert type(output) is tuple and len(output) == 2
        assert type(output[0]) is dict and type(output[1]) is str
        assert (output[0] == metadata1) ^ (output[0] == metadata2)
        assert (output[1] == metadataPath1) ^ (output[1] == metadataPath2)

        # Read by checksum and invalid filename
        output = metadataCatalog.read(checksum, filename='INVALID')
        assert type(output) is tuple and len(output) == 2
        assert output[0] is None and output[1] is None

        # Read by checksum and filename with multiple results
        output = metadataCatalog.read(checksum, filename=filename, all=True)
        assert type(output) is list and len(output) == 2
        for md, mdPath in output:
            assert type(md) is dict and type(mdPath) is str
            assert (md == metadata1) ^ (md == metadata2)
            assert (mdPath == metadataPath1) ^ (mdPath == metadataPath2)

        # Read by checksum, filename, and directory with one result
        output = metadataCatalog.read(checksum, filename=filename, directory=dataRootPath1)
        assert type(output) is tuple and len(output) == 2
        assert type(output[0]) is dict and type(output[1]) is str
        assert (output[0] == metadata1) and (output[1] == metadataPath1)

        # Read by checksum, filename, and directory with one result
        output = metadataCatalog.read(checksum, filename=filename, directory=dataRootPath2)
        assert type(output) is tuple and len(output) == 2
        assert type(output[0]) is dict and type(output[1]) is str
        assert (output[0] == metadata2) and (output[1] == metadataPath2)

        # Read by checksum, filename, and invalid directory
        output = metadataCatalog.read(checksum, filename=filename, directory='INVALID')
        assert type(output) is tuple and len(output) == 2
        assert output[0] is None and output[1] is None

        # Read by checksum, filename, directory, and hostname with one result
        output = metadataCatalog.read(checksum, filename=filename, directory=dataRootPath1, hostname=hostname)
        assert type(output) is tuple and len(output) == 2
        assert type(output[0]) is dict and type(output[1]) is str
        assert (output[0] == metadata1) and (output[1] == metadataPath1)

        # Read by checksum, filename, directory, and hostname with one result
        output = metadataCatalog.read(checksum, filename=filename, directory=dataRootPath2, hostname=hostname)
        assert type(output) is tuple and len(output) == 2
        assert type(output[0]) is dict and type(output[1]) is str
        assert (output[0] == metadata2) and (output[1] == metadataPath2)

        # Read by checksum, filename, directory, and invalid hostname
        output = metadataCatalog.read(checksum, filename=filename, directory=dataRootPath1, hostname='INVALID')
        assert type(output) is tuple and len(output) == 2
        assert output[0] is None and output[1] is None

        # Read by checksum and directory with one result
        output = metadataCatalog.read(checksum, directory=dataRootPath1)
        assert type(output) is tuple and len(output) == 2
        assert type(output[0]) is dict and type(output[1]) is str
        assert (output[0] == metadata1) and (output[1] == metadataPath1)

        # Read by checksum and directory with one result
        output = metadataCatalog.read(checksum, directory=dataRootPath2)
        assert type(output) is tuple and len(output) == 2
        assert type(output[0]) is dict and type(output[1]) is str
        assert (output[0] == metadata2) and (output[1] == metadataPath2)

        # Read by checksum, directory, and invalid hostname
        output = metadataCatalog.read(checksum, directory=dataRootPath1, hostname='INVALID')
        assert type(output) is tuple and len(output) == 2
        assert output[0] is None and output[1] is None

        # Read by checksum, directory, and hostname with one result
        output = metadataCatalog.read(checksum, directory=dataRootPath1, hostname=hostname)
        assert type(output) is tuple and len(output) == 2
        assert type(output[0]) is dict and type(output[1]) is str
        assert (output[0] == metadata1) and (output[1] == metadataPath1)

        # Read by checksum, directory, and hostname with one result
        output = metadataCatalog.read(checksum, directory=dataRootPath2, hostname=hostname)
        assert type(output) is tuple and len(output) == 2
        assert type(output[0]) is dict and type(output[1]) is str
        assert (output[0] == metadata2) and (output[1] == metadataPath2)

        # Read by checksum and hostname with multiple results
        output = metadataCatalog.read(checksum, hostname=hostname, all=True)
        assert type(output) is list and len(output) == 2
        for md, mdPath in output:
            assert type(md) is dict and type(mdPath) is str
            assert (md == metadata1) ^ (md == metadata2)
            assert (mdPath == metadataPath1) ^ (mdPath == metadataPath2)

        # Read by checksum and invalid hostname
        output = metadataCatalog.read(checksum, hostname='INVALID')
        assert type(output) is tuple and len(output) == 2
        assert output[0] is None and output[1] is None

    def test_delete(self, new_catalog, sample_data_dir):
        metadataCatalog = new_catalog
        
        dataRootPath = os.path.join(sample_data_dir, 'album1')
        paths = os.listdir(dataRootPath)
        paths.sort()

        metadata = []
        metadataPaths = []
        for i, path in enumerate(paths):
            fullPath = os.path.join(dataRootPath, path)
            if os.path.isfile(fullPath):
                md = MediaCatalog._getMetadata(fullPath)[0]
                md = MediaCatalog._addAdditionalMetadata(md)
                mdPath = metadataCatalog.write(md)
                metadata.append(md)
                metadataPaths.append(mdPath)

        # Delete metadata one by one, verifying that the metadata is deleted and the rest remain
        for i, (md, mdPath) in enumerate(zip(metadata, metadataPaths)):
            assert metadataCatalog.delete(md[metadataCatalog.hashKey]) == 1
            assert not os.path.exists(mdPath)
            for remainPath in metadataPaths[i+1:]:
                assert os.path.exists(remainPath)

        assert len(os.listdir(metadataCatalog.path)) == 0            

    def test_delete_duplicates(self, new_catalog, sample_data_dir):
        metadataCatalog = new_catalog
        
        dataRootPath1 = os.path.join(sample_data_dir, 'album1')
        paths = os.listdir(dataRootPath1)
        paths.sort()

        otherMdPaths = []
        for i, path in enumerate(paths):
            fullPath = os.path.join(dataRootPath1, path)
            if os.path.isfile(fullPath):
                if i == 0:
                    metadata1 = MediaCatalog._getMetadata(fullPath)[0]
                    metadata1 = MediaCatalog._addAdditionalMetadata(metadata1)
                    metadataPath1 = metadataCatalog.write(metadata1)
                    firstPath = path
                else:
                    md = MediaCatalog._getMetadata(fullPath)[0]
                    md = MediaCatalog._addAdditionalMetadata(md)
                    otherMdPath = metadataCatalog.write(md)
                    otherMdPaths.append(otherMdPath)

        # Add duplicate file in a different directory
        dataRootPath2 = os.path.join(sample_data_dir, 'album1_duplicate')
        path = os.path.join(dataRootPath2, firstPath)
        metadata2 = MediaCatalog._getMetadata(os.path.join(dataRootPath2, path))[0]
        metadata2 = MediaCatalog._addAdditionalMetadata(metadata2)
        metadataPath2 = metadataCatalog.write(metadata2)

        # Delete metadata 1
        assert metadataCatalog.delete(metadata1[metadataCatalog.hashKey], 
                                filename=metadata1[metadataCatalog.filenameKey], 
                                directory=dataRootPath1, 
                                hostname=metadata1[metadataCatalog.hostnameKey])

        # Check that metadata 1 was deleted
        assert not os.path.exists(metadataPath1)
        # Check that metadata 2 was not deleted
        assert os.path.exists(metadataPath2)

        # Delete metadata 2
        assert metadataCatalog.delete(metadata2[metadataCatalog.hashKey], 
                                filename=metadata2[metadataCatalog.filenameKey], 
                                directory=dataRootPath2, 
                                hostname=metadata2[metadataCatalog.hostnameKey])

        # Check that metadata 2 was deleted
        assert not os.path.exists(metadataPath2)

        # Check that the other metadata was not deleted
        for otherMdPath in otherMdPaths:
            assert os.path.exists(otherMdPath)       

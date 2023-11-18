import pytest
import os
import yaml
from mediaCatalog.mediaCatalog import MediaCatalog
from mediaCatalog.googleCloudStorage import GoogleCloudStorage
from mediaCatalog.cloudUploader import CloudUploader

class TestCloudUpload:
    @pytest.fixture
    def catalog_dir(self, tmp_path):
        return tmp_path / 'catalog'

    @pytest.fixture
    def sample_data_dir(self):
        return os.path.join(os.path.dirname(__file__), '..', 'data/sample')
    
    def test_upload(self, catalog_dir, sample_data_dir, tmp_path):
        catalog = MediaCatalog(catalog_dir, create=True)
        catalog.catalog(sample_data_dir)
        with open(os.path.join(os.path.dirname(__file__), 'config.yaml'), 'r') as f:
            catalog.config = yaml.safe_load(f)
        
        cloudStorage = GoogleCloudStorage(catalog.config['cloudProject'], catalog.config['defaultCloudBucket'])
        # Wipe the bucket clean before uploading
        cloudFiles = cloudStorage.listFiles(prefix=catalog.config['cloudObjectPrefix']+'/')
        for cloudFile in cloudFiles:
            cloudStorage.deleteFile(cloudFile)

        uploader = CloudUploader(catalog, cloudStorage)
        uploader.upload()

        # Validate that the files were uploaded correctly and that the catalog was updated correctly
        records = catalog.catalogDb.read(all=True)
        for record in records:
            filename = record['file_name']
            directory = record['directory']
            mimeType = record['file_mime_type']
            sourcePath = os.path.join(directory, filename)
            projectId = record['cloud_name']
            bucketName = record['cloud_bucket']
            objectName = record['cloud_object_name']
            assert projectId == catalog.config['cloudProject']
            assert bucketName == catalog.config['defaultCloudBucket']
            assert objectName == os.path.join(catalog.config['cloudObjectPrefix'], record['checksum'])
            assert cloudStorage.validateFile(objectName, sourcePath=sourcePath)
            assert cloudStorage.getMimeType(objectName) == mimeType

        # Validate the last file uploaded by downloading it and comparing it to the original
        downloadPath = tmp_path / 'cloud_download_test'
        cloudStorage.downloadFile(objectName, downloadPath)
        assert catalog.checksum(downloadPath) == catalog.checksum(sourcePath)

        # Use MediaCatalog.verify() to verify the files in the cloud
        assert catalog.verify(cloudStorage=cloudStorage)
        assert catalog.verify(cloudStorage=cloudStorage, verifyChecksum=True)

        # Delete the last file from the cloud - both should fail
        cloudStorage.deleteFile(objectName)
        assert not catalog.verify(cloudStorage=cloudStorage)
        assert not catalog.verify(cloudStorage=cloudStorage, verifyChecksum=True)

        # Upload the last file with a bad checksum
        data = open(sourcePath, 'rb').read()
        corruptFile = tmp_path / 'corrupt_file'
        with open(corruptFile, 'wb') as f:
            f.write(data[::-1])
        cloudStorage.uploadFile(corruptFile, objectName, mimeType)
        # This will also fail because we always verify cloud checksums since we store them in the catalog
        assert not catalog.verify(cloudStorage=cloudStorage)
        assert not catalog.verify(cloudStorage=cloudStorage, verifyChecksum=True)

        # Restore the last file
        cloudStorage.uploadFile(os.path.join(directory, filename), objectName, mimeType)
        assert catalog.verify(cloudStorage=cloudStorage)
        assert catalog.verify(cloudStorage=cloudStorage, verifyChecksum=True)


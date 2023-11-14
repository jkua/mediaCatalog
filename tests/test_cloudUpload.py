import pytest
import os
import yaml
from mediaCatalog.mediaCatalog import MediaCatalog
from mediaCatalog.googleCloudStorage import GoogleCloudStorage
from mediaCatalog.cloudUploader import CloudUploader

class TestMediaCatalog:
    @pytest.fixture
    def catalog_dir(self, tmp_path):
        return tmp_path / 'catalog'

    @pytest.fixture
    def sample_data_dir(self):
        return os.path.join(os.path.dirname(__file__), '..', 'data/sample')
    
    def test_upload(self, catalog_dir, sample_data_dir):
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
            assert cloudStorage.validateFile(objectName, sourcePath)
            assert cloudStorage.getMimeType(objectName) == mimeType

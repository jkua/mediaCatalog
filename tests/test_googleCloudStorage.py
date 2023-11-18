import pytest
import os
import yaml
import datetime
from mediaCatalog.mediaCatalog import MediaCatalog
from mediaCatalog.googleCloudStorage import GoogleCloudStorage
from mediaCatalog.cloudStorage import (CloudStorage, 
                                    CloudStorageObjectMissingException, 
                                    CloudStorageObjectSizeMismatchException, 
                                    CloudStorageObjectChecksumMismatchException)

class TestGoogleCloudStorage:
    @pytest.fixture
    def catalog_dir(self, tmp_path):
        return tmp_path / 'catalog'

    @pytest.fixture
    def sample_data_dir(self):
        return os.path.join(os.path.dirname(__file__), '..', 'data/sample')

    @pytest.fixture
    def new_cloud_storage(self):
        config = yaml.safe_load(open(os.path.join(os.path.dirname(__file__), 'config.yaml'), 'r'))
        cloudStorage = GoogleCloudStorage(config['cloudProject'], config['defaultCloudBucket'])
        return cloudStorage

    def create_test_bucket(self, cloudStorage):
        bucketName = f'mediacatalog_test_cloud_storage-{datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S_%f")}'
        bucketList = cloudStorage.listBuckets()
        assert bucketName not in bucketList
        cloudStorage.createBucket(bucketName)
        return bucketName
    
    def test_init(self):
        cloudStorage = GoogleCloudStorage('testProject', 'testBucket')
        assert cloudStorage.projectId == 'testProject'
        assert cloudStorage.bucketName == 'testBucket'

        # Missing projectId
        with pytest.raises(ValueError):
            cloudStorage = GoogleCloudStorage(None, 'testBucket')

        with pytest.raises(ValueError):
            cloudStorage = GoogleCloudStorage('', 'testBucket')

        # Missing bucketName
        with pytest.raises(ValueError):
            cloudStorage = GoogleCloudStorage('testProject', None)

        with pytest.raises(ValueError):
            cloudStorage = GoogleCloudStorage('testProject', '')

    def test_bucket_ops(self, new_cloud_storage):
        cloudStorage = new_cloud_storage
        
        bucketName = self.create_test_bucket(cloudStorage)

        # Confirm bucket created
        bucketList = cloudStorage.listBuckets()
        assert bucketName in bucketList

        # Get exception if we try to create a bucket that already exists
        with pytest.raises(Exception):
            cloudStorage.createBucket(bucketName)

        # Delete bucket
        cloudStorage.deleteBucket(bucketName)

        # Confirm bucket deleted
        bucketList = cloudStorage.listBuckets()
        assert bucketName not in bucketList

        # Get exception if we try to delete a bucket that doesn't exist
        with pytest.raises(Exception):
            cloudStorage.deleteBucket(bucketName)

    def test_file_ops(self, new_cloud_storage, sample_data_dir, tmp_path):
        cloudStorage = new_cloud_storage
        filename = 'IMG_0731.JPG'
        albumDir = os.path.join(sample_data_dir, 'album1')
        mimeType = 'image/jpeg'
        checksum = '69be5bae269d2142dc8b37e178af3fcad22ed4ca2c50c16d9fb44484e10d1723'
        objectName = 'file/' + checksum
        cloudChecksum = 1060211233
        sourcePath = os.path.join(albumDir, filename)
        downloadPath = tmp_path / 'cloud_download_test'

        bucketName = self.create_test_bucket(cloudStorage)
        cloudStorage.setBucket(bucketName)

        # Should get an empty file list
        assert not cloudStorage.listFiles()

        # Get validation exception for a file that doesn't exist
        with pytest.raises(CloudStorageObjectMissingException):
            cloudStorage.validateFile(objectName)

        # Test checksum computation
        assert cloudStorage.computeChecksum(sourcePath) == cloudChecksum

        # Upload a file
        cloudStorage.uploadFile(sourcePath, objectName, mimeType)

        # Confirm file uploaded
        assert cloudStorage.fileExists(objectName)
        fileList = cloudStorage.listFiles()
        assert objectName in fileList

        # Get validation exception for bad file size
        with pytest.raises(CloudStorageObjectSizeMismatchException):
            cloudStorage.validateFile(objectName, fileSize=-1)

        # Get validation exception for bad checksum
        with pytest.raises(CloudStorageObjectChecksumMismatchException):
            cloudStorage.validateFile(objectName, checksum=-1)

        # No exception if we try to upload a file that already exists
        cloudStorage.uploadFile(sourcePath, objectName, mimeType)

        # Get MIME type
        assert cloudStorage.getMimeType(objectName) == mimeType

        # Get checksum
        assert cloudStorage.getChecksum(objectName) == cloudChecksum

        # Validate file
        fileSize = os.stat(sourcePath).st_size
        assert cloudStorage.validateFile(objectName, fileSize=fileSize)
        assert cloudStorage.validateFile(objectName, checksum=cloudChecksum)
        assert cloudStorage.validateFile(objectName, fileSize=fileSize, checksum=cloudChecksum)
        assert cloudStorage.validateFile(objectName, sourcePath=sourcePath)
        
        # Download a file
        cloudStorage.downloadFile(objectName, downloadPath)

        # Confirm file downloaded
        assert os.path.exists(downloadPath)
        assert cloudStorage.computeChecksum(downloadPath) == cloudChecksum

        # Delete a file
        cloudStorage.deleteFile(objectName)

        # Confirm file deleted
        assert checksum not in cloudStorage.listFiles()

        # Get exception if we try to delete a file that doesn't exist
        with pytest.raises(Exception):
            cloudStorage.deleteFile(objectName)

        # Clean up - delete bucket
        cloudStorage.deleteBucket(bucketName)

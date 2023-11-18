import os
import base64
import struct

from google.cloud import storage
import google_crc32c

from .cloudStorage import CloudStorage, CloudStorageObjectMissingException, CloudStorageObjectSizeMismatchException, CloudStorageObjectChecksumMismatchException

class GoogleCloudStorage(CloudStorage):
    TRANSFER_CHECKSUM = 'crc32c'
    def __init__(self, projectId, bucketName):
        if not projectId or not projectId.strip():
            raise ValueError('projectId is not set!')
        self.projectId = projectId
        CloudStorage.__init__(self, bucketName)
        
        self.storageClient = storage.Client(project=projectId)

        print(f'Initializing Google Cloud client with project {self.projectId} and bucket {self.bucketName}')

    def listBuckets(self):
        buckets = self.storageClient.list_buckets()
        return [bucket.name for bucket in buckets]

    def createBucket(self, bucketName):
        bucket_ = self.storageClient.create_bucket(bucketName)

    def deleteBucket(self, bucketName):
        bucket_ = self.storageClient.get_bucket(bucketName)
        bucket_.delete()

    def setBucket(self, bucketName):
        self.bucketName = bucketName

    def listFiles(self, prefix=None, delimiter=None):
        return self._listFiles(self.bucketName, prefix=prefix, delimiter=delimiter)

    def _listFiles(self, bucketName, prefix=None, delimiter=None):
        blobs = self.storageClient.list_blobs(bucketName, prefix=prefix, delimiter=delimiter)
        return [blob.name for blob in blobs]

    def _fileExists(self, bucketName, objectName):
        bucket = self.storageClient.bucket(bucketName)
        return bucket.blob(objectName).exists()

    def _validateFile(self, bucketName, objectName, fileSize=None, checksum=None, sourcePath=None):
        bucket = self.storageClient.bucket(bucketName)
        blob = bucket.blob(objectName)
        
        if not blob.exists():
            raise CloudStorageObjectMissingException(bucketName, objectName)
        
        blob.reload()

        if sourcePath:
            fileSize = os.stat(sourcePath).st_size
            checksum = self._computeCrc32c(sourcePath)

        if fileSize and blob.size != fileSize:
            raise CloudStorageObjectSizeMismatchException(bucketName, objectName, fileSize, blob.size)

        if checksum: 
            remoteChecksum = self._convertB64Crc32c(blob.crc32c)
            if remoteChecksum != checksum:
                raise CloudStorageObjectChecksumMismatchException(bucketName, objectName, checksum, remoteChecksum)

        return True

    def _downloadFile(self, bucketName, objectName, destinationPath):
        bucket = self.storageClient.bucket(bucketName)
        blob = bucket.blob(objectName)
        blob.download_to_filename(destinationPath, checksum=self.TRANSFER_CHECKSUM)

    def _uploadFile(self, sourcePath, bucketName, objectName, mimeType=None):
        bucket = self.storageClient.bucket(bucketName)
        blob = bucket.blob(objectName)

        blob.upload_from_filename(sourcePath, content_type=mimeType, checksum=self.TRANSFER_CHECKSUM)

    def _deleteFile(self, bucketName, objectName):
        bucket = self.storageClient.bucket(bucketName)
        blob = bucket.blob(objectName)
        generationMatchPrecondition = None

        blob.reload()  # Fetch blob metadata to use in generationMatchPrecondition.
        generationMatchPrecondition = blob.generation

        blob.delete(if_generation_match=generationMatchPrecondition)

    def _getSize(self, bucketName, objectName):
        bucket = self.storageClient.bucket(bucketName)
        blob = bucket.blob(objectName)
        blob.reload()
        return blob.size

    def _getMimeType(self, bucketName, objectName):
        bucket = self.storageClient.bucket(bucketName)
        blob = bucket.blob(objectName)
        blob.reload()
        return blob.content_type

    def _getChecksum(self, bucketName, objectName):
        bucket = self.storageClient.bucket(bucketName)
        blob = bucket.blob(objectName)
        blob.reload()
        return self._convertB64Crc32c(blob.crc32c)

    def _computeChecksum(self, sourcePath):
        return self._computeCrc32c(sourcePath)

    def _computeCrc32c(self, sourcePath):
        helper = google_crc32c.Checksum()
        with open(sourcePath, 'rb') as f:
            data = f.read()
        helper.update(data)
        return helper._crc

    def _convertB64Crc32c(self, b64encoded):
        return struct.unpack('>I', base64.b64decode(b64encoded))[0]
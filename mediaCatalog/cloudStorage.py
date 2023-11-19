class CloudStorageObjectMissingException(Exception):
    def __init__(self, bucketName, objectName):
        self.bucketName = bucketName
        self.objectName = objectName
        self.message = f'Object {objectName} does not exist in bucket {bucketName}!'
        super().__init__(self.message)

class CloudStorageObjectSizeMismatchException(Exception):
    def __init__(self, bucketName, objectName, expectedSize=None, actualSize=None):
        self.bucketName = bucketName
        self.objectName = objectName
        self.expectedSize = expectedSize
        self.actualSize = actualSize
        if expectedSize is None and actualSize is None:
            self.message = f'Object {objectName} in bucket {bucketName} has the wrong size!'
        else:
            self.message = f'Object {objectName} in bucket {bucketName} has size {actualSize} but expected size {expectedSize}!'
        super().__init__(self.message)

class CloudStorageObjectChecksumMismatchException(Exception):
    def __init__(self, bucketName, objectName, expectedChecksum=None, actualChecksum=None):
        self.bucketName = bucketName
        self.objectName = objectName
        self.expectedChecksum = expectedChecksum
        self.actualChecksum = actualChecksum
        if expectedChecksum is None and actualChecksum is None:
            self.message = f'Object {objectName} in bucket {bucketName} has the wrong checksum!'
        else:
            self.message = f'Object {objectName} in bucket {bucketName} has checksum {actualChecksum} but expected checksum {expectedChecksum}!'
        super().__init__(self.message)

class CloudStorage(object):
    def __init__(self, bucketName):
        if not bucketName or not bucketName.strip():
            raise ValueError('bucketName is not set!')
        self.bucketName = bucketName

    def listBuckets(self):
        raise NotImplementedError

    def createBucket(self, name):
        raise NotImplementedError

    def deleteBucket(self, name):
        raise NotImplementedError

    def setBucket(self, bucketName):
        raise NotImplementedError

    def listFiles(self, prefix=None, extended=False):
        ''' List the files in the cloud, optionally filtering by prefix.

            :param prefix: (str) The prefix to filter by
            :param extended: (bool) If True, return a list of tuples with (objectName, size, checksum)
            :returns: A list of object names or a list of tuples with (objectName, size, checksum)
        '''
        self._listFiles(self.bucketName, prefix, extended)

    def fileExists(self, objectName):
        return self._fileExists(self.bucketName, objectName)

    def validateFile(self, objectName, fileSize=None, checksum=None, sourcePath=None):
        ''' Validate that the file exists in the cloud.
            Optionally check that file size and checksums match provided values
            or those computed from a local file.

            :param objectName: The name of the file in the cloud
            :param fileSize: (int) The expected file size in bytes
            :param checksum: (str) The expected checksum
            :param sourcePath: (str) The path to a local file to get the file size and compute the checksum from
            :returns: True if the file exists and optionally matches the expected file size and checksum
        '''
        return self._validateFile(self.bucketName, objectName, fileSize, checksum, sourcePath)

    def downloadFile(self, objectName, destinationPath):
        self._downloadFile(self.bucketName, objectName, destinationPath)

    def uploadFile(self, sourcePath, objectName, mimeType=None):
        self._uploadFile(sourcePath, self.bucketName, objectName, mimeType)

    def deleteFile(self, objectName):
        self._deleteFile(self.bucketName, objectName)

    def getSize(self, objectName):
        return self._getSize(self.bucketName, objectName)

    def getMimeType(self, objectName):
        return self._getMimeType(self.bucketName, objectName)

    def getChecksum(self, objectName):
        return self._getChecksum(self.bucketName, objectName)

    def computeChecksum(self, sourcePath):
        return self._computeChecksum(sourcePath)

    def _listFiles(self, bucketName, prefix=None):
        raise NotImplementedError

    def _exists(self, bucketName, objectName):
        raise NotImplementedError

    def _downloadFile(self, bucketName, objectName, destinationPath):
        raise NotImplementedError

    def _uploadFile(self, sourcePath, bucketName, objectName, mimeType=None):
        raise NotImplementedError

    def _deleteFile(self, bucketName, objectName):
        raise NotImplementedError

    def _getSize(self, bucketName, objectName):
        raise NotImplementedError

    def _getMimeType(self, bucketName, objectName):
        raise NotImplementedError

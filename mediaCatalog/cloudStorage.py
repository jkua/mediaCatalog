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

    def listFiles(self, prefix=None):
        self._listFiles(self.bucketName, prefix)

    def fileExists(self, objectName):
        return self._fileExists(self.bucketName, objectName)

    def validateFile(self, objectName, sourcePath=None, checksum=None):
        return self._validateFile(self.bucketName, objectName, sourcePath, checksum)

    def downloadFile(self, objectName, destinationPath):
        self._downloadFile(self.bucketName, objectName, destinationPath)

    def uploadFile(self, sourcePath, objectName, mimeType=None):
        self._uploadFile(sourcePath, self.bucketName, objectName, mimeType)

    def deleteFile(self, objectName):
        self._deleteFile(self.bucketName, objectName)

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

    def _getMimeType(self, bucketName, objectName):
        raise NotImplementedError

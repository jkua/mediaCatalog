class CloudStorage(object):
    def __init__(self, bucketName=None):
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

    def validateFile(self, objectName, sourcePath):
        return self._validateFile(self.bucketName, objectName, sourcePath)

    def downloadFile(self, objectName, destinationPath):
        self._downloadFile(self.bucketName, objectName, destinationPath)

    def uploadFile(self, sourcePath, objectName):
        self._uploadFile(sourcePath, self.bucketName, objectName)

    def deleteFile(self, objectName):
        self._deleteFile(self.bucketName, objectName)

    def _listFiles(self, bucketName, prefix=None):
        raise NotImplementedError

    def _exists(self, bucketName, objectName):
        raise NotImplementedError

    def _downloadFile(self, bucketName, objectName, destinationPath):
        raise NotImplementedError

    def _uploadFile(self, sourcePath, bucketName, objectName):
        raise NotImplementedError

    def _deleteFile(self, bucketName, objectName):
        raise NotImplementedError


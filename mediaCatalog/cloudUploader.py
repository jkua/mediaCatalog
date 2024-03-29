import logging
import os

from .cloudStorage import CloudStorageObjectMissingException, CloudStorageObjectSizeMismatchException, CloudStorageObjectChecksumMismatchException

class CloudUploader(object):
	def __init__(self, catalog, cloudStorage):
		self.catalog = catalog
		self.cloudStorage = cloudStorage
		self.catalogDb = self.catalog.catalogDb

	def upload(self):
		totalFileCount = self.catalogDb.getFileCount()
		filesToUpload = self.catalogDb.getFilesNotInCloud()

		# Upload catalog database and config
		self.catalog.upload(self.cloudStorage)
		
		if not filesToUpload:
			print(f'\nAll {totalFileCount} files have already been uploaded to the cloud.')
			return

		notUploadedPercentage = len(filesToUpload)/totalFileCount*100
		print(f'\n{len(filesToUpload)}/{totalFileCount} ({notUploadedPercentage:.3f} %) files to be uploaded')

		uploadedFiles = []
		skippedFiles = []
		for i, (checksum, filename, directory, mimeType) in enumerate(filesToUpload, 1):
			sourcePath = os.path.join(directory, filename)
			objectName = os.path.join(self.catalog.config['cloudObjectPrefix'], checksum)

			uploadedPercentage = i/len(filesToUpload)*100
			print(f'[{i}/{len(filesToUpload)} ({uploadedPercentage:.3f} %)] {sourcePath} -> {objectName}')

			try:
				self.cloudStorage.validateFile(objectName, sourcePath=sourcePath)
			except CloudStorageObjectMissingException as e:
				self.cloudStorage.uploadFile(sourcePath, objectName, mimeType)
				uploadedFiles.append((sourcePath, objectName))
			else:
				print('    WARNING: Object already exists in the cloud!')
				print('    Checksums match. Skipping upload.')
				skippedFiles.append((sourcePath, objectName))
			
			objectChecksum = self.cloudStorage.getChecksum(objectName)
			self.catalogDb.setCloudStorage(checksum, 
											self.cloudStorage.projectId, 
											self.cloudStorage.bucketName, 
											objectName, 
											objectChecksum
											)
			self.catalogDb.commit()
			# self.catalogDb.printFileRecord(checksum)

		numProcessedFiles = len(uploadedFiles) + len(skippedFiles)
		print(f'\nUpload complete!')
		print('====================')
		print(f'Files processed: {numProcessedFiles}')
		print(f'Uploaded files: {len(uploadedFiles)}')
		print(f'Skipped files: {len(skippedFiles)}')
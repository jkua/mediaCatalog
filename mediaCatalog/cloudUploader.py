import logging
import os

class CloudUploader(object):
	def __init__(self, catalog, cloudStorage):
		self.catalog = catalog
		self.cloudStorage = cloudStorage
		self.catalogDb = self.catalog.catalogDb

	def upload(self):
		totalFileCount = self.catalogDb.getFileCount()
		filesToUpload = self.catalogDb.getFilesNotInCloud()

		
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
			
			if not self.cloudStorage.fileExists(objectName):
				self.cloudStorage.uploadFile(sourcePath, objectName, mimeType)
				uploadedFiles.append((sourcePath, objectName))
			else:
				print('    WARNING: Object already exists in the cloud!')
				if self.cloudStorage.validateFile(objectName, sourcePath):
					print('    Checksums match. Skipping upload.')
					skippedFiles.append((sourcePath, objectName))
				else:
					skippedFiles.append((sourcePath, None))
					raise Exception('Cloud object has a different checksum!')
			
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
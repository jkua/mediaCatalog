#!/usr/bin/env python3

import logging
import os

from mediaCatalog import MediaCatalog
from googleCloudStorage import GoogleCloudStorage

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

		
		for i, (checksum, filename, directory) in enumerate(filesToUpload, 1):
			sourcePath = os.path.join(directory, filename)
			objectName = os.path.join('file', checksum)

			uploadedPercentage = i/len(filesToUpload)*100
			print(f'[{i}/{len(filesToUpload)} ({uploadedPercentage:.3f} %)] {sourcePath} -> {objectName}')
			
			if not cloudStorage.fileExists(objectName):
				self.cloudStorage.uploadFile(sourcePath, objectName)
			else:
				print('    WARNING: Object already exists in the cloud!')
				if cloudStorage.validateFile(objectName, sourcePath):
					print('    Checksums match. Skipping upload.')
				else:
					raise Exception('Cloud object has a different checksum!')
			
			self.catalogDb.setCloudStorage(checksum, self.cloudStorage.projectId, self.cloudStorage.bucketName, objectName)
			self.catalogDb.commit()
			self.catalogDb.printFileRecord(checksum)


if __name__=='__main__':
	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument('--catalog', '-c', required=True, help='Path to catalog')
	args = parser.parse_args()

	with MediaCatalog(args.catalog) as catalog:
		cloudStorage = GoogleCloudStorage(catalog.config['project'], catalog.config['defaultBucket'])
		uploader = CloudUploader(catalog, cloudStorage)
		uploader.upload()


#!/usr/bin/env python3

import logging
import os

from catalogDatabase import CatalogDatabase
from googleCloudStorage import GoogleCloudStorage

class CloudUploader(object):
	CATALOG_DB_FILENAME = 'catalog.db'
	def __init__(self, catalogPath, cloudStorage):
		self.catalogPath = catalogPath
		self.cloudStorage = cloudStorage
		self.catalogDb = CatalogDatabase(os.path.join(self.catalogPath, self.CATALOG_DB_FILENAME))

	def upload(self):
		totalFileCount = self.catalogDb.getFileCount()
		filesToUpload = self.catalogDb.getFilesNotInCloud()

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
			self.catalogDb.printFileRecord(checksum)

		self.catalogDb.commit()
		self.catalogDb.close()


if __name__=='__main__':
	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument('--catalog', '-c', required=True, help='Path to catalog')
	parser.add_argument('--project', '-p', required=True, help='Cloud project ID')
	parser.add_argument('--bucket', '-b', required=True, help='Cloud bucket name')
	args = parser.parse_args()

	cloudStorage = GoogleCloudStorage(args.project, args.bucket)
	uploader = CloudUploader(args.catalog, cloudStorage)
	uploader.upload()


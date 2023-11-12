#!/usr/bin/env python3

import logging
import os

from mediaCatalog import MediaCatalog
from googleCloudStorage import GoogleCloudStorage

class CloudDownloader(object):
	def __init__(self, catalog, cloudStorage):
		self.catalog = catalog
		self.cloudStorage = cloudStorage
		self.catalogDb = self.catalog.catalogDb

	def download(self, checksum, destinationPath):
		record = self.catalogDb.read(checksum)
		self.catalogDb.printFileRecord(checksum)
		objectName = record['cloud_object_name']

		# TODO verify project and bucket (cloud_storage)

		if not cloudStorage.fileExists(objectName):
			raise Exception(f'Object {objectName} does not exist in bucket {self.cloudStorage.bucketName}!')
		
		fullDestinationPath = os.path.join(destinationPath, record['file_name'])
		self.cloudStorage.downloadFile(objectName, fullDestinationPath)


if __name__=='__main__':
	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument('--catalog', '-c', required=True, help='Path to catalog')
	parser.add_argument('checksum', help='Checksum of file to download')
	parser.add_argument('destination', help='Path to download file to')
	args = parser.parse_args()

	with MediaCatalog(args.catalog) as catalog:
		cloudStorage = GoogleCloudStorage(catalog.config['project'], catalog.config['defaultBucket'])
		downloader = CloudDownloader(catalog, cloudStorage)
		downloader.download(args.checksum, args.destination)

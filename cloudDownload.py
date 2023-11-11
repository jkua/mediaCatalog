#!/usr/bin/env python3

import logging
import os

from catalogDatabase import CatalogDatabase
from googleCloudStorage import GoogleCloudStorage

class CloudDownloader(object):
	CATALOG_DB_FILENAME = 'catalog.db'
	def __init__(self, catalogPath, cloudStorage):
		self.catalogPath = catalogPath
		self.cloudStorage = cloudStorage
		self.catalogDb = CatalogDatabase(os.path.join(self.catalogPath, self.CATALOG_DB_FILENAME))

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
	parser.add_argument('--project', '-p', required=True, help='Cloud project ID')
	parser.add_argument('--bucket', '-b', required=True, help='Cloud bucket name')
	parser.add_argument('checksum', help='Checksum of file to download')
	parser.add_argument('destination', help='Path to download file to')
	args = parser.parse_args()

	cloudStorage = GoogleCloudStorage(args.project, args.bucket)
	downloader = CloudDownloader(args.catalog, cloudStorage)
	downloader.download(args.checksum, args.destination)

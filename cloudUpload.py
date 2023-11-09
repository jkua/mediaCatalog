#!/usr/bin/env python3

import logging
import os

from catalogDatabase import CatalogDatabase

class CloudUploader(object):
	CATALOG_DB_FILENAME = 'catalog.db'
	def __init__(self, catalogPath):
		self.catalogPath = catalogPath
		self.catalogDb = CatalogDatabase(os.path.join(self.catalogPath, self.CATALOG_DB_FILENAME))

	def upload(self):
		totalFileCount = self.catalogDb.getFileCount()
		filesToUpload = self.catalogDb.getFilesNotInCloud()

		notUploadedPercentage = len(filesToUpload)/totalFileCount*100
		print(f'\n{len(filesToUpload)}/{totalFileCount} ({notUploadedPercentage:.3f} %) files to be uploaded')

		for i, (checksum, filename, directory) in enumerate(filesToUpload, 1):
			uploadedPercentage = i/len(filesToUpload)*100
			print(f'[{i}/{len(filesToUpload)} ({uploadedPercentage:.3f} %)] {os.path.join(directory, filename)}: {checksum}')


if __name__=='__main__':
	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument('--catalog', '-c', required=True, help='Path to catalog')
	args = parser.parse_args()

	uploader = CloudUploader(args.catalog)
	uploader.upload()


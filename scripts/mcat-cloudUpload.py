#!/usr/bin/env python3

import logging

from mediaCatalog.mediaCatalog import MediaCatalog
from mediaCatalog.googleCloudStorage import GoogleCloudStorage
from mediaCatalog.cloudUploader import CloudUploader


if __name__=='__main__':
	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument('--catalog', '-c', required=True, help='Path to catalog')
	args = parser.parse_args()

	logging.basicConfig(level=logging.WARNING, format='%(asctime)s %(levelname)s %(message)s')

	with MediaCatalog(args.catalog) as catalog:
		cloudStorage = GoogleCloudStorage(catalog.config['cloudProject'], catalog.config['defaultCloudBucket'])
		uploader = CloudUploader(catalog, cloudStorage)
		uploader.upload()

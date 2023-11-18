#!/usr/bin/env python3

import logging
import os

from mediaCatalog.mediaCatalog import MediaCatalog
from mediaCatalog.googleCloudStorage import GoogleCloudStorage

if __name__=='__main__':
	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument('--catalog', '-c', required=True, help='Path to catalog')
	parser.add_argument('--path', '-p', help='Only verify files in this path and subpaths')
	parser.add_argument('--local', action='store_true', help='Verify files locally')
	parser.add_argument('--cloud', action='store_true', help='Verify files in the cloud')
	parser.add_argument('--all', action='store_true', help='Verify files in all locations')
	parser.add_argument('--verifyLocalChecksums', action='store_true', help='Verify checksums - this is very slow for local files!')
	args = parser.parse_args()

	logging.basicConfig(level=logging.WARNING, format='%(asctime)s %(levelname)s %(message)s')
	if args.path:
		args.path = os.path.abspath(args.path)
	if args.all:
		args.local = True
		args.cloud = True
	if not args.local and not args.cloud:
		raise Exception('Must specify at least one of --local or --cloud')

	with MediaCatalog(args.catalog) as catalog:
		if args.cloud:
			cloudStorage = GoogleCloudStorage(catalog.config['cloudProject'], catalog.config['defaultCloudBucket'])
		else:
			cloudStorage = None

		if catalog.verify(path=args.path, local=args.local, cloudStorage=cloudStorage, verifyChecksum=args.verifyLocalChecksums):
			print('\nVerification successful!')
		else:
			print('\n*** Verification failed! ***')
        
#!/usr/bin/env python3

import logging
import os

from mediaCatalog.mediaCatalog import MediaCatalog
from mediaCatalog.googleCloudStorage import GoogleCloudStorage

if __name__=='__main__':
	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument('--catalog', '-c', required=True, help='Path to catalog')
	parser.add_argument('path', nargs='+', help='Remove files in this path and subpaths')
	parser.add_argument('--local', action='store_true', help='Remove files locally')
	parser.add_argument('--cloud', action='store_true', help='Remove files in the cloud')
	parser.add_argument('--all', action='store_true', help='Remove files in all locations')
	args = parser.parse_args()

	logging.basicConfig(level=logging.WARNING, format='%(asctime)s %(levelname)s %(message)s')
	args.path = [os.path.abspath(path) for path in args.path]
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

		dbRecordsToRemove = []
		for path in args.path:
			print(f'\nRemoving files in path {path}...')
			if os.path.isfile(path):
				directory, filename = os.path.split(path)
			else:
				directory = path
				filename = None
			dbRecords, metadataAndPaths = catalog.query(filename=filename, directory=directory)

			if not dbRecords:
				print(f'WARNING: No files found matching query: {path}!')
				continue

			dbRecordsToRemove.extend(dbRecords)
			print(f'Found {len(dbRecords)} files matching query: {path}')

		print('\nFiles to remove:')
		print('----------------')
		for i, record in enumerate(dbRecordsToRemove, 1):
			print(f'{i}) {record["directory"]}{record["file_name"]}')
		
		print(f'\nTotal files to remove: {len(dbRecordsToRemove)}')
		while True:
			response = input("Are you sure you want to delete these files? Enter the word DELETE to continue: ")
			if response.upper() == 'DELETE':
				break
		
		numRemoved = catalog.remove(dbRecords, cloudStorage)
		
		if numRemoved == len(dbRecords):
			print(f'\n{numRemoved} records removed from catalog')
		else:
			print(f'\n*** ERROR! Only {numRemoved}/{len(dbRecords)} removed from catalog! ***')
	
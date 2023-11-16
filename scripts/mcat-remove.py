#!/usr/bin/env python3

import logging
import os

from mediaCatalog.mediaCatalog import MediaCatalog
from mediaCatalog.googleCloudStorage import GoogleCloudStorage

if __name__=='__main__':
	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument('--catalog', '-c', required=True, help='Path to catalog')
	parser.add_argument('--path', '-p', help='Exact path to remove - this takes precedence over --filename and --directory')
	parser.add_argument('--checksum', '-s', help='Checksum to remove')
	parser.add_argument('--directory', '-d', help='Directory to remove, wildcards supported')
	parser.add_argument('--filename', '-f', help='Filename to remove, wildcards supported')	
	parser.add_argument('--dryrun', '-r', action='store_true', help='Don\'t actually remove files')
	args = parser.parse_args()

	logging.basicConfig(level=logging.WARNING, format='%(asctime)s %(levelname)s %(message)s')
		
	if args.checksum is None and args.path is None and args.filename is None and args.directory is None:
		raise Exception('Must supply at least one of --checksum, --path, --filename, or --directory!')

	with MediaCatalog(args.catalog) as catalog:
		if catalog.config['cloudProject'] and catalog.config['defaultCloudBucket']:
			cloudStorage = GoogleCloudStorage(catalog.config['cloudProject'], catalog.config['defaultCloudBucket'])
		else:
			cloudStorage = None

		filename = None
		directory = None
		queryTokens = []
		if args.path:
			filename = os.path.basename(args.path)
			filename = filename if filename else None
			directory = os.path.dirname(args.path)
			directory = directory if directory else None

			if filename:
				queryTokens.append(f'filename: {filename}')
			if directory:
				queryTokens.append(f'directory: {directory}')

			if os.path.exists(args.path):
				args.checksum = catalog.checksum(args.path)

			if args.filename or args.directory:
				print('WARNING: --path takes precedence over --filename and --directory! Ignoring --filename and --directory!')
		else:
			if args.filename:
				queryTokens.append(f'filename: {args.filename}')
				filename = args.filename
			if args.directory:
				queryTokens.append(f'directory: {args.directory}')
				directory = args.directory

		if args.checksum:
			queryTokens.append(f'checksum: {args.checksum}')

		queryString = ', '.join(queryTokens)
		print(f'\nQuerying for files to remove with {queryString}...')

		dbRecords, metadataAndPaths = catalog.query(checksum=args.checksum, filename=filename, directory=directory)

		if not dbRecords:
			print(f'WARNING: No files found matching query: {queryString}!')
			import sys; sys.exit(0)

		print('\nFiles to remove:')
		print('----------------')
		for i, record in enumerate(dbRecords, 1):
			print(f'{i}) {record["directory"]}{record["file_name"]}')
		
		print(f'\nTotal files to remove: {len(dbRecords)}')

		if args.dryrun:
			print('\n*** DRY RUN - NO FILES REMOVED ***')
			import sys; sys.exit(0)

		while True:
			response = input("Are you sure you want to delete these files? Enter the word DELETE to continue: ")
			if response.upper() == 'DELETE':
				break
		
		numRemoved = catalog.remove(dbRecords, cloudStorage)
		
		if numRemoved == len(dbRecords):
			print(f'\n{numRemoved} records removed from catalog')
		else:
			print(f'\n*** ERROR! Only {numRemoved}/{len(dbRecords)} removed from catalog! ***')
	
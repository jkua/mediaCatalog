#!/usr/bin/env python3

import logging
import os

from mediaCatalog.mediaCatalog import MediaCatalog


if __name__=='__main__':
	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument('--catalog', '-c', required=True, help='Path to catalog')
	parser.add_argument('--path', '-p', help='File path to query')
	parser.add_argument('--checksum', '-s', help='Checksum to query')
	parser.add_argument('--metadata', '-m', action='store_true', help='Print metadata')
	parser.add_argument('--showBadMatches', '-b', action='store_true', help='Show bad matches')
	args = parser.parse_args()

	filename = None
	directory = None
	badMatches = False
	queryTokens = []

	if args.checksum is None and args.path is None:
		raise Exception('Must supply either a --path or --checksum')

	with MediaCatalog(args.catalog) as catalog:	
		if args.path:
			queryTokens.append(f'path {args.path}')
			filename = os.path.basename(args.path)
			directory = os.path.dirname(args.path)
			directory = directory if directory else None

			if os.path.exists(args.path):
				args.checksum = catalog.checksum(args.path)

		if args.checksum:
			queryTokens.append(f'checksum {args.checksum}')

		try:
			dbRecords, metadataAndPaths = catalog.query(checksum=args.checksum, filename=filename, directory=directory)
		except KeyError as e:
			if args.path and args.showBadMatches:
				badMatches = True
				queryTokens = [f'checksum {args.checksum}']
				dbRecords, metadataAndPaths = catalog.query(checksum=args.checksum)
			else:
				raise e

		message = f'Found {len(dbRecords)} records for {", ".join(queryTokens)}'
		if badMatches:
			message += ' but none for the specified path!'
		print(message)

		for i, record in enumerate(dbRecords, 1):
			message = f"\nDatabase record {i}"
			if badMatches:
				message += ' - PATH DOES NOT MATCH QUERY'
			print(message)
			print('=' * len(message))
			for key, value in zip(record.keys(), record):
				print(f'    {key}: {value}')
		
			if not args.metadata:
				continue

			for metadata, metadataPath in metadataAndPaths:
				if metadata['File:FileName'] == record['file_name'] and \
					metadata['File:Directory'] == os.path.normpath(record['directory']) and \
					metadata['HostName'] == record['host_name']:
						
					print(f'\n    Metadata - Path: {metadataPath} ')
					print('    --------')
					for key, value in metadata.items():
						print(f'    {key}: {value}')

					break

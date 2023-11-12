#!/usr/bin/env python3

import logging
import os

from mediaCatalog import MediaCatalog


if __name__=='__main__':
	import argparse
	parser = argparse.ArgumentParser()
	parser.add_argument('--catalog', '-c', required=True, help='Path to catalog')
	parser.add_argument('--path', '-p', help='File path to query')
	parser.add_argument('--checksum', '-s', help='Checksum to query')
	args = parser.parse_args()

	with MediaCatalog(args.catalog) as catalog:
		if args.checksum is None:
			if args.path is None:
				raise Exception('Must supply either a --path or --checksum')
			else:
				args.checksum = catalog.checksum(args.path)

		catalog.catalogDb.printFileRecord(args.checksum)

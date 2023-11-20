#!/usr/bin/env python3

import logging
import os

from mediaCatalog.mediaCatalog import MediaCatalog

if __name__=='__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--catalog', '-c', required=True, help='Catalog path')
    parser.add_argument('--directory', '-d', help='Directory to query, wildcards supported')
    parser.add_argument('--verbose', '-v', action='store_true', default=False, help='Verbose output')
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARNING)
    catalog = MediaCatalog(args.catalog, verbose=args.verbose)

    print(f'\nQuerying for duplicates in {args.directory if args.directory else "database"}...')
    checksumsWithDuplicates = catalog.catalogDb.getDuplicates(args.directory)
    print(f'--> {len(checksumsWithDuplicates)} checksums with duplicates found.')

    print(f'\nQuerying for records...')
    recordSets = []
    for checksum in checksumsWithDuplicates:
        records = catalog.catalogDb.read(checksum)
        recordSets.append((os.path.join(records[0]["directory"], records[0]["file_name"]), records))

    recordSets.sort()

    for _, records in recordSets:
        checksum = records[0]["checksum"]
        print(f'\nChecksum {checksum}:')
        for i, record in enumerate(records, 1):
            print(f'--> {i}) {os.path.join(record["directory"], record["file_name"])}')

    print(f'\n{len(recordSets)} duplicate checksums found in {args.directory if args.directory else "database"}')

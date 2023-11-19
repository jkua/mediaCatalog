#!/usr/bin/env python3

import logging

from mediaCatalog.mediaCatalog import MediaCatalog

if __name__=='__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--catalog', '-c', required=True, help='Catalog path')
    parser.add_argument('oldDirectory', help='Old directory that files were moved from')
    parser.add_argument('newDirectory', help='New directory that files have been moved to')
    parser.add_argument('--verbose', '-v', action='store_true', default=False, help='Verbose output')
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARNING)
    catalog = MediaCatalog(args.catalog, verbose=args.verbose)

    # Query catalog database for all files in old directory
    oldFiles = catalog.move(args.oldDirectory, args.newDirectory)
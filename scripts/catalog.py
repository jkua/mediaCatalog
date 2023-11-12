#!/usr/bin/env python3

import logging

from mediaCatalog.mediaCatalog import MediaCatalog

if __name__=='__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--catalog', '-c', required=True, help='Catalog path')
    parser.add_argument('path', nargs='+', help='Path(s) to process')
    parser.add_argument('--new', '-n', action='store_true', default=False, help='Create new catalog')
    parser.add_argument('--update', '-u', action='store_true', default=False, help='Update existing catalog entries (rewrite metadata and database)')
    args = parser.parse_args()

    cataloger = MediaCatalog(args.catalog, create=args.new, update=args.update)

    for path in args.path:
        cataloger.catalog(path)

    cataloger.close()   

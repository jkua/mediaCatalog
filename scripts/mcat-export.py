#!/usr/bin/env python3

import logging

from mediaCatalog.mediaCatalog import MediaCatalog

if __name__=='__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--catalog', '-c', required=True, help='Catalog path')
    parser.add_argument('--verbose', '-v', action='store_true', default=False, help='Verbose output')
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.WARNING)

    cataloger = MediaCatalog(args.catalog, verbose=args.verbose)
    print(f'\nExporting catalog to {cataloger.catalogCsvPath}')
    cataloger.catalogDb.export(cataloger.catalogCsvPath)
    cataloger.close()   

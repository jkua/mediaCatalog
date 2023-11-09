#!/usr/bin/env python3

import logging

from mediaCataloger import MediaCataloger

if __name__=='__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--catalog', '-c', required=True, help='Catalog path')
    parser.add_argument('--path', '-p', action='append', required=True, help='Path to process')
    parser.add_argument('--update', '-u', action='store_true', default=False, help='Update existing catalog entries (rewrite metadata and database)')
    args = parser.parse_args()

    cataloger = MediaCataloger(args.catalog, updateMode=args.update)

    for path in args.path:
        cataloger.catalog(path)

    cataloger.close()   

#!/usr/bin/env python3

import logging

from mediaCatalog.mediaCatalog import MediaCatalog

if __name__=='__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--catalog', '-c', required=True, help='Catalog path')
    args = parser.parse_args()

    mediaCatalog = MediaCatalog(args.catalog)
    mediaCatalog.printStats()

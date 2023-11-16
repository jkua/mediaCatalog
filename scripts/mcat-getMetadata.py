#!/usr/bin/env python3

from mediaCatalog.utils import getMetadata

if __name__=='__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Get metadata from a file')
    parser.add_argument('file', nargs='+', help='File to get metadata from')
    args = parser.parse_args()

    metadata = getMetadata(args.file)

    for file, md in zip(args.file, metadata):
        print(file)
        for k, v in md.items():
            print(f'    {k}: {v}')
            
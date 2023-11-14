import hashlib
import logging

import exiftool
import acoustid


def getMimeTypes(filenames: list) -> list:
    with exiftool.ExifToolHelper() as et:
        try:
            tags = et.get_tags(filenames, tags=['File:MIMEType'])
            mimeTypes = [tag['File:MIMEType'] for tag in tags]
        except Exception as e:
            mimeTypes = []
            for file in filenames:
                try:
                    tag = et.get_tags(file, tags=['File:MIMEType'])[0]
                    mimeTypes.append(tag['File:MIMEType'])
                except:
                    # logging.warning(f'Failed to get MIME type for {file}')
                    mimeTypes.append(None)
    return mimeTypes

def getMetadata(filenames: list) -> list:
    with exiftool.ExifToolHelper() as et:
        try:
            metadata = et.get_metadata(filenames)
        except Exception as e:
            metadata = []
            for file in filenames:
                try:
                    metadata.append(et.get_metadata(file)[0])
                except:
                    logging.warning(f'Failed to get metadata for {file}')
                    metadata.append(None)
    return metadata


def md5sum(filename: str, chunkSize=1024*1024) -> str:
    hash_function = hashlib.md5()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(chunkSize), b""):
            hash_function.update(chunk)
    return hash_function.hexdigest()


def sha256sum(filename: str, chunkSize=1024*1024) -> str:
    hash_function = hashlib.sha256()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(chunkSize), b""):
            hash_function.update(chunk)
    return hash_function.hexdigest()


def getPreciseCaptureTimeFromExif(metadata):
    if metadata.get('EXIF:DateTimeOriginal'):
        captureTime = metadata.get('EXIF:DateTimeOriginal')
    else:
        return None

    if metadata.get('EXIF:SubSecTimeOriginal'):
        captureTime += f".{metadata.get('EXIF:SubSecTimeOriginal')}"

    if metadata.get('EXIF:OffsetTimeOriginal'):
        captureTime += metadata['EXIF:OffsetTimeOriginal']

    return captureTime


def getAcoustid(filename: str, quiet=False, ACOUSTID_API_KEY=None) -> list:
    if ACOUSTID_API_KEY:
        results = acoustid.match(ACOUSTID_API_KEY, filename)
    else:
        raise Exception('AcoustID API Key not provided!')
    outputList = []
    for i, (score, rid, title, artist) in enumerate(results, 1):
        outputList.append((score, rid, title, artist))
        if not quiet:
            print(f'Acoustid Match {i}:')
            print(f'    {artist} - {title}')
            print(f'    http://musicbrainz.org/recording/{rid}')
            print(f'    Score: {score*100:.3f} %')
    return outputList
import hashlib

import exiftool
import acoustid


def getMetadata(filenames: list) -> list:
    with exiftool.ExifToolHelper() as et:
        metadata = et.get_metadata(filenames)
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

    if metadata.get(metadata['EXIF:OffsetTimeOriginal']):
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
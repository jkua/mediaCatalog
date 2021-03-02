import hashlib

import exiftool

def getMetadata(filenames: list) -> list:
    with exiftool.ExifTool() as et:
        metadata = et.get_metadata_batch(filenames)
    return metadata


def md5sum(filename: str, chunkSize=1024*1024) -> str:
    hash_md5 = hashlib.md5()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(chunkSize), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()
# mediaCatalog
## Overview
This generates a catalog to track, as well as backup to a cloud archive, 
*media files*. The core concept here is that these media files are 
*immutable objects* and therefore the catalog only needs to track a single 
version of those objects, even if there are multiple copies. This catalog can 
be used to backup these files to a cloud archive as well as determine if files 
are missing or corrupt. If a cloud archive is used, this can be used to restore 
erroneous files.

## Technical Details
1. Each file has a SHA256 checksum computed and logged for validation purposes
2. If a file is present in multiple locations, the database logs multiple entries
3. Only one copy of a file is stored in the cloud archive, named with the checksum
4. `exiftool` is used to extract a set of metadata from each file, including 
the MIME type
5. The MIME type is used to determine if the file is of a media type 
(image/video/audio/text*). In this way, file extensions are irrelevant
6. The catalog consists of a small database which logs checksums, local file 
paths, MIME types, file sizes, capture devices, and cloud locations
7. In addition, there is a metadata store, where the information extracted by 
`exiftool` is stored as a set of JSON files
8. The metadata store is implemented as a hash directory tree utilizing the 
checksum value, with collision handling.

## Installation (MacOS)
1. `brew install exiftool`
2. `pip3 install -r requirements.txt`
3. `pip3 install -e .`

### Run tests
1. Create `config.yaml` in `tests/` with your *test values* for cloud storage: `cloudProject`, `defaultCloudBucket`, `cloudObjectPrefix`
    1. `defaultCloudBucket` should *NOT* be your production bucket
    2. `cloudObjectPrefix` should *NOT* be your production value (typically `file`)
2. `pytest tests`

## Operation
### Catalog
* Create new catalog: `mcat catalog -c <catalog path> -n <path to process>`
* Add files to catalog: `mcat catalog -c <catalog path> <path1 to process> <path2 to process> ...`
* Query catalog by path (add `-m` flag to display metadata): `mcat query -c <catalog path> -p <path>`
* Query catalog by checksum: `mcat query -c <catalog path> -s <checksum>`
* Query catalog by directory (supports wildcards): `mcat query -c <catalog path> -d <directory>`
* Remove file from catalog (and cloud): `mcat remove -c <catalog path> -p <path to remove>`
* Remove files in a directory (use a wildcard to remove subdirectories as well): `mcat remove -c <catalog path> -d <directory>`
* Verify files (local and/or cloud) against the catalog: `mcat verify -c <catalog path> -p <specific path> [--local, --cloud, --all]`

### Cloud
Prior to executing cloud operations, set the following cloud parameters in `<catalog_path>/config.yaml`:
1. `cloudProject`
2. `defaultCloudBucket`
3. `cloudObjectPrefix`

* Upload files to the cloud: `mcat cloudUpload -c <catalog path>`
* Download file from the cloud: `mcat cloudDownload -c <catalog path> <checksum> <destination>`

### Tools
* Directly extract and display metadata from a media file: `mcat getMetadata <path>`



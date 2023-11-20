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
4. [exiftool](https://exiftool.org/) is used to extract a set of metadata from 
    each file, including the MIME type
5. The MIME type is used to determine if the file is of a media type 
    (image/video/audio/text*). In this way, file extensions are irrelevant
6. The catalog consists of:
    1. A small database which logs checksums, local file paths, MIME types, 
        file sizes, capture devices, and cloud locations
    2. A metadata store, where the information extracted by `exiftool` is 
        stored as a set of JSON files
7. The metadata store is implemented as a hash directory tree utilizing the 
    checksum value, with collision handling.

\* This may change. Text files are useful for metadata, but their mutability 
makes them problematic for this tool to track.

## Installation 
### MacOS
1. Install [exiftool](https://exiftool.org/): `brew install exiftool`
2. [Install gcloud CLI](https://cloud.google.com/sdk/docs/install) and login
    1. `sudo snap install google-cloud-cli --classic`
    2. `gcloud auth application-default login`
3. Install MediaCatalog
    1. `git clone https://github.com/jkua/mediaCatalog`
    2. `cd mediaCatalog`
    3. `pip3 install .`
### Debian/Ubuntu
1. Install [exiftool](https://exiftool.org/)
    1. `wget https://exiftool.org/Image-ExifTool-12.70.tar.gz`
    2. `tar xzvf Image-ExifTool-12.70.tar.gz`
    3. `cd Image-ExifTool`
    4. `make test`
    5. `sudo make install`
2. [Install gcloud CLI](https://cloud.google.com/sdk/docs/install) and login
    1. `sudo snap install google-cloud-cli --classic`
    2. `gcloud auth application-default login`
3. Install MediaCatalog
    1. `git clone https://github.com/jkua/mediaCatalog`
    2. `cd mediaCatalog`
    3. `pip3 install .`

### Developer Installation
1. Install exiftool and the gcloud CLI as described above
2. Install MediaCatalog in developer mode
    1. `git clone https://github.com/jkua/mediaCatalog`
    2. `cd mediaCatalog` 
    3. `pip3 install -r requirements.txt`
    4. `pip3 install -e .`

## Cloud setup
Currently the tool only supports Google Cloud for file archival. Create a 
bucket to archive your media files. You will need put the following 
information in the catalog's `config.yaml`:
1. `cloudProject`: Google Cloud project name
2. `defaultCloudBucket`: Google Cloud bucket for the media archive
3. `cloudObjectPrefix`: The prefix (psuedo-directory) that will be appended
    to each cloud object name. The default is `file`, but this can be 
    whatever you want

You will also need to [set up the Application Default Credentials](https://cloud.google.com/docs/authentication/provide-credentials-adc) 
for the Google API Client.

### Storage classes and Lifecycle policy
Currently the tool does not manage [storage classes](https://cloud.google.com/storage/docs/storage-classes). 
It is recommended that the [default storage class](https://cloud.google.com/storage/docs/changing-default-storage-class) 
be *standard* so that any accidental uploads can be removed without running 
afoul of minimum storage durations. If you wish to save on storage costs, it is 
recommended to add a [lifecycle rule](https://cloud.google.com/storage/docs/lifecycle) 
to change the storage class of a file to a colder class (nearline, coldline, 
archive) after a specified time after upload, e.g. 7-30 days, depending on your 
workflow. Be aware that these colder classes have increasing minimum storage 
durations and retrieval fees.

## Run tests
1. Create `config.yaml` in `tests/` with your *test values* for cloud storage: `cloudProject`, `defaultCloudBucket`, `cloudObjectPrefix`
    1. `defaultCloudBucket` should *NOT* be your production bucket
    2. `cloudObjectPrefix` should *NOT* be your production value (typically `file`)
2. `pytest tests`

## Operation
### Catalog
* Create new catalog: 
    * `mcat catalog -c <catalog path> -n <path to process>`
* Add files to catalog: 
    * `mcat catalog -c <catalog path> <path1 to process> <path2 to process> ...`
* Query catalog by path (add `-m` flag to display metadata): 
    * `mcat query -c <catalog path> -p <path>`
* Query catalog by checksum: 
    * `mcat query -c <catalog path> -s <checksum>`
* Query catalog by directory (supports wildcards): 
    * `mcat query -c <catalog path> -d <directory>`
* Verify files (local and/or cloud) against the catalog: 
    * `mcat verify -c <catalog path> -p <specific path> [--local, --cloud, --all]`
* Update paths after files are moved:
    * `mcat move -c <catalog path> <old directory> <new directory>`
* Remove file from catalog (and cloud): 
    * `mcat remove -c <catalog path> -p <path to remove>`
* Remove files in a directory (use a wildcard to remove subdirectories as well): 
    * `mcat remove -c <catalog path> -d <directory>`
* Display catalog stats:
    * `mcat stats -c <catalog path>`
* Export database to CSV at `<catalog path>/catalog.csv`:
    * `mcat export -c <catalog path>`

### Cloud
* Upload files to the cloud: 
    * `mcat cloudUpload -c <catalog path>`
* Download file from the cloud: 
    * `mcat cloudDownload -c <catalog path> <checksum> <destination>`

### Tools
* Directly extract and display metadata from a media file: 
    * `mcat getMetadata <path>`



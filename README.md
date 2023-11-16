# mediaCatalog

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

### Cloud
Prior to executing cloud operations, set the following cloud parameters in `<catalog_path>/config.yaml`:
1. `cloudProject`
2. `defaultCloudBucket`
3. `cloudObjectPrefix`

* Upload files to the cloud: `mcat cloudUpload -c <catalog path>`
* Download file from the cloud: `mcat cloudDownload -c <catalog path> <checksum> <destination>`

### Tools
* Directly extract and display metadata from a media file: `mcat getMetadata <path>`



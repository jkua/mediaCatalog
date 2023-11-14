# mediaCatalog

## Installation (MacOS)
1. `brew install exiftool`
2. `pip3 install -r requirements.txt`
3. `pip3 install -e .`

### Run tests
1. Create `config.yaml` in `tests/` with your *test values* for cloud storage: `project, `defaultBucket`, `cloudObjectPrefix`
    1. `defaultBucket` should *NOT* be your production bucket
    2. `cloudObjectPrefix` should *NOT* be your production value (typically `file`)
2. `pytest tests`

## Operation
### Add files to catalog (first time add `-n` to create a new catalog)
`./catalog.py -c <catalog path> -p <path to process>`

### Query catalog (add `-m` flag to display metadata)
`./queryCatalog.py -c <catalog path> -p <path>`
`./queryCatalog.py -c <catalog path> -s <checksum>`

### Upload files to the cloud
`./cloudUpload.py -c <catalog path>`

### Download file from the cloud
`./cloudDownload.py -c <catalog path> <checksum> <destination>`


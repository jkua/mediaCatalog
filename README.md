# photoCatalog

## Installation (MacOS)
brew install exiftool
pip3 install -r requirements.txt

## Operation
Add files to catalog (first time add -n to create a new catalog)
`./catalog.py -c <catalog path> -p <path to process>`

Upload files to the cloud
`./cloudUpload.py -c <catalog path>`

Download file from the cloud
`./cloudDownload.py -c <catalog path> <checksum> <destination>`

Query catalog
`./queryCatalog.py -c <catalog path> -p <path>`
`./queryCatalog.py -c <catalog path> -s <checksum>`

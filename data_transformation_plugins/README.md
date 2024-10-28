## Information about the folder
This folder is a part of the `automation pipeline using DAG`. It contains the functions which are essential for transforming the given dataset into COGs. There are python files for transforming every dataset which will be used as plugins into the pipeline.

## How this folder fits in the automation pipeline
These functions/files are created by the `developer` to transform a single data file to COG. Once the COGs are validated, these python scripts are pushed to the `GHGC-SMCE S3` These files are then fetched by the `SM2A DAG` to complete the transformation of entire dataset automatically.

## Naming convention for the transformation files in the folder
- `name of python file` - `collectionname_transformation.py`
`collectionname` refers to the STAC collection name of the dataset followed by the word `transformation`. Make sure the `collectionname` within the filename matches with the `collectionname` passed as a `parameter` to the DAG.
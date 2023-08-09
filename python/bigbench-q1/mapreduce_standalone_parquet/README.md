# How to build:

- cd into this directory
- `mkdir package_python_standalone_bb_q1_parquet`
- `pip install --target ./package_python_standalone_bb_q1_parquet/ -r requirements.txt`. Note that your python version must match the runtime's python version.
- `cp *.py package_python_standalone_bb_q1_parquet`
- `cd package_python_standalone_bb_q1_parquet`
- `zip -r ../package_python_standalone_bb_q1_parquet.zip .`
- upload package to s3
- create function with settings
- invoke with `util/invoke.py`
#!/bin/bash

### NOTE: cannot be used with Dataproc Serverless
gsutil cp gs://vijay-lens-python-packages/requirements.txt .
pip install -r requirements.txt
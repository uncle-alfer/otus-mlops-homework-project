#! /bin/bash

root_dir=`git rev-parse --show-toplevel`

cd "$root_dir"

uvicorn src.prediction.main:app --reload
#! /bin/bash

root_dir=`git rev-parse --show-toplevel`

cd "$root_dir"

PYTHONPATH="$root_dir"/src/prediction/:$PYTHONPATH uvicorn main:app --reload
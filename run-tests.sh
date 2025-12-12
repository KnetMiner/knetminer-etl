#!/usr/bin/env bash

# Little wrapper for pytest. TODO: move to CI.


set -e

pytest_params="$@"

# Change to the script's directory
my_path="$(realpath "$0")"
cd "$(dirname "$my_path")"
prj_path="$(realpath "$(pwd)")"

set -x
# Default params are in pyproject.toml
poetry run pytest $pytest_params
set +x


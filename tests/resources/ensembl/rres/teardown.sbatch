#!/usr/bin/env bash
set -euo pipefail

printf "\n\n|==== Tearing down the RRes Environment for KETL\n"

mypath="$(scontrol show job $SLURM_JOBID | awk -F= '/Command=/{print $2}')"
mypath="$(realpath "$mypath")"
mydir="$(dirname "$mypath")"

cd "$mydir"
. env.sh

printf "\n|== Stopping the Neo4j server\n"
kslurm-neo-stop.sh --track "$NEO_TRACK_PATH" || true

printf "\n|== Stopping the Spark\n"
kslurm-spark-stop.sh --track "$SPARK_TRACK_PATH" || true

printf "\n|==== All done\n\n"

#!/usr/bin/env bash
set -euo pipefail

do_update=false
while [[ $# -gt 0 ]]; do
	case "$1" in
		--update) do_update=true; shift ;;
		*) echo "Unknown option: $1" >&2; exit 1 ;;
	esac
done

printf "\n\n|==== Setting up the RRes Environment for KETL\n"

mypath="$(scontrol show job $SLURM_JOBID | awk -F= '/Command=/{print $2}')"
mypath="$(realpath "$mypath")"
mydir="$(dirname "$mypath")"

cd "$mydir"
. env.sh

if $do_update; then
	printf "\n|== Updating the KETL installation\n"
	pip install --force --no-deps git+https://github.com/KnetMiner/knetminer-etl
fi

printf "\n|== Starting the Neo4j server\n"
kslurm-neo-start.sh --track "$NEO_TRACK_PATH"

printf "\n|== Starting the Spark\n"
kslurm-neo-start.sh --track "$SPARK_TRACK_PATH"

printf "\n|==== All done, now you can run \"%s\"\n\n" "$mydir/run.sbatch"

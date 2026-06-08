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

cd "$KETL_PRJ_HOME"

if $do_update; then
	printf "\n|== Installing/Updating the KETL deployment from local distro\n"

	# Alternative is git+https://github.com/KnetMiner/knetminer-etl
	pip_src="dist/knetminer_etl-1.0.0.tar.gz" 

	cd "$KETL_PRJ_HOME"

	if [[ ! -e venv ]]; then
		python -m venv venv
		. venv/bin/activate
		pip install "$pip_src"
	else
		pip install --force --no-deps "$pip_src"
	fi
fi

printf "\n|== Starting the Neo4j server\n"
kslurm-neo-start.sh --track "$NEO_TRACK_PATH"

printf "\n|== Starting the Spark\n"
kslurm-spark-start.sh --track "$SPARK_TRACK_PATH"

printf "\n|==== All done, now you can do: srun \"%s\"\n\n" run.sbatch"

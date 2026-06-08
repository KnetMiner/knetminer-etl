#!/usr/bin/env bash
set -euo pipefail

function usage ()
{
	cat <<EOT
	
	
Usage: srun $0 [--update] [--neo <true|false>] [--spark <true|false>] [--help]
	
Options:
  --update: Update the KETL installation from the local distro (dist/knetminer_etl-*.tar.gz)
  --neo: Whether to start the Neo4j server (default: true)
  --spark: Whether to start the Spark server (default: true)
  --help: Show this help message and exit
EOT
}

do_update=false
do_neo=true
do_spark=true

# Probably not using srun, do usage
[[ -n "${SLURM_JOBID:-}" ]] || { usage; exit 1; }

while [[ $# -gt 0 ]]; do
	case "$1" in
		--update) do_update=true; shift ;;
		--neo) do_neo="$2"; shift 2 ;;
		--spark) do_spark="$2"; shift 2 ;;
		--help) usage; exit 2 ;;
		*) echo "Unknown option: $1" >&2; usage; exit 1 ;;
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
	pip_src=$(echo dist/knetminer_etl-*.tar.gz)

	cd "$KETL_PRJ_HOME"

	if [[ ! -e venv ]]; then
		python -m venv venv
		. venv/bin/activate
		# We don't ship KETL with this, since many environments have it already,
		# This isn't the only way to install Snake, you might want to have it in a shared place
		pip install snakemake
		pip install "$pip_src"
	else
		pip install --force --no-deps "$pip_src"
	fi
fi

if $do_neo; then
	printf "\n|== Starting the Neo4j server\n"
	kslurm-neo-start.sh --track "$NEO_TRACK_PATH" --sbatch "--output=$WF_HOME/neo-server.out --error=$WF_HOME/neo-server.out"
fi

if $do_spark; then
	printf "\n|== Starting the Spark\n"
	kslurm-spark-start.sh --track "$SPARK_TRACK_PATH" --sbatch "--output=$WF_HOME/spark-server.out --error=$WF_HOME/spark-server.out"
fi

printf "\n|==== All done, now you can do: srun run.sbatch\n\n"

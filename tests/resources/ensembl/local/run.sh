#!/usr/bin/env bash
set -euo pipefail

do_neo=true
do_spark=true
do_workflow=true
do_clean=false
function usage ()
{
	cat <<EOT


Usage: $0 [options]

Runs the ENSEMBL workflow locally, with Neo4j and Spark started as local processes.
This is meant for testing and development purposes, and due to that, the script
relies on 'poetry run' launched from the KETL project directory. So, you do need 
the dev environment to make this script work.

At the moment, this means Python, Poetry and a running Docker daemon. Spark is run 
through the project dependencies, while Neo4j is run via Docker (since we do the same
when testing with Testcontainers).

Options:
  --do-neo <true|false>       Start the Neo4j server (default: true)
  --do-spark <true|false>     Start the Spark server (default: true)
  --do-workflow <true|false>  Run the Snakemake workflow (default: true)
	--clean                     Deletes previous output data (WARN)
  --help                      Show this help message and exit

Other env variables that might affect this:

See wf_config.py, kneo-docker-run.sh, kspark-local-run.sh 

EOT
}

while [[ $# -gt 0 ]]; do
	case "$1" in
		--do-neo) do_neo="$2"; shift 2 ;;
		--do-spark) do_spark="$2"; shift 2 ;;
		--do-workflow) do_workflow="$2"; shift 2 ;;
		--clean) do_clean=true; shift ;;
		--help) usage; exit 2 ;;
		*) echo "Unknown option: $1" >&2; usage; exit 1 ;;
	esac
done

printf "\n\n|==== Running the Snakemake workflow locally\n"

mypath="$(realpath "${BASH_SOURCE[0]}")"
mydir="$(dirname "$mypath")"

cd "$mydir/.."
wf_dir="$(pwd)"

export NEO4J_PASSWORD="testTest"
export NEO4J_BOLT_PORT=7687
export NEO4J_URL="bolt://localhost:$NEO4J_BOLT_PORT"

export SPARK_MASTER_URL="spark://localhost:7077"
# export SPARK_MASTER_URL="local[*]"

mkdir -p .snakemake/logs
out_dir="$(realpath .snakemake/logs)"

cd ../../.. # project dir

if $do_clean; then
	printf "\n|== Deleting previous output data\n"
	rm -Rf "$wf_dir"/data/{output,tmp}
fi


neo_pid=""
spark_pid=""

function cleanup ()
{
	if $do_neo && [[ -n "$neo_pid" ]]; then
		echo "|== Stopping Neo (PID: $neo_pid)"
		kill $neo_pid || true
	fi

	if $do_spark && [[ -n "$spark_pid" ]]; then
		echo "|== Stopping Spark (PID: $spark_pid)"
		kill $spark_pid || true
	fi

	wait
	printf "\n\n|==== All done\n\n"
}
trap cleanup EXIT

if $do_neo; then
	neo_out_path="$out_dir/neo4j.out"
	printf "\n|== Starting the Neo4j server, output in %s\n\n" "$neo_out_path"
	poetry run src/scripts/neo4j-utils/kneo-docker-run.sh &> "$neo_out_path" &
	neo_pid=$!
fi

if $do_spark; then
	spark_out_path="$out_dir/spark.out"
	printf "\n|== Starting Spark, output in %s\n\n" "$spark_out_path"
	poetry run src/scripts/spark-utils/kspark-local-run.sh --workers 2 \
		&> "$spark_out_path" &
	spark_pid=$!
fi

if $do_neo || $do_spark; then
	printf "\n|== Waiting for the servers to start\n"
	sleep 20
fi

if $do_workflow; then
	echo "|== Running the Snakemake workflow"
	poetry run snakemake -s "$wf_dir/workflow.snakefile" --cores all all
	# cleanup is called via EXIT
elif $do_neo || $do_spark; then
	echo "|== Servers started ^C or kill to stop"
	wait
	# cleanup is called via INT
fi

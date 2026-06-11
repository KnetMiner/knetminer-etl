#!/usr/bin/env bash
set -euo pipefail

nworkers=2
export SPARK_MASTER=${SPARK_MASTER:-"$(hostname -f)"}
export SPARK_PORT=${SPARK_PORT:-7077}
export SPARK_WEB_PORT=${SPARK_WEB_PORT:-8080}

function usage ()
{
  cat <<EOT


Usage:

  $(basename "$0") [options]

Start a Spark cluster on the local host, to be used for tests and alike.

Options:

  --workers <n>.    Number of workers, including the (default: $nworkers)
  --cores <n>.    Cores per node (default: based on available ones)
  --ram <n>.      RAM per node in GB (default: based on 16 GB total)
  --help          Shows this help and exits with 2

Environment variables:

  SPARK_MASTER      Hostname or IP for the Spark master (default: uses 'hostname')
  SPARK_PORT        Spark master port (equivalent to --port)
  SPARK_WEB_PORT    Spark master web UI port (equivalent to --web-port)
  SPARK_RAM         RAM per node in GB (equivalent to --ram)
  SPARK_CORES       Cores per node (equivalent to --cores)

EOT
}

while [[ $# -gt 0 ]]; do
	case "$1" in
		--workers) nworkers="$2"; shift 2 ;;
		--cores) SPARK_CORES="$2"; shift 2 ;;
		--ram) SPARK_RAM="$2"; shift 2 ;;
		--help) usage; exit 2 ;;
		*) echo "Unknown option: $1" >&2; exit 2;;
	esac
done

# Redo it, it might have changed
export SPARK_RAM=${SPARK_RAM:-$((16 / nworkers))}
export SPARK_CORES=${SPARK_CORES:-$(($(nproc) / nworkers))}


# Where is Spark, exactly?
spark_class_cmd="spark-class"
if [[ -n "${SPARK_HOME:-}" ]]; then
  spark_class_cmd="$SPARK_HOME/bin/${spark_class_cmd}"
fi

printf "\n\n|==== Starting a local Spark cluster with 1 master + %d workers\n\n" "$nworkers"
echo "|== Starting Spark master with host='$SPARK_MASTER', port='$SPARK_PORT', web UI port='$SPARK_WEB_PORT'"

"$spark_class_cmd" org.apache.spark.deploy.master.Master \
  --host "$SPARK_MASTER" \
  --port "$SPARK_PORT" \
  --webui-port "$SPARK_WEB_PORT" &
master_pid=$!

# Give the master a moment to bind its port before workers connect
sleep 5

worker_pids=""
if [[ "$nworkers" -eq 0 ]]; then
	echo "|== No workers requested, only master started"
else
	echo "|== Starting $nworkers Spark worker(s) with $SPARK_CORES cores and ${SPARK_RAM}G RAM each"
	spark_process_ram=$((SPARK_RAM - 1)) # Leave 1 GB for overhead
	for iworker in $(seq 1 "$nworkers"); do
		echo "|== Starting Spark worker $iworker"
		"$spark_class_cmd" org.apache.spark.deploy.worker.Worker \
			"spark://${SPARK_MASTER}:${SPARK_PORT}" \
			--cores "$SPARK_CORES" \
			--memory "${spark_process_ram}G" &
	worker_pids+=" $!"
	done
fi

printf "\n\n|==== Spark cluster started\n\n"

function stop_handler ()
{
	echo "|== Stopping Spark cluster"
	kill $worker_pids $master_pid || true
	wait
	printf "\n\n|==== Spark cluster stopped\n\n"
	exit 0
}

trap stop_handler EXIT
wait

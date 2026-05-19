#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export SPARK_NODES=2
export SPARK_CORES=4
export SPARK_RAM=4
export SPARK_TIME="02:00:00"
export SPARK_TRACK_PATH=""
export SPARK_PORT=7077
export SPARK_WEB_PORT=8080

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Start a Spark standalone cluster as a SLURM job.

Options:
  --nodes <num>           Number of Spark nodes (default: $SPARK_NODES)
  --cores <num>           Cores per node (default: $SPARK_CORES)
  --ram <num>             RAM per node in GB (default: $SPARK_RAM)
                          (the Spark process is given 1 GB less)
  --time <duration>       SLURM time limit (passed to sbatch --time), e.g. 02:00:00 
                          (default: $SPARK_TIME)
  --track <path>          Base path for tracking the cluster nodes/jobs:
                            <path>.master  — Spark master hostname
                            <path>.port    — Spark master port
                            <path>.jobid   — SLURM job ID that started the cluster (through this script)
  --port <num>            Spark master port (default: $SPARK_PORT)
  --web-port <num>        Spark master web UI port (default: $SPARK_WEB_PORT)
  --help                  Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nodes)        SPARK_NODES="$2";        shift 2 ;;
    --cores)        SPARK_CORES="$2";        shift 2 ;;
    --ram)          SPARK_RAM="$2";          shift 2 ;;
    --time)         SPARK_TIME="$2";         shift 2 ;;
    --track)        SPARK_TRACK_PATH="$2";   shift 2 ;;
    --port)         SPARK_PORT="$2";         shift 2 ;;
    --web-port)     SPARK_WEB_PORT="$2";     shift 2 ;;
    --help)         usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
  esac
done

sbatch_path="$script_dir/spark-start.sbatch"

printf "\n\n|==== Starting the Spark cluster\n\n"

exec 3>&1
submit_out=$(sbatch \
  --nodes="$SPARK_NODES" \
  --cpus-per-task="$SPARK_CORES" \
  --mem="${SPARK_RAM}G" \
  --time="$SPARK_TIME" \
  "$sbatch_path"
  | tee >(cat >&3)
)

job_id=$(echo "$submit_out" | grep -oP '(?<=Submitted batch job )\d+')

if [[ -n "$SPARK_TRACK_PATH" && -n "$job_id" ]]; then
  echo "$job_id" > "${SPARK_TRACK_PATH}.jobid"
  echo "|== Job ID $job_id written to ${SPARK_TRACK_PATH}.jobid"
  echo "|== Master host/port will be written to ${SPARK_TRACK_PATH}.master / ${SPARK_TRACK_PATH}.port once the job starts."
fi

printf "\n|==== Spark cluster started\n\n"

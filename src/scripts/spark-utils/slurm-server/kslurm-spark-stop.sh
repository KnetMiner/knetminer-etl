#!/usr/bin/env bash
set -euo pipefail

SPARK_TRACK_PATH=""

function usage() {
  cat <<EOT


Usage: $(basename "$0") --track <path>

Stop a running Spark SLURM job by cancelling it via the stored job ID.

Options:
  --track <path>          Base path used when starting the cluster.
                          Reads <path>.jobid to find the SLURM job to cancel.
  --help                  Show this help
EOT
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --track) SPARK_TRACK_PATH="$2"; shift 2 ;;
    --help)         usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
  esac
done

if [[ -z "$SPARK_TRACK_PATH" ]]; then
  echo "Error: --track is required." >&2
  usage >&2
  exit 1
fi

printf "\n\n|==== Stopping the Spark cluster\n\n"

JOBID_FILE="${SPARK_TRACK_PATH}.jobid"

if [[ ! -f "$JOBID_FILE" ]]; then
  echo "Error: job ID file not found: $JOBID_FILE" >&2
  exit 1
fi

JOB_ID=$(cat "$JOBID_FILE")

if [[ -z "$JOB_ID" ]]; then
  echo "Error: $JOBID_FILE is empty." >&2
  exit 1
fi

echo "|== Cancelling SLURM job $JOB_ID..."
scancel "$JOB_ID"
echo "|== Done."

# Track files are normally removed by spark-start.sbatch on exit,
# but clean up here too in case the job was already dead.
echo "|== Removing Spark track files..."
rm -f "${SPARK_TRACK_PATH}.master" "${SPARK_TRACK_PATH}.port" "${SPARK_TRACK_PATH}.jobid"

printf "\n|==== Spark cluster stopped\n\n"

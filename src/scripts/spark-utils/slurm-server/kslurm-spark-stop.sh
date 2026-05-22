#!/usr/bin/env bash
set -euo pipefail

export SPARK_TRACK_PATH="spark-server"

function usage() {
  cat <<EOT


Usage: $(basename "$0") --track <path>

Stop a running Spark SLURM job by cancelling it via the stored job ID.

Options:
  --track <path>          Base path used when starting the cluster.
                          Reads <path>.jobid to find the SLURM job to cancel.
                          Default: $SPARK_TRACK_PATH
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

jobid_path="${SPARK_TRACK_PATH}.jobid"

if [[ ! -f "$jobid_path" ]]; then
  echo "Error: job ID file not found: $jobid_path" >&2
  exit 1
fi

job_id=$(cat "$jobid_path")

if [[ -z "$job_id" ]]; then
  echo "Error: $jobid_path is empty." >&2
  exit 1
fi

echo "|== Cancelling SLURM job $job_id..."
scancel "$job_id"
echo "|== Done."

# Track files are normally removed by spark-start.sbatch on exit,
# but clean up here too in case the job was already dead.
echo "|== Removing Spark track files..."
rm -f "${SPARK_TRACK_PATH}".{master,port,jobid}

printf "\n|==== Spark cluster stopped\n\n"

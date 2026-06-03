#!/usr/bin/env bash
set -euo pipefail

NEO_TRACK_PATH="neo-server"
NEO_STOP_TIMEOUT=120


function usage() {
	cat <<EOT

Usage: $(basename "$0") --track <path>

Options:
  --track <path>          Base path used when starting the Neo4j server.
                          Reads <path>.jobid to find the SLURM job to cancel.
                          Default: $NEO_TRACK_PATH
	--stop-timeout <secs>   Shutdown timeout in seconds (default: $NEO_STOP_TIMEOUT). 
													Similar to the same option in the start script, it waits up to this
													time for the complete shutdown and cleaning of the server tracking files.
  --help                  Show this help
EOT
}

while [[ $# -gt 0 ]]; do
	case "$1" in
		--track) NEO_TRACK_PATH="$2"; shift 2 ;;
		--help)         usage; exit 0 ;;
		*) echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
	esac
done

printf "\n\n|==== Stopping the Neo4j server\n\n"
if [[ -z "$NEO_TRACK_PATH" ]]; then
	echo "Error: --track is required." >&2
	usage >&2
	exit 1
fi

jobid_path="${NEO_TRACK_PATH}.jobid"
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

# the sbatch has a INT trap, which manages the Neo shutdown. --signal tells SLURM to 
# send it to the job's process.
scancel --batch --signal SIGINT "$job_id"
# Keep polling squeue until done
timeout "$NEO_STOP_TIMEOUT" bash -c "while squeue --job $job_id &>/dev/null; do sleep 1; done"
# Still around?
if squeue --job "$job_id" &>/dev/null; then
	echo "|== Neo4j server didn't stop normally within ${NEO_STOP_TIMEOUT}s, killing it"
	scancel --batch --signal KILL "$job_id"
fi

printf "\n|==== Neo4j job stopped.\n\n"

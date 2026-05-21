#!/usr/bin/env bash
set -euo pipefail

NEO_TRACK_PATH=""

function usage() {
	cat <<EOT

Usage: $(basename "$0") --track <path>

Options:
  --track <path>          Base path used when starting the Neo4j server.
                          Reads <path>.jobid to find the SLURM job to cancel.
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

JOBID_FILE="${NEO_TRACK_PATH}.jobid"
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

printf "\n|==== Neo4j server stopped.\n\n"

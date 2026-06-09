#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
my_base_name="$(basename "${BASH_SOURCE[0]}" '.sh')"

# non-exported vars are passed down as arguments, while the exported vars are picked as
# such downstream too.
#
neo_ram="${NEO_RAM:-4}"
neo_cores="${NEO_CORES:-4}"
neo_time="${NEO_TIME:-02:00:00}"
export NEO_TRACK_PATH="${NEO_TRACK_PATH:-neo-server}"
export NEO_STOP_TIMEOUT="${NEO_STOP_TIMEOUT:-120}"
export sbatch_opts="${KETL_SBATCH_OPTS:-""}"

function usage() {
  cat <<EOT


Usage: ${my_base_name}.sh [OPTIONS]

Starts a Neo4j server as a SLURM job.

WARNING: likely, you require to set JAVA_HOME and NEO4J_HOME in advance. The script
just tries to launch 'neo4j' if it can't find NEO4J_HOME.

Options:

	--ram <num>             RAM requested for the running node, in GB (default: $neo_ram)
	--cores <num>           Number of CPU cores requested for the running node (default: $neo_cores)
	--time <duration>       SLURM time limit (passed to sbatch --time), e.g. 02:00:00 
													(default: $neo_time)
	--stop-timeout <secs>   Shutdown timeout in seconds (default: $NEO_STOP_TIMEOUT). When stopping
	                        Neo, it is killed with SIGKILL if it doesn't stop within this time 
													more gently.
	--track <path>          Base path for tracking the cluster nodes/jobs:
														<path>.host  	— Neo4j running node
														<path>.jobid  — SLURM job ID 
													Default: $NEO_TRACK_PATH
	--sbatch <option>       <option> is passed to the 'sbatch' command (can override #SBATCH)
	--help                  Show this help

EOT
}


while [[ $# -gt 0 ]]; do
	case "$1" in
		--ram)          neo_ram="$2";          shift 2 ;;
		--cores)        neo_cores="$2";        shift 2 ;;
		--time)         neo_time="$2";         shift 2 ;;
		--track)        NEO_TRACK_PATH="$2";   shift 2 ;;
		--stop-timeout)  NEO_STOP_TIMEOUT="$2"; shift 2 ;;
		--sbatch)       sbatch_opts+=" $2";     shift 2 ;;
		--help)         usage; exit 0 ;;
		*) echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
	esac
done


sbatch_path="$script_dir/${my_base_name}.sbatch"

printf "\n\n|==== Starting Neo4j\n\n"

# Show and capture the output
exec 3>&1
submit_out=$(sbatch \
  --cpus-per-task="$neo_cores" \
  --mem="${neo_ram}G" \
  --time="$neo_time" \
  $sbatch_opts \
  "$sbatch_path" \
  | tee >(cat >&3) \
)

job_id="$(echo "$submit_out" | grep -oP '(?<=Submitted batch job )\d+')"

if [[ -n "$NEO_TRACK_PATH" && -n "$job_id" ]]; then
  echo "$job_id" > "${NEO_TRACK_PATH}.jobid"
  echo "|== Job ID $job_id written to ${NEO_TRACK_PATH}.jobid"
  echo "|== Host being written to ${NEO_TRACK_PATH}.host"
fi

printf "\n|==== Neo4j server started\n\n"

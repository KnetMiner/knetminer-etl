#!/usr/bin/env bash
#SBATCH --job-name=neo4j-server
#SBATCH --output=neo4j-server.out
#SBATCH --error=neo4j-server.out

set -euo pipefail

# Where is neo?
neo_cmd=neo4j
if [[ -n "${NEO4J_HOME:-}" ]]; then
	neo_cmd="$NEO4J_HOME/bin/neo4j"
fi

# What's my node?
host=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -n 1)

# Save it
if [[ -n "$NEO_TRACK_PATH" ]]; then
	echo "$host" > "${NEO_TRACK_PATH}.host"
fi

# scancel must be intercepted and managed
function cleanup ()
{
	echo "|== Shutting down Neo4j server, process $neo_pid"

	if ! timeout "$NEO_STOP_TIMEOUT" kill -TERM "$neo_pid"; then
		echo "|== Neo4j server didn't stop normally within ${NEO_STOP_TIMEOUT}s, killing it"
		kill -KILL "$neo_pid"
	fi

	if [[ -n "${NEO_TRACK_PATH:-}" ]]; then
		echo "|== Removing Neo4j server tracking files..."
		rm -f "${NEO_TRACK_PATH}".{host,jobid}
	fi
		
	echo "|== Neo4j server stopped, quitting the SLURM job too."
	exit
}

# Let's go, '&' + wait allows Neo the time to terminate
$neo_cmd console &
neo_pid=$!
trap cleanup INT

wait

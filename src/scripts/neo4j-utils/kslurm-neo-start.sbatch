#!/usr/bin/env bash
#SBATCH --job-name=neo4j-server
#SBATCH --output=neo4j-server.log
#SBATCH --error=neo4j-server.log

set -euo pipefail

# Where is neo?
neo_cmd=neo4j
if [[ -n "${NEO4J_HOME:-}" ]]; then
	neo_cmd="$NEO4J_HOME/bin/neo4j"
fi

# What's my node?
host=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -n 1)

$neo_cmd start
neo_pid=$!

# Just in case
sleep 5

if [[ -n "$NEO_TRACK_PATH" ]]; then
  echo "$host" > "${NEO_TRACK_PATH}.host"
fi

# Wait until scancel
wait $neo_pid

echo "|== Shutting down Neo4j server"

# Clean up
if [[ -n "$NEO_TRACK_PATH" ]]; then
	echo "|== Removing Neo4j server tracking files..."
	rm -f "${NEO_TRACK_PATH}.host" "${NEO_TRACK_PATH}.jobid"
fi

echo "|== Neo4j server stopped"

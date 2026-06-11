#!/usr/bin/env bash
set -euo pipefail

cmds="setup|run|teardown"
cmd="$(basename -- "$1" .sbatch)"
if [[ $# -lt 1 ]] || [[ "$1" = "--help" ]] || [[ ! "$cmd" =~ ^($cmds)$ ]]; then
	printf "\n\n  Usage: $0 <$cmds>[.sbatch] [options]\n\n" >&2
	exit 1
fi
cmd="$1"
shift

# Get some config vars from the env script

mypath="$(realpath "$BASH_SOURCE[0]")"
mydir="$(dirname "$mypath")"

cd "$mydir"
. env.sh

cd "$mydir"

# Then run the command on SLURM, so that we can get access to cluster resources like the
# Python module.
#
srun $KETL_SBATCH_OPTS ./${cmd}.sbatch "$@"

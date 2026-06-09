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

mypath="$(realpath "$BASH_SOURCE[0]")"
mydir="$(dirname "$mypath")"

cd "$mydir"
. env.sh

cd "$mydir"
srun $KETL_SBATCH_OPTS ./${cmd}.sbatch "$@"

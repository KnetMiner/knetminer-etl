#!/usr/bin/env bash
set -euo pipefail

cmds="setup|run|teardown"
if [[ $# -lt 1 ]] || [[ "$1" = "--help" ]] || [[ ! "$1" =~ ^($cmds)$ ]]; then
	echo "Usage: $0 <$cmds> [options]" >&2
	exit 1
fi
cmd="$1"
shift

mypath="$(realpath "$BASH_SOURCE[0]")"
mydir="$(dirname "$mypath")"

cd "$mydir"
. env.sh

srun $KETL_SBATCH_OPTS ./$cmd.sbatch "$@"


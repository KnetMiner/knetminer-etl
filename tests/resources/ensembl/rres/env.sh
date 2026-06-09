KETL_PRJ_HOME="$(realpath "${BASH_SOURCE[0]}")"
KETL_PRJ_HOME="$(dirname "$KETL_PRJ_HOME")"
export WF_HOME="$(realpath "$KETL_PRJ_HOME/..")"

KETL_PRJ_HOME="$(realpath "$KETL_PRJ_HOME/../../../..")"
export KETL_PRJ_HOME

export KNET_HOME=/home/data/knetminer
export JAVA_HOME="$KNET_HOME/software/jdk21"
export NEO4J_HOME="$(realpath "$KETL_PRJ_HOME/../neo4j")"

export NEO_TRACK_PATH="$WF_HOME/neo-server"
export SPARK_TRACK_PATH="$WF_HOME/spark-server"

export NEO4J_PASSWORD="testTest"
if [[ -e "$NEO_TRACK_PATH.host" ]]; then
	# The port is hardcoded in neo4j.conf
	export NEO4J_URL="bolt://$(cat "$NEO_TRACK_PATH.host"):8687"
fi

if [ -e "$SPARK_TRACK_PATH.host" ]; then
	export SPARK_MASTER="$(cat "$SPARK_TRACK_PATH.host"):$(cat "$SPARK_TRACK_PATH.port")"
fi

cd "$KETL_PRJ_HOME"

[[ -z "$PYTHONPATH" ]] || PYTHONPATH="$PYTHONPATH:"
export PYTHONPATH="${PYTHONPATH}${WF_HOME}"

# This is a bad guy, who often gets jobs delivered, but it refuses them with execve error
export KETL_SBATCH_OPTS="${KETL_SBATCH_OPTS:-"-x rothhpc407"}"


# The following makes sense only when we're in a SBATCH job
[[ -n "${SLURM_JOBID:-}" ]] || exit

# We don't use the Snake module, since it forced Python 3.10, and that's too old.
# setup.sbatch can install Snake into the current venv
module load git Python/3.12.3-GCCcore-13.3.0
[[ -e venv ]] && . venv/bin/activate \
	|| printf "|== Virtual env not found at \"%s\", Use --update if needed" "$KETL_PRJ_HOME/venv"

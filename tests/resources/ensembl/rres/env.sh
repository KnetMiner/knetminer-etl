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

cd "$KETL_PRJ_HOME"
module load Python/3.12.3-GCCcore-13.3.0 git
[[ -e venv ]] && . venv/bin/activate \
	|| printf "|== Virtual env not found at \"%s\", Use --update if needed" "$KETL_PRJ_HOME/venv"

[[ -z "$PYTHONPATH" ]] || PYTHONPATH="$PYTHONPATH:"
export PYTHONPATH="${PYTHONPATH}${WF_HOME}"

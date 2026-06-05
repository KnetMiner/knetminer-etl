export KNET_HOME=/home/data/knetminer
export WF_HOME="$KNET_HOME/software/knetminer-nova/knetminer-etl-test"
export JAVA_HOME="$KNET_HOME/software/jdk21"
export NEO4J_HOME="$WF_HOME/neo4j"

export NEO_TRACK_PATH="$WF_HOME/neo-server"
export SPARK_TRACK_PATH="$WF_HOME/spark-server"

cd "$WF_HOME"
module load Python/3.12.3-GCCcore-13.3.0 git
. .venv/bin/activate

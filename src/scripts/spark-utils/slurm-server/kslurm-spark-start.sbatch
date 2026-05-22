#!/usr/bin/env bash
#SBATCH --job-name=spark-server
#SBATCH --output=spark-server.out
#SBATCH --error=spark-server.out

set -euo pipefail

# Where is Spark, exactly?
spark_class_cmd="spark-class"
if [[ -n "${SPARK_HOME:-}" ]]; then
  spark_class_cmd="$SPARK_HOME/bin/${spark_class_cmd}"
fi

# What did I get from SLURM?
master_host=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -n 1)
worker_nodes=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | tail -n +2)

all_pids=""

echo "|== Starting Spark master on $master_host (job $SLURM_JOB_ID)"

# Start master on this node (the job's first node)
"$spark_class_cmd" org.apache.spark.deploy.master.Master \
  --host "$master_host" \
  --port "$SPARK_PORT" \
  --webui-port "$SPARK_WEB_PORT" &

all_pids=$!

# Give the master a moment to bind its port before workers connect
sleep 5

# Write track files once the master is live
if [[ -n "$SPARK_TRACK_PATH" ]]; then
  echo "$master_host"          > "${SPARK_TRACK_PATH}.master"
  echo "$SPARK_PORT"           > "${SPARK_TRACK_PATH}.port"
fi

# Start one worker per allocated worker node
spark_process_ram=$((SPARK_RAM - 1)) # Leave 1 GB for overhead
for node in $worker_nodes; do
  echo "|== Starting Spark worker on $node"
  srun --nodes=1 --ntasks=1 -w "$node" \
    "$spark_class_cmd" org.apache.spark.deploy.worker.Worker \
      "spark://${master_host}:${SPARK_PORT}" \
      --cores "$SPARK_CORES" \
      --memory "${spark_process_ram}G" &
  all_pids="$all_pids $!"
done


# Catch this when scancel, so that we can finalise it
function cleanup()
{
  echo "|== Shutting down the Spark cluster"

  kill -TERM $all_pids
  # Try to wait for their exit
  timeout "$SPARK_STOP_TIMEOUT" bash -c \
    "while kill -0 $all_pids 2>/dev/null; do sleep 5; done"
  
  # If you're still around, you must die!
  if kill -0 $all_pids 2>/dev/null; then
    echo "|== Spark cluster didn't stop normally within ${SPARK_STOP_TIMEOUT}s, killing it"
    kill -KILL $all_pids
  fi

  # Clean up track files
  if [[ -n "$SPARK_TRACK_PATH" ]]; then
    echo "|== Removing Spark cluster tracking files"
    rm -f "${SPARK_TRACK_PATH}.master" "${SPARK_TRACK_PATH}.port" "${SPARK_TRACK_PATH}.jobid"
  fi

  echo "|== Spark cluster stopped"
}
trap cleanup INT

# Keep the job alive until SLURM time limit or scancel
wait 

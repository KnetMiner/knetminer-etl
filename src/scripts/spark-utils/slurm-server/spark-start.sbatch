#!/usr/bin/env bash
#SBATCH --job-name=spark-server
#SBATCH --output=spark-server-%j.log
#SBATCH --error=spark-server-%j.log

set -euo pipefail

master_host=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -n 1)
worker_nodes=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | tail -n +2)

echo "Starting Spark master on $master_host (job $SLURM_JOB_ID)"

# Start master on this node (the job's first node)
"$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.master.Master \
  --host "$master_host" \
  --port "$SPARK_PORT" \
  --webui-port "$SPARK_WEB_PORT" &

MASTER_PID=$!

# Give the master a moment to bind its port before workers connect
sleep 5

# Write track files once the master is live
if [[ -n "$TRACK_PATH" ]]; then
  echo "$master_host"          > "${TRACK_PATH}.master"
  echo "$SPARK_PORT"           > "${TRACK_PATH}.port"
fi

# Start one worker per allocated worker node
spark_process_ram=$((SPARK_RAM - 1)) # Leave 1 GB for overhead
for node in $worker_nodes; do
  echo "Starting Spark worker on $node"
  srun --nodes=1 --ntasks=1 -w "$node" \
    "$SPARK_HOME/bin/spark-class" org.apache.spark.deploy.worker.Worker \
      "spark://${master_host}:${SPARK_PORT}" \
      --cores "$CORES" \
      --memory "${spark_process_ram}G" &
done

# Keep the job alive until SLURM time limit or scancel
wait $MASTER_PID

# Clean up track files on normal exit
if [[ -n "$TRACK_PATH" ]]; then
  rm -f "${TRACK_PATH}.master" "${TRACK_PATH}.port" "${TRACK_PATH}.jobid"
fi

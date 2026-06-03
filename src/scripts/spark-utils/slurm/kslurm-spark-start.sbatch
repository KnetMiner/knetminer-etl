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

echo "|== Starting Spark master on $master_host (job $SLURM_JOB_ID)"

# Start master on this node (the job's first node)
"$spark_class_cmd" org.apache.spark.deploy.master.Master \
  --host "$master_host" \
  --port "$SPARK_PORT" \
  --webui-port "$SPARK_WEB_PORT" &

master_pid=$!

# Give the master a moment to bind its port before workers connect
sleep 5

# Write track files once the master is live
if [[ -n "$SPARK_TRACK_PATH" ]]; then
  echo "$master_host"          > "${SPARK_TRACK_PATH}.master"
  echo "$SPARK_PORT"           > "${SPARK_TRACK_PATH}.port"
fi

function track_step_id ()
{
  # Tracks the job ID of a step, after having waited for its start.
  # Used below.

  # TODO: introduce srun --parsable when available

  step_name="$1"
  while true; do
    step_id=$(
      squeue -h -s -j "$SLURM_JOB_ID" -o "%i %j" \
        | awk -v n="$step_name" '$2 == n {print $1; exit}'
    )

    [[ -n "$step_id" ]] && break
    sleep 0.5s
  done
  echo "$step_id"
}

# Start one worker per allocated worker node
#
worker_job_ids=""
spark_process_ram=$((SPARK_RAM - 1)) # Leave 1 GB for overhead
for node in $worker_nodes; do
  echo "|== Starting Spark worker on $node"
  step_name="spark_worker_${node}_$$"
  srun --nodes=1 --ntasks=1 -w "$node" --job-name="$step_name" \
    "$spark_class_cmd" org.apache.spark.deploy.worker.Worker \
      "spark://${master_host}:${SPARK_PORT}" \
      --cores "$SPARK_CORES" \
      --memory "${spark_process_ram}G" &
done

# Track their job IDS separately, so that the launches remain async
for node in $worker_nodes; do
  step_name="spark_worker_${node}_$$"
  worker_job_id=$(track_step_id "$step_name")
  worker_job_ids+=" $worker_job_id"
done


# Catch this when scancel, so that we can finalise it
function cleanup()
{
  echo "|== Shutting down the Spark cluster"

  kill $master_pid
  scancel --batch "$worker_job_ids"

  # Try to wait for the master exit
  timeout "$SPARK_STOP_TIMEOUT" bash -c "while kill -0 $master_pid &>/dev/null; do sleep 1; done"
  # Still around?
  if kill -0 "$master_pid" &>/dev/null; then
    echo "|== Spark master didn't stop normally within ${SPARK_STOP_TIMEOUT}s, killing it"
    kill -KILL "$master_pid"
  fi

  # Now wait for the workers to stop too
  timeout "$SPARK_STOP_TIMEOUT" bash -c "while squeue --jobs $worker_job_ids &>/dev/null; do sleep 1; done"
  # Still around?
  if squeue --jobs "$worker_job_ids" &>/dev/null; then
    echo "|== Spark worker jobs didn't stop normally within ${SPARK_STOP_TIMEOUT}s, killing them"
    scancel --batch --signal KILL "$worker_job_ids"
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

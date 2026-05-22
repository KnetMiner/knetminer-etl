#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
my_base_name="$(basename "${BASH_SOURCE[0]}" '.sh')"

export SPARK_NODES=2
export SPARK_CORES=4
export SPARK_RAM=4
export SPARK_TIME="02:00:00"
export SPARK_TRACK_PATH="spark-server"
export SPARK_PORT=7077
export SPARK_WEB_PORT=8080
export SPARK_STOP_TIMEOUT=120
sbatch_opts=""

function usage() {
  cat <<EOT


Usage: ${my_base_name}.sh [OPTIONS]

Start a Spark standalone cluster as a SLURM job.

Options:

  --nodes <num>           Number of Spark nodes (default: $SPARK_NODES)
  --cores <num>           Cores per node (default: $SPARK_CORES)
  --ram <num>             RAM per node in GB (default: $SPARK_RAM)
                          (the Spark process is given 1 GB less)
  --time <duration>       SLURM time limit (passed to sbatch --time), e.g. 02:00:00 
                          (default: $SPARK_TIME)
  --track <path>          Base path for tracking the cluster nodes/jobs:
                            <path>.master  — Spark master hostname
                            <path>.port    — Spark master port
                            <path>.jobid   — SLURM job ID that started the cluster (through this script)
                          Default: $SPARK_TRACK_PATH
  --port <num>            Spark master port (default: $SPARK_PORT)
  --web-port <num>        Spark master web UI port (default: $SPARK_WEB_PORT)
  --stop-timeout <num>    Timeout in seconds to wait for Spark cluster to stop (default: $SPARK_STOP_TIMEOUT)
                          The cluster is killed if it doesn't stop within this time.
  --sbatch <option>       <option> is passed to the 'sbatch' command (can override #SBATCH)
  --help                  Show this help


WARNING: probably you need setup as follow before this script: 

export JAVA_HOME=...
module load Python/3.12.3-GCCcore-13.3.0
<your-virtual-environment>/bin/activate # Do this BEFORE module load

With this setup, usually you don't need SPARK_PATH, since it automatically picks the Spark locations
from the venv.

EOT
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nodes)        SPARK_NODES="$2";        shift 2 ;;
    --cores)        SPARK_CORES="$2";        shift 2 ;;
    --ram)          SPARK_RAM="$2";          shift 2 ;;
    --time)         SPARK_TIME="$2";         shift 2 ;;
    --track)        SPARK_TRACK_PATH="$2";   shift 2 ;;
    --port)         SPARK_PORT="$2";         shift 2 ;;
    --web-port)     SPARK_WEB_PORT="$2";     shift 2 ;;
    --stop-timeout) SPARK_STOP_TIMEOUT="$2"; shift 2 ;;
    --sbatch)       sbatch_opts+=" $2";     shift 2 ;;
    --help)         usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
  esac
done

sbatch_path="$script_dir/${my_base_name}.sbatch"

printf "\n\n|==== Starting the Spark cluster\n\n"

# Show and capture the output
exec 3>&1
submit_out=$(sbatch \
  --nodes="$SPARK_NODES" \
  --cpus-per-task="$SPARK_CORES" \
  --mem="${SPARK_RAM}G" \
  --time="$SPARK_TIME" \
  $sbatch_opts \
  "$sbatch_path" \
  | tee >(cat >&3) \
)

job_id="$(echo "$submit_out" | grep -oP '(?<=Submitted batch job )\d+')"

if [[ -n "$SPARK_TRACK_PATH" && -n "$job_id" ]]; then
  echo "$job_id" > "${SPARK_TRACK_PATH}.jobid"
  echo "|== Job ID $job_id written to ${SPARK_TRACK_PATH}.jobid"
  echo "|== Master host/port being written to ${SPARK_TRACK_PATH}.{master|port}"
fi

printf "\n|==== Spark cluster started\n\n"

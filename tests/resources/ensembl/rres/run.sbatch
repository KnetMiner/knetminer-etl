#!/usr/bin/env bash
#SBATCH --job-name=ensembl-snake
#SBATCH --output=snake.out
#SBATCH --error=snake.out
#SBATCH --cores=8
#SBATCH --time=02:00:00

set -euo pipefail

printf "\n\n|==== Running the Snakemake workflow\n"

mypath="$(scontrol show job $SLURM_JOBID | awk -F= '/Command=/{print $2}')"
mypath="$(realpath "$mypath")"
mydir="$(dirname "$mypath")"

cd "$mydir"
. env.sh

cd "$WF_HOME"
snakemake -s workflow.snakefile --cores all all

printf "\n\n|==== Snakemake workflow completed\n"

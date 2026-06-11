# The ENSEMBL Workflow RRes Runner

These scripts are used to run the [ENSEMBL test workflow](../workflow.snakefile) on the Rothamsted Research SLURM cluster.

It shows how to manage a Spark (standalone) cluster on SLURM and let a KETL workflow send 
jobs to it.

[cmd.sh](cmd.sh) is the main entry point, which deals with controlling the Neo and Spark servers needed by the workflow and the workflow itself. Each of these component are run as
SLURM jobs, since we have Python available under the cluster nodes.


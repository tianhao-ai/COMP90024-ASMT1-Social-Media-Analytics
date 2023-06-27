#!/bin/bash

#SBATCH --job-name=search_twitter_with_1_node_1_core
#SBATCH --output=job_1_output.txt
#SBATCH --error=job_1_error.txt
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --time=0-12:00:00

module load python/3.7.4
module load foss/2019b
module load mpi4py/3.0.2-timed-pingpong
source ~/virtualenv/python3.7.4/bin/activate

mpiexec -n 1 python twitterSearch.py bigTwitter.json
my-job-stats -a -n -s
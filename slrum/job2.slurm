#!/bin/bash

#SBATCH --job-name=search_twitter_with_1_node_8_cores
#SBATCH --output=job_2_output.txt
#SBATCH --error=job_2_error.txt
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --time=00-12:00:00 # days-hh:mm:ss

module load python/3.7.4
module load foss/2019b
module load mpi4py/3.0.2-timed-pingpong
source ~/virtualenv/python3.7.4/bin/activate

srun -n 8 python3 twitterSearch.py bigTwitter.json
my-job-stats -a -n -s
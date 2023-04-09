#!/bin/bash

#SBATCH --time=0:15:0

module load python/3.7.4

mkdir ~/virtualenv

virtualenv ~/virtualenv/python3.7.4

source ~/virtualenv/python3.7.4/bin/activate

pip install ijson

deactivate
#!/bin/bash

#SBATCH -J example1        # job name
#SBATCH -A xxxxxx-xxxxxx   # fund account number
#SBATCH -p orion           # partition name
#SBATCH -q normal          # queue name
#SBATCH -N 1               # number of nodes
#SBATCH -n 1               # number of tasks (a default task uses 1 core/CPU)
#SBATCH -t 00:00:30        # amount of time allowed before the job times out 
#SBATCH -o outputs/%x.out  # name of SLURM output file
#SBATCH -e errors/%x.err   # name of SLURM error file


echo "running helloworld.f"
./../fortran_programs/hw77


echo "running helloworld.f90"
./../fortran_programs/hw90

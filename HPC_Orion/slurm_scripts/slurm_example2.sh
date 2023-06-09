#!/bin/bash

#SBATCH -J example2           # job name
#SBATCH -A xxx                # fund account number
#SBATCH -p 400p48h            # partition name
#SBATCH -q normal             # queue name
#SBATCH --array=0-99          # execute batch script 100 times simultaneously, each time using 1 task/CPU
#SBATCH -t 00:00:30           # amount of time allowed before the job times out 
#SBATCH -o outputs/%x_%a.out  # name of SLURM output file, includes the array/task id
#SBATCH -e errors/%x_%a.err   # name of SLURM error file, includes the array/task id


# print some info at each execution
echo ${SLURMD_NODENAME}
echo ${SLURM_ARRAY_TASK_ID}

# read file of scale factors
readarray -t sf < ../inputs/dummy_scalefactors.txt

# execute the program
# passing a unique array task id and scale factor to the program
echo "running readwrite.f90"
./../fortran_programs/rw90 ${SLURM_ARRAY_TASK_ID} ${sf[${SLURM_ARRAY_TASK_ID}]}

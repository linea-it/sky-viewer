#!/bin/bash
#SBATCH --job-name=hipsgen_fits
#SBATCH --output=/lustre/t0/scratch/users/%u/hipsgen_cat/logs_of_submission/hipsgen_fits_%j.out
#SBATCH --error=/lustre/t0/scratch/users/%u/hipsgen_cat/logs_of_submission/hipsgen_fits_%j.out
#SBATCH --partition=cpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=100
#SBATCH --mem=240GB
#SBATCH --time=2-00:00:00
#SBATCH --account=hpc-bpglsst

# Activate the Conda environment
source /lustre/t0/scratch/users/$USER/miniconda3/etc/profile.d/conda.sh
conda activate /lustre/t0/scratch/users/$USER/java_env

# Run the Python script
python /lustre/t0/scratch/users/$USER/hipsgen_cat/hipsgen_fits_input.py

#!/bin/bash
#SBATCH --job-name=hipsgen_csv
#SBATCH --output=/scratch/users/%u/sky-viewer/docs/hipsgen_cat_scripts/DP1/logs_of_submission/hipsgen_csv_%j.out
#SBATCH --error=/scratch/users/%u/sky-viewer/docs/hipsgen_cat_scripts/DP1/logs_of_submission/hipsgen_csv_%j.out
#SBATCH --partition=cpu
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=50
#SBATCH --mem=100GB
#SBATCH --time=2-00:00:00
#SBATCH --account=hpc-bpglsst

set -euo pipefail

# --- Initialize Conda in batch shells (no interactive rc files) ---
# (Ajuste o caminho para o seu conda.sh real)
CONDA_SH="/scratch/users/$USER/ondemand/miniconda/etc/profile.d/conda.sh"
if [ -f "$CONDA_SH" ]; then
  source "$CONDA_SH"
else
  # Fallback universal: usa o "hook" do conda
  eval "$(/scratch/users/$USER/ondemand/miniconda/bin/conda shell.bash hook)"
fi

# --- Activate your env by prefix path ---
conda activate /scratch/users/$USER/ondemand/java_env

# Opcional: debug
conda info --envs
python -V
java -version

# Run the Python script
python /scratch/users/$USER/sky-viewer/docs/hipsgen_cat_scripts/DP1/hipsgen_csv_input.py

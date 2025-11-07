#!/bin/bash
#SBATCH --job-name=hipsgen
#SBATCH --output=hipsgen_%j.out
#SBATCH --error=hipsgen_%j.err
#SBATCH --time=08:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --partition=cpu
#SBATCH --account=hpc-bpglsst

# =============================================================================
# HiPS Catalog Pipeline — SLURM submission script
# -----------------------------------------------------------------------------
# This template runs the HiPS catalog generation pipeline on LIneA’s Apollo cluster.
#
# Before submitting, make sure to:
#   1) Adjust the 'partition' (--partition) and 'account' (--account) directives
#      according to your access permissions.
#      → If you don’t have access to 'hpc-bpglsst', choose another valid account.
#      → For details on partitions, accounts, and job submission, see:
#        https://docs.linea.org.br/processamento/apollo/index.html
#   2) Update the paths for your Conda environment and code directory below.
# =============================================================================

# -----------------------------------------------------------------------------
# 1. Activate the Conda environment
# -----------------------------------------------------------------------------
# Replace the path below with the location of your environment.
# The environment must include Dask, LSDB, and all pipeline dependencies.
source /scripts/$USER/hipsgencat_env/bin/activate

# -----------------------------------------------------------------------------
# 2. Go to the pipeline directory
# -----------------------------------------------------------------------------
# Replace with the directory where Hipsgen-cat.py is located.
cd /scripts/$USER/sky-viewer/docs/hipsgen_python_parallelized

# -----------------------------------------------------------------------------
# 3. Run the HiPS Catalog Pipeline
# -----------------------------------------------------------------------------
# Adjust the path and name of the configuration file as needed.
python Hipsgen-cat.py --config config.yaml
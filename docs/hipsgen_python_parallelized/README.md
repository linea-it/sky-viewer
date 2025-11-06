# HiPS Catalog Pipeline (Dask + Parquet)

This pipeline generates HiPS-compliant catalog hierarchies from large input tables using Dask.  
It reproduces the general directory and metadata structure recognized by Aladin, following the logic of the CDS *hipsgen-cat.jar* tool, but implemented in Python for large-scale, parallelized workflows.

The pipeline:
- Reads large catalogs (Parquet or CSV/TSV) in a fully parallelized way.  
- Groups sources by HEALPix coverage cells (`coverage_order`) and performs uniform selection per HiPS depth (`level_limit`).  
- Applies density-based selection rules and probabilistic sampling (`fractional_mode`).  
- Produces HiPS-compatible outputs, including per-depth tiles (`Norder*` directories), `Allsky.tsv`, MOC files (`Moc.fits` and `Moc.json`), and metadata descriptors (`properties`, `metadata.xml`).  
- Supports both low-memory (out-of-core) and high-throughput (in-memory) execution modes depending on cluster configuration.

---

## 1. Clone the repository

```bash
git clone https://github.com/linea-it/sky-viewer.git
```

---

## 2. Navigate to the pipeline directory

```bash
cd sky-viewer/docs/hipsgen_python_parallelized
```

---

## 3. Create and activate the Conda environment

If you are running this on an **OnDemand JupyterHub** environment,  
it is recommended to install Miniconda and create the environment **inside your `$SCRIPTS` area** to avoid quota or permission issues:

```bash
conda env create -f environment.yml -p $SCRIPTS/hipsgencat_env
conda activate $SCRIPTS/hipsgencat_env
```

If you are running locally or on a standard system, simply create it with a name:

```bash
conda env create -f environment.yml --name hipsgencat_env
conda activate hipsgencat_env
```

If you do not have Miniconda or Anaconda, install Miniconda first from:  
https://docs.conda.io/en/latest/miniconda.html

---

## 4. Adjust configuration

Edit `config.yaml` to match your environment and data paths.

**Key points:**
- Set the input and output directories.
- Choose the cluster mode: `local` (LocalCluster) or `slurm` (SLURMCluster).
- Adjust the number of workers, memory, and parallelization settings.
- Optionally modify algorithm parameters such as `level_limit`, `coverage_order`, and `fractional_mode`.

Example configuration files are provided in the repository.

---

## 5. Run the pipeline

Execute the main script:

```bash
python Hipsgen-cat.py --config config.yaml
```

The pipeline will:
1. Read all input files and build HEALPix-based density maps for each HiPS depth.  
2. Generate a coverage MOC (`Moc.fits` and `Moc.json`) using `level_coverage`.  
3. Perform per-depth source selection according to uniform coverage and density profiles.  
4. Write hierarchical HiPS directories with completeness headers and metadata.  
5. Save process logs, arguments, and configuration snapshots in the output directory.

---

## Acknowledgment
This work was inspired by the HiPS Catalog tools developed at the CDS,  
whose design and public documentation provided valuable guidance for this independent reimplementation.

**References:**
- CDS (Strasbourg Astronomical Data Center): https://cds.unistra.fr/  
- HiPSgen-cat (official Java implementation): https://aladin.cds.unistra.fr/hips/Hipsgen-cat.gml
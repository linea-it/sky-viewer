# HiPS Catalog Pipeline (Dask + Parquet / HATS)

This pipeline generates HiPS-compliant catalog hierarchies from large input tables using Dask.  
It reproduces the general directory and metadata structure recognized by Aladin, following the logic of the CDS *Hipsgen-cat.jar* tool, but implemented in Python for large-scale, parallelized workflows.

The pipeline:
- Reads large catalogs in a fully parallelized way:
  - Standard files: Parquet or CSV/TSV.
  - LSDB / HATS catalogs: using `lsdb.open_catalog(...)` to keep the native partitioning.
- Groups sources by HEALPix coverage cells (`coverage_order`) and performs uniform selection per HiPS depth (`level_limit`).  
- Applies density-based selection rules and probabilistic sampling (`fractional_mode`).  
- Produces HiPS-compatible outputs, including per-depth tiles (`Norder*` directories), `Allsky.tsv`, MOC files (`Moc.fits` and `Moc.json`), and metadata descriptors (`properties`, `metadata.xml`).  
- Supports both low-memory (out-of-core) and high-throughput (in-memory) execution modes depending on cluster configuration.  
- When using HATS/LSDB catalogs, reuses the native HEALPix nested index when present (e.g. `_healpix_<order>`) to derive coverage and density maps efficiently.

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
conda env create -f environment.yaml -p $SCRIPTS/hipsgencat_env
conda activate $SCRIPTS/hipsgencat_env
```

If you are running locally or on a standard system, simply create it with a name:

```bash
conda env create -f environment.yaml --name hipsgencat_env
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
- Choose the input format:
  - `parquet`, `csv`, or `tsv` for standard flat files.
  - `hats` when reading a HATS/LSDB catalog with `lsdb` (in this case, `input.paths` must resolve to exactly one catalog).

Example configuration files are provided in the repository.

### Using HATS / LSDB catalogs as input

To use a HATS/LSDB catalog as input, set:

```yaml
input:
  paths:
    - "/path/to/catalog.hats"   # must resolve to exactly one catalog
  format: hats
```

Notes:

- The pipeline internally uses `lsdb.open_catalog(...)` and keeps a native `lsdb.catalog.Catalog` instead of a plain Dask DataFrame.
- If the catalog index is a nested HEALPix index named like `_healpix_<order>`, the pipeline:
  - Derives coverage cells (`__icov__`) by bit-shifting from the index to `coverage_order`.
  - Builds density maps directly from the index when possible, avoiding recomputing HEALPix indices from RA/DEC.
- All high-level selection logic (density profile, fractional selection, per-depth tiles, MOC, metadata) remains the same as for Parquet/CSV/TSV inputs.

---
## 5. Run the pipeline

Execute the main script directly:

```bash
python Hipsgen-cat.py --config config.yaml
```

The pipeline will:
1. Read all input files and build HEALPix-based density maps for each HiPS depth.  
2. Generate a coverage MOC (`Moc.fits` and `Moc.json`) using `level_coverage`.  
3. Perform per-depth source selection according to uniform coverage and density profiles.  
4. Write hierarchical HiPS directories with completeness headers and metadata.  
5. Save process logs, arguments, and configuration snapshots in the output directory.

When `input.format: hats` is used, the same steps apply, but some operations (such as coverage indexing and density maps) reuse the HEALPix index from the HATS catalog for better performance and consistency.

### Running on the LIneA HPC Cluster (Apollo)

If you are running this pipeline on the **LIneA HPC cluster (Apollo)**, you can submit it using **SLURM** instead of running it directly in the terminal.  
This ensures that the process continues even if your session is closed.

A template job submission script (`run_hips.sbatch`) is provided in this repository.  
It already includes typical SLURM directives, resource requests, and environment activation steps.  
Before running, make sure to:

- Adjust the paths to your Conda environment and pipeline directory.  
- Check the `--partition` and `--account` directives to match your available resources.  
  If you do not have access to the default account (`hpc-bpglsst`), use another one you are authorized for.  
  See the Apollo documentation for more details:  
  https://docs.linea.org.br/processamento/apollo/index.html

Submit the job with:

```bash
sbatch run_hips.sbatch
```

You can then monitor its progress with:

```bash
squeue -u $USER
```

---
## Acknowledgment
This work was inspired by the HiPS Catalog tools developed at the CDS,  
whose design and public documentation provided valuable guidance for this independent reimplementation.

**References:**
- CDS (Strasbourg Astronomical Data Center): https://cds.unistra.fr/  
- HiPSgen-cat (official Java implementation): https://aladin.cds.unistra.fr/hips/Hipsgen-cat.gml
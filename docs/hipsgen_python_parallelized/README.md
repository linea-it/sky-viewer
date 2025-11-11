# HiPS Catalog Pipeline (Dask + Parquet / HATS)

This pipeline generates HiPS-compliant catalog hierarchies from large input tables using Dask.  
It reproduces the general directory and metadata structure recognized by Aladin, following the logic of the CDS *Hipsgen-cat.jar* tool, but implemented in Python for large-scale, parallelized workflows.

The pipeline supports **two main selection modes**, each with distinct purposes and parameters.

---

## 1. Shared Core Functionality (common to both modes)

These options control the general HiPS hierarchy generation process and apply to both modes.

- **Inputs**: Parquet/CSV/TSV tables or HATS/LSDB catalogs (`input.format`).
- **HiPS depth control**:
  - `level_limit`: maximum HiPS order to write.
  - `level_coverage`: order used for the MOC and global coverage.
  - `coverage_order`: order for coverage cells (`__icov__`).
- **Parallelism**: full Dask-based execution, with cluster configuration under `cluster:`.
- **Outputs**:
  - Per-depth tiles (`Norder*/Dir*/Npix*.tsv`).
  - All-sky files (`Allsky.tsv`).
  - Density maps (`densmap_o<depth>.fits`).
  - Coverage maps (`Moc.fits`, `Moc.json`).
  - Metadata descriptors (`properties`, `metadata.xml`).
- **Reproducibility**:
  - Automatic log files, `arguments`, and YAML config snapshots.

---

## 2. Coverage-Based Mode (`selection_mode: coverage`)

This mode performs selection based on coverage cells (`__icov__`), applying density laws and optional fractional randomization.

### Parameters (exclusive to coverage mode)

- **Coverage partitioning**
  - `use_hats_as_coverage`: use HATS partitions instead of HEALPix cells (for HATS input only).
  - `coverage_order`: HEALPix order for coverage cells.

- **Score ordering**
  - `order_desc`:
    - `false` → ascending (lower score is better).
    - `true` → descending (higher score is better).

- **Density scaling**
  - `density_mode`: `"constant"`, `"linear"`, `"exp"`, or `"log"`.
  - `k_per_cov_initial`: expected rows per coverage cell at depth 1.
  - `targets_total_initial`: alternative to `k_per_cov_initial`.
  - `density_exp_base`: exponential growth base when `density_mode: exp`.

- **Biasing**
  - `density_bias_mode`: `"none"`, `"proportional"`, or `"inverse"`.
  - `density_bias_exponent`: power-law exponent controlling bias strength.

- **Overrides**
  - `k_per_cov_per_level`: manual overrides of `k_per_cov` per depth.
  - `targets_total_per_level`: global caps for total number of sources per depth.

- **Fractional sampling**
  - `fractional_mode`: `"random"` or `"score"`.
  - `fractional_mode_logic`: `"local"` or `"global"`.
  - `tie_buffer`: extra candidates per coverage cell to mitigate score ties.

### When to use

- When you want uniform spatial coverage and density-balanced sampling.
- When selection is driven by positional density or ranking scores.
- When you want hierarchical downsampling per HiPS order.

---

## 3. Global Magnitude-Complete Mode (`selection_mode: mag_global`)

This mode ensures a magnitude-complete catalog by analyzing the full magnitude distribution before per-depth selection.

### Parameters (exclusive to mag_global mode)

- **Magnitude column**
  - `mag_column`: column name used for global ordering and histogram.

- **Magnitude range**
  - `mag_min` / `mag_max`: brightness limits for completeness.
    - If omitted:
      - `mag_min` = lowest magnitude in the dataset.
      - `mag_max` = **mode (peak)** of the global magnitude histogram.

- **Histogram and quantiles**
  - `mag_hist_nbins`: number of bins used for global histogram (controls quantile precision).

- **Global targets**
  - `n_1`, `n_2`, `n_3`: optional approximate total target counts per depth.  
    Must be provided cumulatively (e.g. `n_2` requires `n_1`).

### When to use

- When you want **photometric completeness** rather than spatial uniformity.
- When you aim for consistent magnitude limits across depths.
- When physical brightness is the main selection variable.

---

## 4. Running the Pipeline

### Option 1 — As a library

```python
from hipsgen_cat import load_config, run_pipeline

cfg = load_config("config.yaml")
run_pipeline(cfg)
```

### Option 2 — From the command line

```bash
python -m hipsgen_cat.cli --config config.yaml
```

### Option 3 — On the LIneA Apollo cluster (SLURM)

Submit via SLURM job script:

```bash
sbatch run_hips.sbatch
```

Monitor progress:

```bash
squeue -u $USER
```

---

## 5. Output Structure

Each HiPS order (`NorderX`) includes:
- `Dir*/Npix*.tsv`: per-cell tiles with completeness headers.
- `Allsky.tsv`: all-sky view for that order.
- `densmap_o<depth>.fits`: density maps (depth < 13).
- `Moc.fits`, `Moc.json`: coverage maps.
- `metadata.xml`, `properties`: HiPS metadata descriptors.
- `arguments`, logs, and config snapshots for reproducibility.

---

## 6. Mode Comparison

| Feature | Coverage Mode | Mag_Global Mode |
|----------|----------------|----------------|
| Partition basis | HEALPix or HATS cells | Global (no spatial partition) |
| Main selection variable | Score / density | Magnitude |
| Completeness goal | Uniform sky density | Photometric completeness |
| Adaptive with depth | Yes (density laws) | Yes (global quantiles) |
| Parameters | `k_per_cov`, `density_mode`, `fractional_mode` | `mag_column`, `mag_min/max`, `n_1/n_2/n_3` |
| Typical use | Uniform sampling | Brightness-limited sampling |

---

## 7. Acknowledgment

This work was inspired by the HiPS Catalog tools developed at the CDS,  
whose design and public documentation provided valuable guidance for this independent reimplementation.

**References:**
- CDS (Strasbourg Astronomical Data Center): https://cds.unistra.fr/  
- HiPSgen-cat (official Java implementation): https://aladin.cds.unistra.fr/hips/Hipsgen-cat.gml

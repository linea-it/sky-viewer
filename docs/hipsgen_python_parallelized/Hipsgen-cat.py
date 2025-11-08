#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HiPS Catalog Pipeline (Dask + Parquet)
--------------------------------------

This script, ``Hipsgen-cat.py``, generates HiPS-compliant catalog hierarchies
from large input tables using Dask. Its design and output organization were
initially inspired by the HiPS Catalog tools developed at the
CDS (Strasbourg Astronomical Data Center).

Although this implementation started from the same conceptual foundation, it
was developed independently in Python and evolved to accommodate large-scale,
parallelized workflows based on Dask. Throughout its development, several design
choices led to some simplifications and also to structural and logical
adaptations compared to the original CDS implementation. Certain features
present in the CDS tools were intentionally left out, while new ones were
introduced to better support distributed computation and high-performance
catalog processing.

The resulting pipeline reproduces the general layout, metadata structure,
and MOC products recognized by Aladin, even though minor differences may exist
in the final directory organization or internal logic.

Acknowledgment:
    This work was inspired by the HiPS Catalog tools developed at the CDS,
    whose design and public documentation provided valuable guidance for this
    independent reimplementation.

References:
    - CDS (Strasbourg Astronomical Data Center): https://cds.unistra.fr/
    - HiPSgen-cat (official Java implementation): 
      https://aladin.cds.unistra.fr/hips/Hipsgen-cat.gml

Author: Luigi Silva
Affiliation: Data Scientist, LIneA (Laboratório Interinstitucional de e-Astronomia)
Contact: luigi.silva@linea.org.br

Usage:
    python Hipsgen-cat.py --config config.yaml
"""

from __future__ import annotations

# =============================================================================
# Standard library
# =============================================================================
import argparse
import glob
import json
import math
import os
import re
import shutil
import sys
import textwrap
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Any

# =============================================================================
# Third-party libraries
# =============================================================================
import dask

import dask.dataframe as dd
import numpy as np
import pandas as pd
import yaml
import healpy as hp

import lsdb
from lsdb.catalog import Catalog as LsdbCatalog

from dask import compute as dask_compute
from dask import delayed as _delayed
from dask.distributed import Client, LocalCluster, wait, performance_report
from contextlib import nullcontext

try:
    from dask_jobqueue import SLURMCluster  # optional for cluster execution
except Exception:
    SLURMCluster = None

from astropy.io import fits
from astropy.io.votable import from_table, writeto as vot_writeto
from astropy.table import Table
from mocpy import MOC

dask.config.set({"dataframe.shuffle.method": "tasks"})

# =============================================================================
# Internal aliases and numpy underscore variant (for clarity in delayed ops)
# =============================================================================
import numpy as _np


# =============================================================================
# Configuration dataclasses
# =============================================================================

@dataclass
class AlgoOpts:
    """
    Algorithm options controlling the coverage-based HiPS catalog selection.

    The logic in the default mode is:
      * Sources are grouped by a HEALPix "coverage" order (coverage_order → __icov__),
        or by HATS partitions when use_hats_as_coverage=True.
      * At each HiPS level (depth), a target number of rows per coverage cell
        is computed from a density profile (density_mode, k_per_cov_initial),
        optionally overridden by per-level settings (k_per_cov_per_level) and
        by global per-level caps (targets_total_per_level).
      * Within each coverage cell, rows are ranked by the score expression and
        truncated according to the desired density.

    Additionally, instead of specifying a per-coverage initial density
    (k_per_cov_initial), the user may opt to specify an initial *total*
    target per level (targets_total_initial). The two parameters are
    conceptually equivalent and mutually exclusive at the configuration
    level: only one of them should be provided in the YAML.
    """
    # HiPS / coverage geometry
    level_limit: int              # maximum HiPS order (NorderL)
    level_coverage: int           # MOC / coverage order (lC)
    order_desc: bool              # False -> ascending score (lower is better)
    coverage_order: int           # HEALPix order for __icov__ coverage cells (default mode)

    # Optional per-level overrides for k (hard overrides by depth).
    # Example: {3: 0.6, 4: 1.2}
    k_per_cov_per_level: Optional[Dict[int, float]] = None

    # Optional global total caps per level (total rows per level).
    targets_total_per_level: Optional[Dict[int, int]] = None

    tie_buffer: int = 10          # score tie-buffer near cut (helps avoid artifacts)

    # -------------------------
    # Density profile controls
    # -------------------------
    # How k (or total targets) varies with depth:
    #   "constant" → same base value at all depths
    #   "linear"   → increases linearly with depth
    #   "exp"      → increases exponentially with depth
    #   "log"      → increases ~log(depth)
    density_mode: str = "constant"

    # Expected rows per coverage cell (__icov__) at depth=1
    # (base of the density profile in per-coverage mode).
    k_per_cov_initial: float = 1.0

    # Expected *total* number of rows at depth=1 (base of the density
    # profile in total-target mode). When this is not None, the per-depth
    # density is derived as:
    #   T_desired(depth) → k_desired(depth) = T_desired / N_cov
    # and k_per_cov_initial is ignored by the density profile.
    targets_total_initial: Optional[float] = None

    # Base used only when density_mode == "exp".
    density_exp_base: float = 2.0

    # How to handle the fractional part of k:
    # "random" -> per-coverage random +1 (uniformity-oriented)
    # "score"  -> global best-score selection (may break uniformity)
    fractional_mode: str = "random"

    # When True (and input.format == "hats"), use the HATS/LSDB partitions
    # themselves as coverage cells (__icov__), instead of HEALPix cells.
    use_hats_as_coverage: bool = False


@dataclass
class ColumnsCfg:
    ra: str       # RA column name (or index string for ASCII without header)
    dec: str      # DEC column name
    score: str    # score expression or column used for ranking
    keep: Optional[List[str]] = None  # optional explicit list of columns to keep


@dataclass
class InputCfg:
    paths: List[str]        # list of glob patterns for files
    format: str             # 'parquet' | 'csv' | 'tsv'
    header: bool            # header row present for CSV/TSV
    ascii_format: Optional[str] = None  # optional hint ('CSV' or 'TSV')


@dataclass
class ClusterCfg:
    mode: str                  # 'local' | 'slurm'
    n_workers: int
    threads_per_worker: int
    memory_per_worker: str     # e.g. "8GB"
    slurm: Optional[Dict] = None
    persist_ddfs: bool = False  # default: low-memory mode
    avoid_computes_wherever_possible: bool = True  # default: low-memory mode
    diagnostics_mode: str = "per_step"  # "per_step" | "global" | "off"


@dataclass
class OutputCfg:
    out_dir: str
    cat_name: str
    target: str                # "<RA0> <DEC0>" for properties


@dataclass
class Config:
    input: InputCfg
    columns: ColumnsCfg
    algorithm: AlgoOpts
    cluster: ClusterCfg
    output: OutputCfg


# =============================================================================
# Utilities
# =============================================================================

def _mkdirs(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def _write_text(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def _now_str() -> str:
    return time.strftime("%d/%m/%y %H:%M:%S %Z", time.localtime())


def _ts() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def _fmt_dur(seconds: float) -> str:
    s = int(seconds)
    ms = int(round((seconds - s) * 1000))
    h, rem = divmod(s, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}.{ms:03d}"


def _stats_counts(counts: np.ndarray) -> tuple[int, int]:
    """Return (total_rows, non_empty_pixels) for a densmap vector."""
    total = int(counts.sum())
    nonempty = int((counts > 0).sum())
    return total, nonempty


def _log_depth_stats(
    _log_fn: Callable[[str, bool], None],
    depth: int,
    phase: str,
    counts: Optional[np.ndarray] = None,
    candidates_len: Optional[int] = None,
    selected_len: Optional[int] = None,
    written: Optional[Dict[int, int]] = None,
    remainder_len: Optional[int] = None,
) -> None:
    """
    Print a compact one-line summary for a given depth and pipeline phase.
    `phase` examples: "start", "candidates", "selected", "written", "filtered".
    """
    parts = []
    if counts is not None:
        tot, nz = _stats_counts(counts)
        parts.append(f"input_rows={tot}")
        parts.append(f"non_empty_pixels={nz}")
    if candidates_len is not None:
        parts.append(f"candidates={candidates_len}")
    if selected_len is not None:
        parts.append(f"selected={selected_len}")
    if written is not None:
        rows_written = int(sum(written.values())) if written else 0
        tiles_written = int(len(written)) if written else 0
        parts.append(f"tiles_written={tiles_written}")
        parts.append(f"rows_written={rows_written}")
    if remainder_len is not None:
        parts.append(f"remainder={remainder_len}")
    _log_fn(f"[DEPTH {depth}] {phase}: " + "; ".join(parts), always=True)


# -----------------------------------------------------------------------------
# Score dependency extraction and column resolution
# -----------------------------------------------------------------------------
_ID_RE = re.compile(r"[A-Za-z_]\w*")
_HEALPIX_INDEX_RE = re.compile(r"_healpix_(\d+)$")

def _score_deps(score_expr: str, available: List[str]) -> List[str]:
    """
    Extract potential column names referenced by the score expression and
    keep only those that actually exist in the dataframe columns.
    Works for simple expressions like: 'COL1', '-MAG_I', '(FLUX_R/ERR_R)**2', etc.
    """
    if not score_expr:
        return []
    tokens = set(_ID_RE.findall(str(score_expr)))
    return [c for c in available if c in tokens]


def _resolve_col_name(spec: str, ddf: dd.DataFrame, header: bool) -> str:
    """
    Resolve a column spec that can be a name or a 1-based integer index (ASCII without header).
    - If 'header' is True, 'spec' must be an existing column name.
    - If 'header' is False and 'spec' is a number, map 1->col0, 2->col1...
    """
    if header:
        if spec not in ddf.columns:
            raise KeyError(f"Column '{spec}' not found in input.")
        return spec
    # header == False → allow numeric indexes (1-based like the tool docs imply)
    try:
        idx_1based = int(spec)
        idx = idx_1based - 1
        cols = list(ddf.columns)
        if not (0 <= idx < len(cols)):
            raise IndexError(f"Column index {idx_1based} out of range (1..{len(cols)})")
        return cols[idx]
    except ValueError:
        if spec not in ddf.columns:
            raise KeyError(f"Column '{spec}' not found (header=False).")
        return spec


def _get_meta_df(ddf_like: Any) -> pd.DataFrame:
    """
    Return an empty pandas.DataFrame with the same columns/dtypes as the
    given collection (Dask DataFrame or LSDB Catalog).

    Priority:
      1) If the object has ._meta (plain Dask DataFrame), use it.
      2) If the object has ._ddf (LSDB Catalog), use its ._meta.
      3) Otherwise, fall back to .head(0) if available.
    """
    # Plain Dask DataFrame
    if hasattr(ddf_like, "_meta"):
        meta = ddf_like._meta
        if isinstance(meta, pd.DataFrame):
            return meta

    # LSDB Catalog: use underlying Dask DataFrame (_ddf) for meta only
    if hasattr(ddf_like, "_ddf"):
        try:
            meta = ddf_like._ddf._meta  # type: ignore[attr-defined]
            if isinstance(meta, pd.DataFrame):
                return meta
        except Exception:
            pass

    # Fallback: try .head(0)
    try:
        head0 = ddf_like.head(0)
        if isinstance(head0, pd.DataFrame):
            return head0
    except Exception:
        pass

    # Last resort: empty DataFrame
    return pd.DataFrame()


def _validate_and_normalize_radec(
    ddf_like: Any,
    ra_col: str,
    dec_col: str,
    log_fn: Callable[[str, bool], None],
) -> Any:
    """
    Validate RA/DEC ranges and normalize RA into [0, 360] when needed.

    Supports both:
      * plain Dask DataFrames, and
      * LSDB Catalogs (lsdb.catalog.Catalog), which behave like Dask collections.

    Rules:
      * DEC must be within [-90, +90] degrees (up to a small numerical epsilon),
        otherwise a ValueError is raised.
      * RA allowed cases:
          - [0, 360] degrees  → kept as is
          - [-180, 180] degrees → converted to [0, 360] via (RA + 360) % 360
        Any other range raises a ValueError.

    This helper assumes coordinates are in degrees. If RA/DEC are provided in
    other units (e.g. hours), the range check will fail and an explicit error
    will be raised.
    """
    # Convert to numeric with coercion, so non-numeric values become NaN.
    # ddf_like[ra_col] / ddf_like[dec_col] work both for Dask DataFrame and
    # for LsdbCatalog, returning a Dask Series.
    ra_num = dd.to_numeric(ddf_like[ra_col], errors="coerce")
    dec_num = dd.to_numeric(ddf_like[dec_col], errors="coerce")

    ra_min, ra_max, dec_min, dec_max = dask_compute(
        ra_num.min(),
        ra_num.max(),
        dec_num.min(),
        dec_num.max(),
    )

    log_fn(
        f"[RA/DEC check] RA in [{ra_min:.6f}, {ra_max:.6f}], "
        f"DEC in [{dec_min:.6f}, {dec_max:.6f}] (assuming degrees)",
        always=True,
    )

    # Basic sanity: we need finite values to proceed
    if not (np.isfinite(ra_min) and np.isfinite(ra_max)
            and np.isfinite(dec_min) and np.isfinite(dec_max)):
        raise ValueError(
            "RA/DEC contain non-finite values or could not be converted to numeric. "
            "This pipeline currently supports only RA/DEC in degrees."
        )

    eps = 1e-6

    # ------------------------------------------------------------------
    # DEC must be in [-90, +90] degrees
    # ------------------------------------------------------------------
    if dec_min < -90.0 - eps or dec_max > 90.0 + eps:
        raise ValueError(
            f"Unsupported DEC range: [{dec_min}, {dec_max}]. "
            "This pipeline currently supports only sky coordinates in degrees, "
            "with DEC in [-90, +90]."
        )

    # ------------------------------------------------------------------
    # RA logic:
    #   * [0, 360]   → keep as is
    #   * [-180,180] → convert to [0,360]
    #   * anything else → error
    # ------------------------------------------------------------------
    if (0.0 - eps) <= ra_min <= (360.0 + eps) and (0.0 - eps) <= ra_max <= (360.0 + eps):
        # Already in [0, 360] degrees → no change
        log_fn(
            "[RA/DEC check] Detected RA in [0, 360] degrees; keeping values as they are.",
            always=True,
        )
        return ddf_like

    if (-180.0 - eps) <= ra_min <= (180.0 + eps) and (-180.0 - eps) <= ra_max <= (180.0 + eps):
        # RA in [-180, 180] degrees → normalize to [0, 360]
        log_fn(
            "[RA/DEC check] Detected RA in [-180, 180] degrees; converting to [0, 360] degrees.",
            always=True,
        )

        def _shift_ra_partition(pdf: pd.DataFrame) -> pd.DataFrame:
            if pdf.empty:
                return pdf
            vals = pd.to_numeric(pdf[ra_col], errors="coerce")
            # Map [-180, 180] → [0, 360] using a simple modulo transform
            vals = (vals + 360.0) % 360.0
            pdf = pdf.copy()
            pdf[ra_col] = vals
            return pdf

        meta = _get_meta_df(ddf_like)
        # ddf_like.map_partitions exists both on Dask DataFrame and on LsdbCatalog.
        ddf_like = ddf_like.map_partitions(_shift_ra_partition, meta=meta)
        return ddf_like

    # Any other RA range is considered unsupported (likely not degrees)
    raise ValueError(
        f"Unsupported RA range: [{ra_min}, {ra_max}]. "
        "This pipeline currently supports only RA in degrees, with RA either in "
        "[0, 360] or [-180, 180] and DEC in [-90, +90]. "
        "Please convert your coordinates to degrees before running the pipeline."
    )


# -----------------------------------------------------------------------------
# Build Dask DataFrame from input files (no debug/origin in this simplified version)
# -----------------------------------------------------------------------------
def _build_input_ddf(paths: List[str], cfg: Config) -> tuple[Any, str, str, List[str]]:
    """
    Build the main input collection for the pipeline.

    Returns:
      ddf       : Dask-like collection ready to use
                  - dd.DataFrame for parquet/csv/tsv
                  - lsdb.catalog.Catalog for hats
      RA_NAME  : resolved RA column name
      DEC_NAME : resolved DEC column name
      keep_cols: final ordered list of columns kept (header order for tiles)
    """
    assert len(paths) > 0, "No input files matched."

    fmt = cfg.input.format.lower()

    # ------------------------------------------------------------------
    # HATS / LSDB input: use lsdb.open_catalog and keep LSDB structure
    # ------------------------------------------------------------------
    if fmt == "hats":
        if len(paths) != 1:
            raise ValueError(
                "For input.format='hats', please specify exactly one HATS catalog "
                "path in input.paths."
            )

        hats_path = paths[0]

        # Columns explicitly requested by the user in the YAML
        requested_keep = cfg.columns.keep or []

        # Extract potential score dependencies directly from the score expression.
        # We assume the user only uses valid column names.
        score_expr = cfg.columns.score or ""
        score_tokens = set(_ID_RE.findall(str(score_expr)))

        # Always request RA, DEC and score dependencies
        must_keep = [cfg.columns.ra, cfg.columns.dec, *score_tokens]

        needed_cols: List[str] = []
        seen: set[str] = set()
        for c in [*must_keep, *requested_keep]:
            if c and (c not in seen):
                needed_cols.append(c)
                seen.add(c)

        # If the user did not define "keep" and score has no dependencies,
        # we open the catalog with all columns. Otherwise we open only
        # the requested subset.
        if needed_cols:
            cat0 = lsdb.open_catalog(hats_path, columns=needed_cols)
        else:
            cat0 = lsdb.open_catalog(hats_path)

        # Resolve RA/DEC using the catalog columns (HATS always has named columns)
        available_cols = list(cat0.columns)

        ra_col = _resolve_col_name(
            cfg.columns.ra,
            cat0,
            header=True,
        )
        dec_col = _resolve_col_name(
            cfg.columns.dec,
            cat0,
            header=True,
        )
        RA_NAME = ra_col
        DEC_NAME = dec_col

        # Build keep_cols in a deterministic order:
        #   - RA, DEC, score dependencies, then everything else requested.
        score_dependencies = [c for c in score_tokens if c in available_cols]

        candidate = requested_keep if requested_keep else available_cols
        must_keep_resolved = [RA_NAME, DEC_NAME, *score_dependencies]

        seen = set()
        keep_cols: List[str] = []
        for c in [*must_keep_resolved, *candidate]:
            if c in available_cols and c not in seen:
                keep_cols.append(c)
                seen.add(c)

        # Sub-select columns via LSDB API; this returns a new Catalog.
        ddf = cat0[keep_cols]

        return ddf, RA_NAME, DEC_NAME, keep_cols

    # ------------------------------------------------------------------
    # Standard Parquet / CSV / TSV input (original behaviour)
    # ------------------------------------------------------------------

    # 1) Base read to discover columns and resolve RA/DEC
    if fmt == "parquet":
        ddf0 = dd.read_parquet(paths, engine="pyarrow")
    elif fmt in ("csv", "tsv"):
        ascii_fmt = (cfg.input.ascii_format or "").upper().strip()
        if ascii_fmt in ("CSV", ""):
            sep = ","
        elif ascii_fmt == "TSV":
            sep = "\t"
        else:
            sep = "," if fmt == "csv" else "\t"
        if cfg.input.header:
            ddf0 = dd.read_csv(paths, sep=sep, assume_missing=True)
        else:
            ddf0 = dd.read_csv(paths, sep=sep, header=None, assume_missing=True)
    else:
        raise ValueError("Unsupported input.format; use 'parquet', 'csv', 'tsv', or 'hats'.")

    # Resolve RA/DEC
    ra_col = _resolve_col_name(
        cfg.columns.ra,
        ddf0,
        header=(fmt == "parquet" or cfg.input.header),
    )
    dec_col = _resolve_col_name(
        cfg.columns.dec,
        ddf0,
        header=(fmt == "parquet" or cfg.input.header),
    )
    RA_NAME = ra_col
    DEC_NAME = dec_col

    # 2) Column selection (preserve order; ensure score deps)
    available_cols = list(ddf0.columns)
    score_dependencies = _score_deps(cfg.columns.score, available_cols)
    requested_keep = (cfg.columns.keep or [])
    must_keep = [RA_NAME, DEC_NAME, *score_dependencies]
    candidate = requested_keep if requested_keep else available_cols

    seen = set()
    keep_cols: List[str] = []
    for c in [*must_keep, *candidate]:
        if c in available_cols and c not in seen:
            keep_cols.append(c)
            seen.add(c)

    ddf = ddf0[keep_cols]
    return ddf, RA_NAME, DEC_NAME, keep_cols


# =============================================================================
# Column report
# =============================================================================

def compute_column_report_sample(ddf_like: Any, sample_rows: int = 200_000) -> Dict:
    """
    Build a small column summary on a sample (keeps it fast and scalable).

    Works both for plain Dask DataFrames and for LSDB Catalogs, as long as
    they implement a .sample(...) method and a .head(...) method that can
    materialize a pandas.DataFrame.
    """
    # Try to use the native .sample(...) API whenever it exists
    if hasattr(ddf_like, "sample"):
        # Heuristic for the sampling fraction based on the number of columns
        try:
            ncols = len(getattr(ddf_like, "columns", []))
        except Exception:
            ncols = 0

        if ncols > 0:
            frac = min(1.0, sample_rows / max(1, ncols * 10_000))
        else:
            frac = 1.0

        # First try the Dask/pandas-style signature (frac, replace)
        try:
            sample = ddf_like.sample(frac=frac, replace=False)
        except TypeError:
            # Some implementations may support only "n="
            try:
                sample = ddf_like.sample(n=int(sample_rows))
            except Exception:
                # Last resort: skip sampling and use the full object
                sample = ddf_like
    else:
        # No sampling API available → use the full object
        sample = ddf_like

    # Materialize up to `sample_rows` rows as a pandas.DataFrame
    try:
        pdf = sample.head(sample_rows, compute=True)
    except TypeError:
        # For pandas-like .head(n) without "compute=" keyword
        pdf = sample.head(sample_rows)

    report: Dict[str, Dict[str, Any]] = {}
    for c in pdf.columns:
        s = pdf[c]
        col_info: Dict[str, Any] = {"dtype": str(s.dtype), "n_null": int(s.isna().sum())}

        if pd.api.types.is_numeric_dtype(s):
            if len(s):
                col_info.update({
                    "min": float(np.nanmin(s.values)),
                    "max": float(np.nanmax(s.values)),
                    "mean": float(np.nanmean(s.values)),
                })
            else:
                col_info.update({"min": np.nan, "max": np.nan, "mean": np.nan})
        else:
            example = next((x for x in s.values if pd.notna(x)), "")
            col_info.update({"example": str(example)})

        report[c] = col_info

    return {"columns": report}


def compute_column_report_global(ddf_like: Any) -> Dict:
    """
    Build a column summary using *global* statistics computed with Dask-like
    operations (no sampling).

    This works for:
      - plain Dask DataFrames, and
      - LSDB Catalogs, as long as they implement .dtypes and column access
        via __getitem__ (ddf_like[col]) returning a Series with the usual
        Dask-style aggregations.
    """
    report: Dict[str, Dict[str, Any]] = {}

    dtypes = ddf_like.dtypes.to_dict()

    tasks = []
    task_keys = []  # (column_name, field_name)

    for col, dt in dtypes.items():
        s = ddf_like[col]

        # Always compute n_null
        tasks.append(s.isna().sum())
        task_keys.append((col, "n_null"))

        # Numeric → global min/max/mean
        if np.issubdtype(dt, np.number):
            tasks.append(s.min())
            task_keys.append((col, "min"))

            tasks.append(s.max())
            task_keys.append((col, "max"))

            tasks.append(s.mean())
            task_keys.append((col, "mean"))
        else:
            # For non-numeric: get one non-null example (if any)
            tasks.append(s.dropna().head(1))
            task_keys.append((col, "example"))

    # Execute all aggregations in a single Dask graph compute
    results = dask_compute(*tasks)

    # Rebuild the output dictionary
    tmp: Dict[str, Dict[str, Any]] = {}
    for (col, field), value in zip(task_keys, results):
        if col not in tmp:
            tmp[col] = {"dtype": str(dtypes[col])}

        if field == "example":
            # value is a pandas Series with up to 1 element
            try:
                v = value.iloc[0]
            except (IndexError, AttributeError):
                v = ""
            tmp[col]["example"] = str(v)
        elif field in ("min", "max", "mean"):
            tmp[col][field] = float(value) if value is not None else np.nan
        elif field == "n_null":
            tmp[col]["n_null"] = int(value)

    report["columns"] = tmp
    return report

# =============================================================================
# HEALPix helpers & density maps
# =============================================================================

def ipix_for_depth(ra_deg: np.ndarray, dec_deg: np.ndarray, depth: int) -> np.ndarray:
    """Return HEALPix NESTED pixel index for a given depth (order)."""
    nside = 1 << depth
    theta = np.deg2rad(90.0 - dec_deg)  # colatitude
    phi = np.deg2rad(ra_deg)            # longitude
    return hp.ang2pix(nside, theta, phi, nest=True)


def densmap_for_depth_delayed(ddf: Any, ra_col: str, dec_col: str, depth: int):
    """
    Delayed version: build a delayed HEALPix histogram at 'depth'.

    For HATS / LSDB catalogs (format: hats) with a HEALPix nested index
    named like "_healpix_<order>", we derive the density map directly
    from the index by bit-shifting to the requested depth.

    For all other inputs we fall back to computing HEALPix pixel indices
    from RA/DEC, preserving the original behaviour.
    """
    nside = 1 << depth
    npix = hp.nside2npix(nside)

    # Detect whether this looks like a HATS/LSDB catalog with a HEALPix index
    meta = _get_meta_df(ddf)
    idx_name = getattr(meta.index, "name", None)
    base_order = None

    if idx_name:
        m = _HEALPIX_INDEX_RE.match(str(idx_name))
        if m:
            base_order = int(m.group(1))


    def _part_hist(pdf: pd.DataFrame) -> _np.ndarray:
        if pdf is None or len(pdf) == 0:
            return _np.zeros(npix, dtype=_np.int64)

        # Fast path: HATS/LSDB with HEALPix nested index, and depth <= base_order
        if (
            base_order is not None
            and pdf.index.name == idx_name
            and depth <= base_order
        ):
            # pdf.index contains the HEALPix nested index at order base_order
            ipix_base = pdf.index.to_numpy()
            shift = 2 * (base_order - depth)
            ip = (ipix_base >> shift).astype(_np.int64)
        else:
            # Generic path: compute HEALPix indices from RA/DEC
            ip = ipix_for_depth(
                pdf[ra_col].to_numpy(),
                pdf[dec_col].to_numpy(),
                depth,
            )

        return _np.bincount(ip, minlength=npix).astype(_np.int64)

    # One histogram per partition
    part_delayed = ddf.to_delayed()
    hists = [_delayed(_part_hist)(p) for p in part_delayed]

    if len(hists) == 0:
        # Still return a delayed object for consistency
        return _delayed(lambda: _np.zeros(npix, dtype=_np.int64))()

    def _sum_vecs(vecs: list[_np.ndarray]) -> _np.ndarray:
        return _np.sum(vecs, axis=0, dtype=_np.int64)

    total = _delayed(_sum_vecs)(hists)
    return total


def densmap_for_depth(ddf: dd.DataFrame, ra_col: str, dec_col: str, depth: int) -> np.ndarray:
    """
    Backward-compatible wrapper: compute densmap for a given depth
    immediately, keeping the original behaviour.
    """
    return densmap_for_depth_delayed(ddf, ra_col, dec_col, depth).compute()


# =============================================================================
# Tile writer (faithful on-disk layout)
# =============================================================================

class TSVTileWriter:
    def __init__(self, out_dir: Path, depth: int, header_line: str):
        self.depth = depth
        self.out_dir = out_dir
        self.norder_dir = out_dir / f"Norder{depth}"
        _mkdirs(self.norder_dir)
        self.header_line = header_line

    def _dir_for_ipix(self, ipix: int) -> Path:
        base = (ipix // 10_000) * 10_000
        p = self.norder_dir / f"Dir{base}"
        _mkdirs(p)
        return p

    def allsky_tmp(self) -> Path:
        return self.norder_dir / ".Allsky.tsv"

    def allsky_path(self) -> Path:
        return self.norder_dir / "Allsky.tsv"

    def cell_tmp(self, ipix: int) -> Path:
        return self._dir_for_ipix(ipix) / f".Npix{ipix}.tsv"

    def cell_path(self, ipix: int) -> Path:
        return self._dir_for_ipix(ipix) / f"Npix{ipix}.tsv"


# =============================================================================
# Metadata writers (properties, metadata.xml, MOC, densmaps, arguments)
# =============================================================================

def write_properties(out_dir: Path, label: str, target: str, level_limit: int, n_src: int, tile_format: str = "tsv"):
    now = _now_str()
    buf = []
    #buf.append("# Generated by the CDS HiPS tool for catalogues.\n")
    buf.append("# Generated by the Hipsgen-cat.py tool (inspired by the CDS HiPS catalog tools).\n")
    buf.append(f"# {now}\n")
    buf.append(f"publisher_did     = ivo://PRIVATE_USER/{label}\n")
    buf.append("dataproduct_type  = catalog\n")
    buf.append(f"hips_service_url  = {str(out_dir).rstrip('/')}/{label}\n")
    buf.append("hips_builder      = cds.hips.cat.standalone.v0.2\n")
    buf.append(f"hips_release_date = {now}\n")
    buf.append("hips_frame        = equatorial\n")
    buf.append(f"hips_cat_nrows    = {n_src}\n")
    buf.append(f"hips_order        = {level_limit}\n")
    buf.append(f"hips_tile_format  = {tile_format}\n")
    try:
        ra0, dec0 = target.split()
    except Exception:
        ra0, dec0 = "0", "0"
    buf.append(f"hips_initial_ra   = {ra0}\n")
    buf.append(f"hips_initial_dec  = {dec0}\n")
    buf.append("hips_status       = public master unclonable\n")
    buf.append("# Deprecated but still in use\n")
    buf.append(f"label={label}\n")
    buf.append("coordsys=C\n")
    _write_text(out_dir / "properties", "".join(buf))


def write_arguments(out_dir: Path, args_text: str):
    _write_text(out_dir / "arguments", args_text)


def write_metadata_xml(out_dir: Path, columns: List[tuple[str, str, Optional[str]]], ra_idx: int, dec_idx: int):
    """
    Build a VOTable (metadata-only) marking RA/DEC with UCD 'meta.main'.
    Writes metadata.xml and Metadata.xml to match CDS behavior.
    """
    table = Table()
    for (name, dtype, ucd) in columns:
        if dtype.startswith("float"):
            np_dt = np.float64
        elif dtype.startswith("int"):
            np_dt = np.int64
        else:
            np_dt = "U1"
        table[name] = np.array([], dtype=np_dt)

    vot = from_table(table)
    res = vot.resources[0]
    vtab = res.tables[0]

    for i, field in enumerate(vtab.fields):
        field.name = columns[i][0]
        if i == ra_idx:
            field.ucd = "pos.eq.ra;meta.main"
        elif i == dec_idx:
            field.ucd = "pos.eq.dec;meta.main"
        else:
            field.ucd = columns[i][2] or field.ucd

    path_lower = str(out_dir / "metadata.xml")
    path_upper = str(out_dir / "Metadata.xml")
    try:
        vot_writeto(vot, path_lower)
    except TypeError:
        with open(path_lower, "wb") as fh:
            vot.to_xml(fh)
    try:
        vot_writeto(vot, path_upper)
    except TypeError:
        with open(path_upper, "wb") as fh:
            vot.to_xml(fh)


def write_moc(out_dir: Path, level_coverage: int, dens_counts: np.ndarray):
    """Build MOC at lC with all pixels having count > 0, write FITS + JSON."""
    order = int(level_coverage)
    ipix = np.flatnonzero(dens_counts > 0)

    if ipix.size == 0:
        moc = MOC.empty(order)
    else:
        ipix_list = [int(x) for x in np.asarray(ipix, dtype=np.int64).tolist()]
        nside = 1 << order

        moc = None
        last_err = None
        candidates = [
            lambda: MOC.from_healpix_cells(order, ipix_list, True),
            lambda: MOC.from_healpix_cells(order, ipix_list),
            lambda: MOC.from_healpix_cells(nside, ipix_list, order, True),
            lambda: MOC.from_healpix_cells(nside, ipix_list, order),
            lambda: MOC.from_healpix_cells(ipix_list, order, True),
            lambda: MOC.from_healpix_cells(ipix_list, order),
            lambda: MOC.from_healpix_cells(nside=nside, ipix=ipix_list, max_depth=order, nested=True),
            lambda: MOC.from_healpix_cells(nside=nside, ipix=ipix_list, max_depth=order),
        ]
        for builder in candidates:
            try:
                moc = builder()
                break
            except Exception as e:
                last_err = e
                continue
        if moc is None:
            raise RuntimeError(
                f"Failed to build MOC with your mocpy version. Last error: {type(last_err).__name__}: {last_err}"
            )

    fits_path = out_dir / "Moc.fits"
    try:
        try:
            moc.save(str(fits_path), "fits")
        except TypeError:
            moc.save(str(fits_path), format="fits")
    except Exception:
        moc.write(fits_path, overwrite=True)

    json_path = out_dir / "Moc.json"
    data = moc.serialize(format="json")
    with json_path.open("w", encoding="utf-8") as f:
        if isinstance(data, str):
            f.write(data)
        elif isinstance(data, bytes):
            f.write(data.decode("utf-8"))
        else:
            json.dump(data, f)


def write_densmap_fits(out_dir: Path, depth: int, counts: np.ndarray):
    if depth >= 13:
        return
    hdu0 = fits.PrimaryHDU()
    col = fits.Column(name="VALUE", array=counts.astype(np.int64), format="K")
    hdu1 = fits.BinTableHDU.from_columns([col])
    fits.HDUList([hdu0, hdu1]).writeto(out_dir / f"densmap_o{depth}.fits", overwrite=True)


# =============================================================================
# Tile / Allsky helpers
# =============================================================================

def finalize_write_tiles(
    out_dir: Path,
    depth: int,
    header_line: str,
    ra_col: str,
    dec_col: str,
    counts: np.ndarray,
    selected: pd.DataFrame,
    order_desc: bool,
    allsky_collect: bool = False,
) -> tuple[Dict[int, int], Optional[pd.DataFrame]]:
    """
    Write one TSV per HEALPix cell (atomic rename), with a Completeness header
    and a single header line, followed by selected rows in the SAME column
    order as the header. Uses pandas.to_csv to avoid partial/truncated first line.
    """
    writer = TSVTileWriter(out_dir, depth, header_line)
    npix = len(counts)
    written: Dict[int, int] = {}
    allsky_rows: List[List[str]] = []

    header_cols = header_line.strip("\n").split("\t")
    internal = {"__ipix__", "__score__", "__icov__"}
    tile_cols = [c for c in header_cols if c not in internal and c in selected.columns]

    if selected is None or len(selected) == 0 or len(tile_cols) == 0:
        return {}, None

    for pid, g in selected.groupby("__ipix__"):
        ip = int(pid)
        if ip < 0 or ip >= npix:
            continue

        n_src_cell = int(counts[ip])

        g_tile = g[tile_cols].copy()
        n_written = int(len(g_tile))
        written[ip] = n_written
        n_remaining = max(0, n_src_cell - n_written)

        completeness_header = f"# Completeness = {n_remaining} / {n_src_cell}\n"

        final_path = writer.cell_path(ip)
        final_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = final_path.with_name(f".Npix{ip}.tsv.tmp")

        # 1) Write completeness + header
        with tmp.open("w", encoding="utf-8", newline="") as f:
            f.write(completeness_header)
            f.write(header_line)

        # Sanitize string columns
        obj_cols = g_tile.select_dtypes(include=["object", "string"]).columns
        if len(obj_cols) > 0:
            g_tile[obj_cols] = g_tile[obj_cols].replace({r"[\t\r\n]": " "}, regex=True)

        # 2) Append rows
        g_tile.to_csv(
            tmp,
            sep="\t",
            index=False,
            header=False,
            mode="a",
            encoding="utf-8",
            lineterminator="\n",
        )

        os.replace(tmp, final_path)

        if allsky_collect and n_written > 0:
            allsky_rows.extend(g_tile.values.tolist())

    allsky_df = None
    if allsky_collect and allsky_rows:
        allsky_df = pd.DataFrame(allsky_rows, columns=tile_cols)

    return written, allsky_df


def build_header_line_from_keep(keep_cols: List[str]) -> str:
    """Build header line from keep_cols (before internal columns are added)."""
    return "\t".join([str(c) for c in keep_cols]) + "\n"


# =============================================================================
# Coverage-based selection helpers
# =============================================================================

def build_cov_thresholds(selected: pd.DataFrame, score_col: str, order_desc: bool) -> Dict[int, float]:
    """
    Per-coverage worst-kept thresholds:
      - ascending  -> max(score) among kept in each __icov__
      - descending -> min(score) among kept in each __icov__
    Used to filter the remainder so that selected rows never reappear.
    """
    if len(selected) == 0:
        return {}
    if order_desc:
        s = selected.groupby("__icov__")[score_col].min()
    else:
        s = selected.groupby("__icov__")[score_col].max()
    return {int(k): float(v) for k, v in s.items()}


def filter_remainder_by_coverage_partition(
    pdf: pd.DataFrame,
    score_expr: str,
    order_desc: bool,
    thr_cov: Dict[int, float],
    ra_col: str,
    dec_col: str,
) -> pd.DataFrame:
    """
    Keep only rows that are strictly *worse* than the kept threshold in their
    coverage pixel (__icov__). Rows from pixels with no threshold pass through.
    """
    if len(pdf) == 0:
        return pdf

    # Score computation: column or expression on existing columns
    if score_expr in pdf.columns:
        sc = pd.to_numeric(pdf[score_expr], errors="coerce")
    else:
        code = compile(score_expr, "<score>", "eval")
        env = {"__builtins__": {}, "np": np, "numpy": np}
        env.update({col: pdf[col] for col in pdf.columns})
        out = eval(code, env, {})
        sc = pd.to_numeric(out, errors="coerce")

    sc = sc.replace([np.inf, -np.inf], np.nan)
    sc = sc.fillna(-np.inf if order_desc else np.inf)

    if "__icov__" not in pdf.columns:
        return pdf

    icov = pdf["__icov__"].to_numpy()
    thr = np.array([thr_cov.get(int(c), None) for c in icov], dtype=object)

    if order_desc:
        mask = np.array([(t is None) or (s < t) for s, t in zip(sc.values, thr)], dtype=bool)
    else:
        mask = np.array([(t is None) or (s > t) for s, t in zip(sc.values, thr)], dtype=bool)

    return pdf.loc[mask]


def _candidates_by_coverage_partition(
    pdf: pd.DataFrame,
    score_col: str,
    order_desc: bool,
    k_per_cov: int | dict,
    tie_buffer: int,
    ra_col: str,
    dec_col: str,
) -> pd.DataFrame:
    """
    If k_per_cov is a dict {icov: k}, use per-coverage k; otherwise use scalar k for all.
    Keeps up to (k + tie_buffer) rows per coverage pixel (__icov__).
    """
    if len(pdf) == 0:
        return pdf.iloc[0:0]

    asc = (not order_desc)
    pdf = pdf.sort_values(
        ["__icov__", score_col, ra_col, dec_col],
        ascending=[True, asc, True, True],
        kind="mergesort",
    )
    pdf["__rk__"] = pdf.groupby("__icov__").cumcount()

    if isinstance(k_per_cov, dict):
        def _keep_group(g: pd.DataFrame) -> pd.DataFrame:
            ic = int(g["__icov__"].iloc[0])
            k = int(k_per_cov.get(ic, 0))
            need = max(0, k + int(tie_buffer))
            return g.iloc[:need]
        out = pdf.groupby("__icov__", group_keys=False).apply(_keep_group)
    else:
        need = int(k_per_cov) + int(tie_buffer)
        out = pdf[pdf["__rk__"] < need]

    return out.drop(columns=["__rk__"])


def _reduce_coverage_exact(
    pdf: pd.DataFrame,
    score_col: str,
    order_desc: bool,
    k_per_cov: int | dict,
    ra_col: str,
    dec_col: str,
) -> pd.DataFrame:
    """
    Keep exactly k rows per coverage when possible; if availability < k, keep what's available.
    Supports scalar k or dict {icov: k}.
    """
    if len(pdf) == 0:
        return pdf.iloc[0:0]

    asc = (not order_desc)
    out = []
    for icov, g in pdf.groupby("__icov__"):
        k = int(k_per_cov.get(int(icov), 0)) if isinstance(k_per_cov, dict) else int(k_per_cov)
        if k <= 0 or len(g) == 0:
            continue
        gg = g.sort_values(
            [score_col, ra_col, dec_col],
            ascending=[asc, True, True],
            kind="mergesort",
        ).head(k)
        out.append(gg)
    return pd.concat(out, ignore_index=True) if out else pdf.iloc[0:0]


def _reduce_coverage_exact_dask(
    ddf_like: Any,
    score_col: str,
    order_desc: bool,
    k_per_cov: int,
    ra_col: str,
    dec_col: str,
) -> Any:
    """
    Dask-based equivalent of _reduce_coverage_exact for the scalar k_per_cov case.

    It keeps up to k_per_cov rows per coverage cell (__icov__), with the same
    sorting and tie-breaking rules as the pandas implementation:
      - sort by score_col, then RA, then DEC
      - if there are fewer than k_per_cov rows in a cell, keep all of them.

    This function accepts:
      - a plain Dask DataFrame, or
      - an LSDB Catalog, in which case the underlying ._ddf is used directly
        to avoid interfering with LSDB's higher-level semantics.
    """
    if k_per_cov <= 0:
        empty_meta = _get_meta_df(ddf_like)
        # We always return an object with the same "shape" (columns/dtypes),
        # but with zero rows.
        return ddf_like.map_partitions(
            lambda pdf: pdf.iloc[0:0],
            meta=empty_meta,
        )

    asc = (not order_desc)

    def _take_topk(g: pd.DataFrame) -> pd.DataFrame:
        if g.empty:
            return g
        g_sorted = g.sort_values(
            [score_col, ra_col, dec_col],
            ascending=[asc, True, True],
            kind="mergesort",
        )
        # If len(g) < k_per_cov, head(k_per_cov) simply returns the whole group.
        return g_sorted.head(int(k_per_cov))

    # Build a meta DataFrame once and reuse it for the groupby/apply
    meta = _get_meta_df(ddf_like)

    # ------------------------------------------------------------------
    # HATS / LSDB path: operate on the underlying Dask DataFrame (. _ddf)
    # ------------------------------------------------------------------
    if isinstance(ddf_like, LsdbCatalog) and hasattr(ddf_like, "_ddf"):
        base_ddf = ddf_like._ddf  # type: ignore[attr-defined]
        return base_ddf.groupby("__icov__", group_keys=False).apply(
            _take_topk,
            meta=meta,
        )

    # ------------------------------------------------------------------
    # Generic Dask DataFrame path (parquet / CSV / TSV etc.)
    # ------------------------------------------------------------------
    if hasattr(ddf_like, "groupby"):
        return ddf_like.groupby("__icov__", group_keys=False).apply(
            _take_topk,
            meta=meta,
        )

    # Fallback: if we do not recognize the type, simply return as-is.
    # This keeps non-HATS behaviour unchanged in unexpected cases.
    return ddf_like


def apply_fractional_k_per_cov(
    selected: pd.DataFrame,
    k_desired: float,
    score_col: str,
    order_desc: bool,
    mode: str = "random",
    ra_col: str | None = None,
    dec_col: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Apply fractional-k adjustment on top of an integer per-coverage selection.

    Inputs
    -------
    selected : pd.DataFrame
        DataFrame already limited to <= k_int rows per __icov__ 
        (output of _reduce_coverage_exact).
    k_desired : float
        Expected number of rows per coverage cell (can be fractional).
    score_col : str
        Column name used for ranking.
    order_desc : bool
        If False → lower score is better; if True → higher score is better.
    mode : str
        * "random" → per-coverage behavior: probabilistic +1 per cell.
        * "score"  → global behavior: keeps the globally best sources by score,
    ra_col, dec_col : str, optional
        RA and DEC column names (used for deterministic tie-breaking).

    Returns
    -------
    kept_df, dropped_df : tuple[pd.DataFrame, pd.DataFrame]
        kept_df    : rows actually kept at this depth.
        dropped_df : rows not written at this depth, but still available for
                     deeper levels.
    """
    # Nothing to do if no data or k is non-positive
    if selected is None or len(selected) == 0 or k_desired <= 0.0:
        return selected, selected.iloc[0:0]

    mode = (mode or "random").lower()

    # ============================================================
    # MODE "score" → GLOBAL SELECTION (rank by score, not per_cov)
    # ============================================================
    if mode == "score":
        if "__icov__" not in selected.columns:
            # Fallback to random if __icov__ is missing
            mode = "random"
        else:
            n_cov = int(selected["__icov__"].nunique())
            if n_cov == 0:
                return selected.iloc[0:0], selected

            # Compute the total expected number of sources for this depth
            # (fractional k times number of coverage cells)
            k_total_desired = float(k_desired) * float(n_cov)

            # Round and cap to available number of rows
            n_sel = len(selected)
            n_keep = int(round(k_total_desired))
            n_keep = max(0, min(n_keep, n_sel))

            if n_keep == 0:
                return selected.iloc[0:0], selected

            # Sort globally by score, using RA/DEC as deterministic tie-breakers
            asc = (not order_desc)
            sort_cols = [score_col]
            ascending = [asc]

            if ra_col is not None and ra_col in selected.columns:
                sort_cols.append(ra_col)
                ascending.append(True)
            if dec_col is not None and dec_col in selected.columns:
                sort_cols.append(dec_col)
                ascending.append(True)

            selected_sorted = selected.sort_values(
                sort_cols,
                ascending=ascending,
                kind="mergesort",
            )

            kept_df = selected_sorted.iloc[:n_keep]
            dropped_df = selected_sorted.iloc[n_keep:]
            return kept_df, dropped_df

    # ============================================================
    # MODE "random" → per-coverage uniform sampling
    # ============================================================
    # In this mode each coverage cell behaves independently:
    # we keep floor(k_desired) rows for sure, and one additional
    # row with probability equal to the fractional part.
    k_floor_global = math.floor(k_desired)
    frac = k_desired - k_floor_global

    # If k is effectively integer → nothing to adjust
    if frac <= 1e-9:
        return selected, selected.iloc[0:0]

    rng = np.random.default_rng()
    kept_parts = []
    dropped_parts = []

    for icov, g in selected.groupby("__icov__"):
        n_avail = len(g)
        if n_avail == 0:
            continue

        k_local_desired = min(k_desired, float(n_avail))
        k_floor = int(math.floor(k_local_desired))
        k_floor = max(0, k_floor)

        if k_floor > 0:
            base_keep = g.iloc[:k_floor]
        else:
            base_keep = g.iloc[0:0]

        remaining = g.iloc[k_floor:]

        extra_keep = 0
        frac_local = max(0.0, min(1.0, k_local_desired - float(k_floor)))
        if len(remaining) > 0 and frac_local > 0.0:
            u = rng.random()
            if u < frac_local:
                extra_keep = 1

        if extra_keep == 1:
            extra_row = remaining.iloc[:1]  # best of the remaining
            final_keep = pd.concat([base_keep, extra_row], ignore_index=False)
            final_drop = remaining.iloc[1:]
        else:
            final_keep = base_keep
            final_drop = remaining

        if len(final_keep) > 0:
            kept_parts.append(final_keep)
        if len(final_drop) > 0:
            dropped_parts.append(final_drop)

    kept_df = pd.concat(kept_parts, ignore_index=False) if kept_parts else selected.iloc[0:0]
    dropped_df = pd.concat(dropped_parts, ignore_index=False) if dropped_parts else selected.iloc[0:0]

    return kept_df, dropped_df


# =============================================================================
# Pipeline (per_cov-only)
# =============================================================================

def run_pipeline(cfg: Config) -> None:
    out_dir = Path(cfg.output.out_dir)
    _mkdirs(out_dir)

    # Directory for Dask performance reports
    report_dir = out_dir / "dask_reports"
    _mkdirs(report_dir)

    t0 = time.time()
    log_lines: List[str] = []

    def _log(msg: str, always: bool = False):
        """Log message with timestamp to stdout and to the in-memory log buffer."""
        if always:
            line = f"{_ts()} | {msg}"
            print(line)
            log_lines.append(line)
        else:
            # In this simplified version we always log when called.
            line = f"{_ts()} | {msg}"
            print(line)
            log_lines.append(line)

    _log(f"START HiPS catalog pipeline: cat_name={cfg.output.cat_name} out_dir={out_dir}", always=True)
    _log(
        f"Config -> lM={cfg.algorithm.level_limit} "
        f"lC={cfg.algorithm.level_coverage} "
        f"Oc={cfg.algorithm.coverage_order} "
        f"order_desc={cfg.algorithm.order_desc}",
        always=True,
    )

    # CDS-like validation for level_limit
    if not (4 <= int(cfg.algorithm.level_limit) <= 11):
        raise ValueError("level_limit (lM) must be within [4, 11] to mirror the CDS tool.")

    if cfg.algorithm.level_coverage > cfg.algorithm.level_limit:
        cfg.algorithm.level_coverage = cfg.algorithm.level_limit
        _log("WARNING: level_coverage was > level_limit; set lC = lM", always=True)

    # Cluster
    if cfg.cluster.mode == "slurm":
        assert SLURMCluster is not None, "dask-jobqueue not available"
        sl = cfg.cluster.slurm or {}
        job_directives = sl.get("job_extra_directives", sl.get("job_extra", []))
        cluster = SLURMCluster(
            queue=sl.get("queue", "cpu_dev"),
            account=sl.get("account", None),
            cores=cfg.cluster.threads_per_worker,
            processes=1,
            memory=cfg.cluster.memory_per_worker,
            job_extra_directives=job_directives,
        )
        cluster.scale(cfg.cluster.n_workers)
        client = Client(cluster)
    else:
        cluster = LocalCluster(
            n_workers=cfg.cluster.n_workers,
            threads_per_worker=cfg.cluster.threads_per_worker,
            memory_limit=cfg.cluster.memory_per_worker,
        )
        client = Client(cluster)

    _log(f"Dask dashboard: {client.dashboard_link}", always=True)

    # Decide how aggressively we use memory vs. computes.
    # By default we stay in low-memory mode (persist_ddfs = False and avoid_computes_wherever_possible = True).
    persist_ddfs = bool(getattr(cfg.cluster, "persist_ddfs", False))
    avoid_computes = bool(getattr(cfg.cluster, "avoid_computes_wherever_possible", True))

    if persist_ddfs:
        _log("[cluster] persist_ddfs=True → will persist large intermediates in memory", always=True)
    else:
        _log("[cluster] persist_ddfs=False (lower memory consumption)", always=True)

    if avoid_computes:
        _log(
            "[cluster] avoid_computes_wherever_possible=True → will try to avoid large .compute() calls "
            "whenever possible (using more Dask-native operations) (lower memory consumption)",
            always=True,
        )
    else:
        _log(
            "[cluster] avoid_computes_wherever_possible=False → keep standard behaviour for computes.",
            always=True,
        )


    diagnostics_mode = getattr(cfg.cluster, "diagnostics_mode", "per_step")

    def _diag_ctx(label: str):
        """
        Return a context manager for diagnostics.

        - "per_step": one HTML report per labeled step.
        - "global": diagnostics handled by outer global report, so here we return a no-op.
        - "off": no diagnostics at all (no-op).
        """
        if diagnostics_mode == "per_step":
            return performance_report(filename=str(report_dir / f"{label}.html"))
        else:
            # In "global" or "off" mode, local diagnostics are disabled.
            return nullcontext()

    def _run_core_pipeline() -> None:
        """
        Main pipeline body. This function contains all the logic that builds
        the HiPS catalog (input reading, RA/DEC validation, densmaps,
        per-depth selection loop, etc.).
        """

        # ------------------------------------------------------------------
        # Input
        # ------------------------------------------------------------------
        paths: List[str] = []
        for p in cfg.input.paths:
            paths.extend(glob.glob(p))
        assert len(paths) > 0, "No input files matched."

        _log(f"Matched {len(paths)} input files", always=True)
        _log(
            "Some input files: "
            + ", ".join(paths[:3])
            + (" ..." if len(paths) > 3 else ""),
            always=True,
        )

        # Build input collection (Dask DataFrame or LSDB Catalog)
        ddf, RA_NAME, DEC_NAME, keep_cols = _build_input_ddf(paths, cfg)

        is_hats = isinstance(ddf, LsdbCatalog)

        # ------------------------------------------------------------------
        # RA/DEC sanity check + normalization for all inputs
        # ------------------------------------------------------------------
        # Supports both:
        #   * plain Dask DataFrames (parquet/csv/tsv), and
        #   * HATS / LSDB catalogs (lsdb.catalog.Catalog).
        # Enforces:
        #   * DEC in [-90, +90] degrees
        #   * RA in [0, 360] or [-180, 180] degrees
        #     (the latter is converted to [0, 360]).
        with _diag_ctx("dask_radec"):
            ddf_local = _validate_and_normalize_radec(
                ddf_like=ddf,
                ra_col=RA_NAME,
                dec_col=DEC_NAME,
                log_fn=_log,
            )
        ddf = ddf_local

        # For HATS catalogs we keep the native spatial partitioning and DO NOT repartition.
        if not is_hats:
            ddf = ddf.repartition(partition_size="256MB")
    
        # In high-throughput mode we keep the repartitioned dataframe in memory.
        # In low-memory mode we let Dask stream from disk/out-of-core as needed.
        if persist_ddfs and hasattr(ddf, "persist"):
            ddf = ddf.persist()
            wait(ddf)
    
        # ------------------------------------------------------------------
        # Coverage: either fixed HEALPix coverage (default) or HATS partitions
        # ------------------------------------------------------------------
        use_hats_cov = is_hats and bool(getattr(cfg.algorithm, "use_hats_as_coverage", False))

        base_meta = _get_meta_df(ddf)
        meta_with_icov = base_meta.copy()
        meta_with_icov["__icov__"] = pd.Series([], dtype="int64")

        if use_hats_cov:
            # ==============================================================
            # HATS-specific coverage: one coverage cell per HATS partition
            # --------------------------------------------------------------
            # Each LSDB/HATS partition becomes a coverage cell (__icov__).
            # This follows the adaptive HATS pixelization (different orders,
            # non-overlapping pixels), but keeps the rest of the pipeline
            # logic unchanged (densmaps, MOC, hierarchical selection).
            # ==============================================================

            try:
                hp_pixels = ddf.get_healpix_pixels()
                n_hp_pixels = len(hp_pixels)
            except Exception:
                hp_pixels = None
                n_hp_pixels = None

            n_parts = ddf.npartitions

            msg = (
                f"[coverage] Using HATS partitions as coverage cells (__icov__), "
                f"n_partitions={n_parts}"
            )
            if n_hp_pixels is not None:
                msg += f", get_healpix_pixels() returned {n_hp_pixels} pixels"
            _log(msg, always=True)

            # Build a small Dask Series with one integer per partition: 0, 1, ..., n_parts-1
            part_ids = dd.from_pandas(
                pd.Series(range(n_parts), dtype="int64"),
                npartitions=n_parts,
            )

            def _assign_icov(pdf: pd.DataFrame, part_series: pd.Series) -> pd.DataFrame:
                """Assign the partition id as __icov__ for all rows in the partition."""
                if pdf.empty:
                    pdf["__icov__"] = pd.Series([], dtype="int64")
                    return pdf
                cov_id = int(part_series.iloc[0])
                pdf = pdf.copy()
                pdf["__icov__"] = cov_id
                return pdf

            ddf = ddf.map_partitions(
                _assign_icov,
                part_ids,
                meta=meta_with_icov,
            )

        else:
            # ==============================================================
            # Default coverage: HEALPix cells at coverage_order (Oc)
            # ==============================================================

            Oc = int(cfg.algorithm.coverage_order)
            NSIDE_C = 1 << Oc

            def _add_icov(pdf: pd.DataFrame, ra_col: str, dec_col: str) -> pd.DataFrame:
                """
                Add coverage cell index (__icov__) at coverage_order.

                For HATS / LSDB catalogs (format: hats) with a HEALPix nested index
                named like "_healpix_<order>", we derive __icov__ directly from the
                index by bit-shifting.

                For all other inputs we fall back to computing HEALPix indices from
                RA/DEC, preserving the original behaviour.
                """
                if len(pdf) == 0:
                    pdf["__icov__"] = pd.Series([], dtype="int64")
                    return pdf

                # Fast path only for HATS catalogs
                if is_hats:
                    idx_name = getattr(pdf.index, "name", None)
                    m = _HEALPIX_INDEX_RE.match(str(idx_name)) if idx_name else None

                    if m is not None:
                        # pdf.index stores the HEALPix nested pixel index at order base_order
                        base_order = int(m.group(1))
                        if Oc <= base_order:
                            # Coarsen from base_order to coverage_order by dropping 2 bits per level
                            ipix_base = pdf.index.to_numpy()
                            shift = 2 * (base_order - Oc)
                            icov = (ipix_base >> shift).astype(np.int64)
                            pdf = pdf.copy()
                            pdf["__icov__"] = icov
                            return pdf
                        # If coverage_order > base_order, fall through to the RA/DEC fallback

                # Generic path: compute coverage index from RA/DEC with healpy
                theta = np.deg2rad(
                    90.0 - pd.to_numeric(pdf[dec_col], errors="coerce").to_numpy()
                )
                phi = np.deg2rad(
                    (pd.to_numeric(pdf[ra_col], errors="coerce").to_numpy()) % 360.0
                )
                icov = hp.ang2pix(NSIDE_C, theta, phi, nest=True).astype(np.int64)
                pdf = pdf.copy()
                pdf["__icov__"] = icov
                return pdf

            ddf = ddf.map_partitions(
                _add_icov,
                RA_NAME,
                DEC_NAME,
                meta=meta_with_icov,
            )
    
        # Column report sample
        #nhh_dir = out_dir / "nhhtree"
        #_mkdirs(nhh_dir)
        #report = compute_column_report_sample(ddf)
        #_write_text(nhh_dir / "metadata.info", json.dumps(report, indent=2))
    
        # Column report global
        #nhh_dir = out_dir / "nhhtree"
        #_mkdirs(nhh_dir)
        #report = compute_column_report_global(ddf)
        #_write_text(nhh_dir / "metadata.info", json.dumps(report, indent=2))
    
        # Densmaps 0..lM + FITS (computed in parallel)
        depths = list(range(0, cfg.algorithm.level_limit + 1))
        densmaps: Dict[int, np.ndarray] = {}
        
        # Build delayed densmaps for all depths
        delayed_maps = {
            d: densmap_for_depth_delayed(ddf, RA_NAME, DEC_NAME, depth=d)
            for d in depths
        }
        
        with _diag_ctx("dask_densmaps"):
            computed = dask_compute(*delayed_maps.values())
        
        # Fill dict and write FITS files (isso já é NumPy/FITS puro, fora do with)
        for d, dens in zip(delayed_maps.keys(), computed):
            densmaps[d] = dens
            write_densmap_fits(out_dir, d, dens)
    
        # Coverage densmap used for the MOC
        dens_lc = densmaps[cfg.algorithm.level_coverage]
    
        # MOC
        write_moc(out_dir, cfg.algorithm.level_coverage, dens_lc)
    
        # metadata.xml / Metadata.xml
        dtypes_map = ddf.dtypes.to_dict()
        cols = [(c, str(dtypes_map.get(c, "object")), None) for c in keep_cols]
        ra_idx = keep_cols.index(RA_NAME)
        dec_idx = keep_cols.index(DEC_NAME)
        write_metadata_xml(out_dir, cols, ra_idx, dec_idx)
    
        # properties
        n_src_total = int(densmaps[0].sum())
        write_properties(
            out_dir,
            cfg.output.cat_name,
            cfg.output.target,
            cfg.algorithm.level_limit,
            n_src_total,
            tile_format="tsv",
        )
    
        # arguments
        arg_text = textwrap.dedent(
            f"""
            # Input/output
            Input files: {paths}
            Input type: {cfg.input.format}
            Output dir: {out_dir}
            # Input data parameters
            Catalogue name: {cfg.output.cat_name}
            RA column name: {RA_NAME}
            DE column name: {DEC_NAME}
            # Selection parameters (coverage-based)
            level_limit(lM): {cfg.algorithm.level_limit}
            level_coverage(lC): {cfg.algorithm.level_coverage}
            coverage_order(Oc): {cfg.algorithm.coverage_order}
            order_desc: {cfg.algorithm.order_desc}
            k_per_cov_per_level: {cfg.algorithm.k_per_cov_per_level}
            targets_total_per_level: {cfg.algorithm.targets_total_per_level}
            tie_buffer: {cfg.algorithm.tie_buffer}
            density_mode: {cfg.algorithm.density_mode}
            k_per_cov_initial: {cfg.algorithm.k_per_cov_initial}
            targets_total_initial: {cfg.algorithm.targets_total_initial}
            density_exp_base: {cfg.algorithm.density_exp_base}
            """
        ).strip("\n")
        write_arguments(out_dir, arg_text + "\n")
    
    
        # =====================================================================
        # Coverage-based selection: uniform expected k per coverage (__icov__), no duplicates
        # =====================================================================
        remainder_ddf = ddf
    
        # If the base expected density and all per-level overrides are non-positive,
        # there is nothing to select.
        if (
            float(cfg.algorithm.k_per_cov_initial) <= 0.0
            and not (cfg.algorithm.k_per_cov_per_level or {})
        ):
            _log(
                "[selection] k_per_cov_initial <= 0 and no per-level overrides → "
                "nothing to select; finishing early.",
                always=True,
            )
            # graceful shutdown
            try:
                client.close()
            except Exception:
                pass
            try:
                cluster.close()
            except Exception:
                pass
            t1 = time.time()
            _log(f"END HiPS catalog pipeline. Elapsed {_fmt_dur(t1 - t0)}", always=True)
            try:
                with (out_dir / "process.log").open("a", encoding="utf-8") as f:
                    f.write("\n".join(log_lines) + "\n")
            except Exception as e:
                _log(f"{_ts()} | ERROR writing process.log: {type(e).__name__}: {e}")
            return
    
        Tmap = cfg.algorithm.targets_total_per_level or {}
    
        for depth in range(1, cfg.algorithm.level_limit + 1):
            depth_t0 = time.time()
    
            with _diag_ctx(f"dask_depth_{depth:02d}"):
    
                # Choose k for this level: density profile + per-level overrides
                # -----------------------------------------------------------------
                # 1) Base k_desired from the density profile (can be fractional).
                #    Two mutually exclusive modes are supported:
                #      * per-coverage profile  (k_per_cov_initial)
                #      * total-target profile  (targets_total_initial)
                # -----------------------------------------------------------------
                algo = cfg.algorithm
                mode = (getattr(algo, "density_mode", "constant") or "constant").lower()
                use_total_profile = getattr(algo, "targets_total_initial", None) is not None

                # Depth index for the profile (start at 1)
                delta = max(0, depth - 1)

                # Number of coverage cells (non-empty __icov__) at this depth,
                # used both for the total-target profile and for per-level caps.
                n_cov = None
                if use_total_profile or depth in Tmap:
                    try:
                        n_cov = int(remainder_ddf["__icov__"].dropna().nunique().compute())
                    except Exception:
                        n_cov = 0

                if use_total_profile:
                    # --- Total-target profile: evolve a total desired count per level ---
                    T0 = float(algo.targets_total_initial)

                    if mode == "constant":
                        T_desired = T0
                    elif mode == "linear":
                        # Grows linearly with depth (1×, 2×, 3×, ...)
                        T_desired = T0 * float(1 + delta)
                    elif mode == "exp":
                        # Grows exponentially with depth
                        base = float(getattr(algo, "density_exp_base", 2.0))
                        if base <= 1.0:
                            base = 2.0
                        T_desired = T0 * (base ** float(delta))
                    elif mode == "log":
                        # Grows roughly like log2(depth + const)
                        T_desired = T0 * math.log2(delta + 2.0)
                    else:
                        raise ValueError(f"Unknown density_mode: {algo.density_mode!r}")

                    if n_cov is None or n_cov <= 0:
                        # No coverage cells → nothing to select on this level
                        k_desired = 0.0
                    else:
                        # Convert total-target profile into per-coverage expectation
                        k_desired = T_desired / float(n_cov)

                    _log(
                        f"[DEPTH {depth}] start (density_mode={mode}, "
                        f"targets_total_initial={T0:.4f}, "
                        f"n_cov={n_cov}, k_desired_from_total={k_desired:.4f})",
                        always=True,
                    )
                else:
                    # --- Per-coverage profile: evolve k_per_cov_initial directly ---
                    k0 = float(algo.k_per_cov_initial)

                    if mode == "constant":
                        k_desired = k0
                    elif mode == "linear":
                        # Grows linearly with depth (1×, 2×, 3×, ...)
                        k_desired = k0 * float(1 + delta)
                    elif mode == "exp":
                        # Grows exponentially with depth
                        base = float(getattr(algo, "density_exp_base", 2.0))
                        if base <= 1.0:
                            base = 2.0
                        k_desired = k0 * (base ** float(delta))
                    elif mode == "log":
                        # Grows roughly like log2(depth + const)
                        k_desired = k0 * math.log2(delta + 2.0)
                    else:
                        raise ValueError(f"Unknown density_mode: {algo.density_mode!r}")

                    _log(
                        f"[DEPTH {depth}] start (density_mode={mode}, k_desired={k_desired:.4f})",
                        always=True,
                    )

                # 2) Per-level override (if provided) always wins over the profile
                if algo.k_per_cov_per_level and depth in algo.k_per_cov_per_level:
                    k_desired = float(algo.k_per_cov_per_level[depth])

                # Ensure non-negative
                k_desired = max(0.0, float(k_desired))

                _log_depth_stats(_log, depth, "start", counts=densmaps[depth])

                # Optionally apply a global total cap T_L for this level:
                #   - If T_L is set, we compute cap_per_cov = T_L / N_cov_nonempty,
                #     where N_cov_nonempty is the number of coverage cells (__icov__)
                #     that still have data in the current remainder.
                #   - The effective k_desired is then min(k_desired, cap_per_cov).
                if depth in Tmap:
                    T_L = float(Tmap[depth])
                    if n_cov is None:
                        try:
                            n_cov = int(remainder_ddf["__icov__"].dropna().nunique().compute())
                        except Exception:
                            n_cov = 0

                    if n_cov > 0 and T_L > 0.0:
                        cap_per_cov = T_L / float(n_cov)
                        if cap_per_cov < k_desired:
                            _log(
                                f"[DEPTH {depth}] applying total cap: T_L={int(T_L)}, "
                                f"N_cov={n_cov} → cap_per_cov={cap_per_cov:.4f} "
                                f"(before k_desired={k_desired:.4f})",
                                always=True,
                            )
                            k_desired = cap_per_cov
                    else:
                        _log(
                            f"[DEPTH {depth}] cannot apply total cap: "
                            f"T_L={T_L}, N_cov={n_cov}",
                            always=True,
                        )

                # If after capping k_desired <= 0, there is nothing to select at this level.
                if k_desired <= 0.0:
                    _log(f"[DEPTH {depth}] k_desired <= 0 → skipping this depth", always=True)
                    continue
        
                # We still need an integer ceiling for the candidate selection phase:
                # we select up to k_int rows per coverage cell, then let the fractional
                # part decide (probabilistically) whether to keep the "extra" row.
                k_int = max(1, int(math.ceil(k_desired)))
        
                # Narrow DF to needed columns (from remainder)
                needed_cols = list(remainder_ddf.columns)
                if cfg.columns.score not in needed_cols:
                    needed_cols.append(cfg.columns.score)
                if "__icov__" not in needed_cols:
                    needed_cols.append("__icov__")
                sel_ddf = remainder_ddf[needed_cols]
        
                # Partition-level candidate pass: keep (k + tie_buffer) per __icov__
                meta_cand = _get_meta_df(sel_ddf)
        
                cand_ddf = sel_ddf.map_partitions(
                    _candidates_by_coverage_partition,
                    score_col=cfg.columns.score,
                    order_desc=cfg.algorithm.order_desc,
                    # Use integer ceiling for the candidate set per coverage cell.
                    k_per_cov=k_int,
                    tie_buffer=int(cfg.algorithm.tie_buffer),
                    ra_col=RA_NAME,
                    dec_col=DEC_NAME,
                    meta=meta_cand,
                )
        
                # Shuffle so that rows with the same __icov__ are co-located and
                # groupby operations are efficient and deterministic.
                #
                # For standard inputs (parquet / CSV / TSV) we keep the existing
                # explicit shuffle("__icov__").
                #
                # For HATS / LSDB catalogs we *skip* this explicit shuffle and rely
                # on the native spatial partitioning provided by LSDB. The global
                # groupby("__icov__") in _reduce_coverage_exact_dask still enforces
                # the same selection logic, but we avoid disturbing HATS' own layout.
                target_parts = cfg.cluster.n_workers * cfg.cluster.threads_per_worker * 2
        
                if (not is_hats) and hasattr(cand_ddf, "shuffle"):
                    # Non-HATS path: keep the original shuffle-by-coverage behaviour.
                    cand_ddf = cand_ddf.shuffle(
                        "__icov__",
                        npartitions=max(target_parts, sel_ddf.npartitions),
                    )
                elif (not is_hats) and hasattr(cand_ddf, "_ddf") and hasattr(cand_ddf._ddf, "shuffle"):
                    # Non-HATS LSDB-like objects that expose a ._ddf with shuffle support.
                    base_ddf = cand_ddf._ddf  # type: ignore[attr-defined]
                    cand_ddf = base_ddf.shuffle(
                        "__icov__",
                        npartitions=max(target_parts, sel_ddf.npartitions),
                    )
                else:
                    # HATS / LSDB path (is_hats == True) or objects with no explicit shuffle:
                    # we keep the existing partitioning and let Dask + LSDB handle data
                    # movement internally during the groupby("__icov__") step.
                    _log(
                        f"[DEPTH {depth}] HATS / LSDB path or no shuffle available "
                        f"→ keeping native partitioning for __icov__",
                        always=True,
                    )
        
                # ------------------------------------------------------------------
                # Two modes controlled by avoid_computes_wherever_possible:
                #   1) "Avoid computes wherever possible" (avoid_computes=True):
                #        - selected_ddf = _reduce_coverage_exact_dask(cand_ddf, ...)
                #        - selected_pdf = selected_ddf.compute()
                #      This works for both plain Dask DataFrames and LSDB Catalogs.
                #   2) Use intermediate computes (avoid_computes=False):
                #        - cand_pdf = cand_ddf.compute()
                #        - selected_pdf = _reduce_coverage_exact(cand_pdf, ...)
                # ------------------------------------------------------------------
                if avoid_computes:
                    # Avoid materializing the full candidate set; we only materialize
                    # the final per-depth selection, which should be much smaller.
                    selected_ddf = _reduce_coverage_exact_dask(
                        cand_ddf,
                        score_col=cfg.columns.score,
                        order_desc=cfg.algorithm.order_desc,
                        k_per_cov=k_int,
                        ra_col=RA_NAME,
                        dec_col=DEC_NAME,
                    )
        
                    selected_pdf = selected_ddf.compute()
                    _log_depth_stats(
                        _log,
                        depth,
                        "selected_before_fractional",
                        selected_len=len(selected_pdf),
                    )
                else:
                    # Original behaviour: materialize candidates first, then reduce.
                    cand_pdf = cand_ddf.compute()
                    _log_depth_stats(_log, depth, "candidates", candidates_len=len(cand_pdf))
        
                    selected_pdf = _reduce_coverage_exact(
                        cand_pdf,
                        score_col=cfg.columns.score,
                        order_desc=cfg.algorithm.order_desc,
                        # Again, we use k_int here; fractional part is applied later.
                        k_per_cov=k_int,
                        ra_col=RA_NAME,
                        dec_col=DEC_NAME,
                    )
                    _log_depth_stats(
                        _log,
                        depth,
                        "selected_before_fractional",
                        selected_len=len(selected_pdf),
                    )
        
                # -----------------------------------------------------------------
                # 2bis) Apply fractional k: implement expected k_desired per coverage
                # -----------------------------------------------------------------
                # This step randomly drops some of the selected rows so that the
                # *expected* number of kept rows per coverage cell is k_desired.
                # Dropped rows are not written at this level and remain available
                # for deeper levels (because the remainder filter uses thresholds
                # computed only from the finally kept set).
                selected_pdf, _dropped_pdf = apply_fractional_k_per_cov(
                    selected_pdf,
                    k_desired=k_desired,
                    score_col=cfg.columns.score,
                    order_desc=cfg.algorithm.order_desc,
                    mode=getattr(cfg.algorithm, "fractional_mode", "random"),
                    ra_col=RA_NAME,
                    dec_col=DEC_NAME,
                )
                _log_depth_stats(_log, depth, "selected", selected_len=len(selected_pdf))
        
                # Map selected rows to ipix for this order
                if len(selected_pdf) > 0:
                    # Ensure we are working with plain NumPy float arrays
                    ra_vals = pd.to_numeric(selected_pdf[RA_NAME], errors="coerce").to_numpy()
                    dec_vals = pd.to_numeric(selected_pdf[DEC_NAME], errors="coerce").to_numpy()
        
                    theta = np.deg2rad(90.0 - dec_vals)            # colatitude
                    phi = np.deg2rad(ra_vals % 360.0)              # longitude in [0, 360)
        
                    NSIDE_L = 1 << depth
                    ipixL = hp.ang2pix(NSIDE_L, theta, phi, nest=True).astype(np.int64)
                    selected_pdf["__ipix__"] = ipixL
        
                # Write tiles + Allsky(1/2)
                header_line = build_header_line_from_keep(keep_cols)
                counts = densmaps[depth]
                allsky_needed = (depth in (1, 2))
        
                written_per_ipix, allsky_df = finalize_write_tiles(
                    out_dir=out_dir,
                    depth=depth,
                    header_line=header_line,
                    ra_col=RA_NAME,
                    dec_col=DEC_NAME,
                    counts=counts,
                    selected=selected_pdf,
                    order_desc=cfg.algorithm.order_desc,
                    allsky_collect=allsky_needed,
                )
                _log_depth_stats(_log, depth, "written", counts=densmaps[depth], written=written_per_ipix)
        
                if allsky_needed and allsky_df is not None and len(allsky_df) > 0:
                    norder_dir = out_dir / f"Norder{depth}"
                    norder_dir.mkdir(parents=True, exist_ok=True)
                    tmp_allsky = norder_dir / ".Allsky.tsv.tmp"
                    final_allsky = norder_dir / "Allsky.tsv"
        
                    nsrc_tot = int(counts.sum())
                    nwritten_tot = int(sum(written_per_ipix.values())) if written_per_ipix else 0
                    nremaining_tot = max(0, nsrc_tot - nwritten_tot)
                    completeness_header_allsky = f"# Completeness = {nremaining_tot} / {nsrc_tot}\n"
        
                    header_cols = header_line.strip("\n").split("\t")
                    allsky_cols = [c for c in header_cols if c in allsky_df.columns]
                    df_as = allsky_df[allsky_cols].copy()
        
                    with tmp_allsky.open("w", encoding="utf-8", newline="") as f:
                        f.write(completeness_header_allsky)
                        f.write(header_line)
        
                    obj_cols = df_as.select_dtypes(include=["object", "string"]).columns
                    if len(obj_cols) > 0:
                        df_as[obj_cols] = df_as[obj_cols].replace({r"[\t\r\n]": " "}, regex=True)
        
                    df_as.to_csv(
                        tmp_allsky,
                        sep="\t",
                        index=False,
                        header=False,
                        mode="a",
                        encoding="utf-8",
                        lineterminator="\n",
                    )
                    os.replace(tmp_allsky, final_allsky)
        
                # Build per-coverage thresholds and filter remainder
                thr_cov = build_cov_thresholds(
                    selected_pdf,
                    score_col=cfg.columns.score,
                    order_desc=cfg.algorithm.order_desc,
                )
                if len(thr_cov) == 0:
                    _log(f"[INFO] Depth {depth}: nothing selected; stopping selection loop.", always=True)
                    break
        
                remainder_meta = _get_meta_df(remainder_ddf)
        
                remainder_ddf = remainder_ddf.map_partitions(
                    filter_remainder_by_coverage_partition,
                    score_expr=cfg.columns.score,
                    order_desc=cfg.algorithm.order_desc,
                    thr_cov=thr_cov,
                    ra_col=RA_NAME,
                    dec_col=DEC_NAME,
                    meta=remainder_meta,
                )
        
                # Persist the filtered remainder only when we explicitly want
                # high in-memory performance. In low-memory mode we avoid
                # materializing this large dataframe in RAM.
                if persist_ddfs:
                    remainder_ddf = remainder_ddf.persist()
                    wait(remainder_ddf)
        
        
                # Optionally compute remainder size (can be expensive on huge datasets)
                #rem_len = None
                #if depth == 1 or depth == cfg.algorithm.level_limit:
                #    try:
                #        rem_len = int(remainder_ddf.shape[0].compute())
                #    except Exception:
                #        rem_len = None
                #
                #_log_depth_stats(_log, depth, "filtered", remainder_len=rem_len)
    
            _log(f"[DEPTH {depth}] done in {_fmt_dur(time.time() - depth_t0)}", always=True)

    try:
        # Choose diagnostics mode: global vs per-step vs off.
        if diagnostics_mode == "global":
            global_report = report_dir / "dask_global.html"
            with performance_report(filename=str(global_report)):
                _run_core_pipeline()
        else:
            # "per_step" or "off": no global report wrapper.
            _run_core_pipeline()
    finally:
        # graceful shutdown
        try:
            client.close()
        except Exception:
            pass
        try:
            cluster.close()
        except Exception:
            pass

        t1 = time.time()
        elapsed = t1 - t0
        _log(f"END HiPS catalog pipeline. Elapsed {_fmt_dur(elapsed)} ({elapsed:.3f} s)", always=True)

        # persist log
        try:
            with (out_dir / "process.log").open("a", encoding="utf-8") as f:
                f.write("\n".join(log_lines) + "\n")
        except Exception as e:
            _log(f"{_ts()} | ERROR writing process.log: {type(e).__name__}: {e}", always=True)

# =============================================================================
# CLI
# =============================================================================

def load_config(path: str) -> Config:
    with open(path, "r", encoding="utf-8") as f:
        y = yaml.safe_load(f)

    algo = y["algorithm"]

    # --- coverage / MOC orders ---
    level_limit = int(algo["level_limit"])

    raw_level_coverage = algo.get("level_coverage")
    raw_coverage_order = algo.get("coverage_order")

    # If only one of level_coverage / coverage_order is provided, use it for the other.
    if raw_level_coverage is None and raw_coverage_order is None:
        # Default: use level_limit for both if neither is explicitly given
        raw_level_coverage = level_limit
        raw_coverage_order = level_limit
    elif raw_level_coverage is None:
        raw_level_coverage = raw_coverage_order
    elif raw_coverage_order is None:
        raw_coverage_order = raw_level_coverage

    level_coverage = int(raw_level_coverage)
    coverage_order = int(raw_coverage_order)

    # --- density / selection parameters ---
    density_mode = algo.get("density_mode", "constant")

    # Mutually exclusive initial parameters:
    #   * k_per_cov_initial       → base expected rows per coverage cell (depth=1)
    #   * targets_total_initial   → base expected *total* rows per level (depth=1)
    #
    # Only one of them should be defined in the YAML. If both are provided,
    # raise an error to avoid ambiguous behaviour.
    raw_k_per_cov_initial = algo.get("k_per_cov_initial", None)
    raw_targets_total_initial = algo.get("targets_total_initial", None)

    if raw_k_per_cov_initial is not None and raw_targets_total_initial is not None:
        raise ValueError(
            "algorithm.k_per_cov_initial and algorithm.targets_total_initial "
            "are mutually exclusive. Please define only one of them in the YAML."
        )

    if raw_k_per_cov_initial is not None:
        k_per_cov_initial = float(raw_k_per_cov_initial)
        targets_total_initial = None
    elif raw_targets_total_initial is not None:
        targets_total_initial = float(raw_targets_total_initial)
        # k_per_cov_initial is not used by the density profile when
        # targets_total_initial is set, but we keep a harmless default
        # for completeness / backward-compatibility.
        k_per_cov_initial = 1.0
    else:
        # Default behaviour (backward-compatible): per-coverage profile
        # with k_per_cov_initial = 1.0 and no total-target profile.
        k_per_cov_initial = 1.0
        targets_total_initial = None

    cfg = Config(
        input=InputCfg(
            paths=y["input"]["paths"],
            format=y["input"].get("format", "parquet"),
            header=y["input"].get("header", True),
            ascii_format=y["input"].get("ascii_format"),
        ),
        columns=ColumnsCfg(
            ra=y["columns"]["ra"],
            dec=y["columns"]["dec"],
            score=y["columns"]["score"],
            keep=y["columns"].get("keep"),
        ),
        algorithm=AlgoOpts(
            level_limit=level_limit,
            level_coverage=level_coverage,
            order_desc=bool(algo.get("order_desc", False)),
            coverage_order=coverage_order,

            # Optional per-level overrides: convert keys to int and values to float.
            k_per_cov_per_level=(
                {int(k): float(v) for k, v in algo.get("k_per_cov_per_level", {}).items()}
                if isinstance(algo.get("k_per_cov_per_level"), dict)
                else None
            ),

            # Total caps per level remain integers (total rows per level).
            targets_total_per_level=(
                {int(k): int(v) for k, v in algo.get("targets_total_per_level", {}).items()}
                if isinstance(algo.get("targets_total_per_level"), dict)
                else None
            ),

            tie_buffer=int(algo.get("tie_buffer", 10)),

            density_mode=density_mode,
            k_per_cov_initial=k_per_cov_initial,
            targets_total_initial=targets_total_initial,
            density_exp_base=float(algo.get("density_exp_base", 2.0)),

            fractional_mode=algo.get("fractional_mode", "random"),

            # Use HATS/LSDB partitions as coverage cells when True
            use_hats_as_coverage=bool(algo.get("use_hats_as_coverage", False)),
        ),
        cluster=ClusterCfg(
            mode=y["cluster"].get("mode", "local"),
            n_workers=int(y["cluster"].get("n_workers", 4)),
            threads_per_worker=int(y["cluster"].get("threads_per_worker", 1)),
            memory_per_worker=str(y["cluster"].get("memory_per_worker", "4GB")),
            slurm=y["cluster"].get("slurm"),
            # Optional flags to control how aggressively we use memory vs. computes
            persist_ddfs=bool(y["cluster"].get("persist_ddfs", False)),
            avoid_computes_wherever_possible=bool(
                y["cluster"].get("avoid_computes_wherever_possible", True)
            ),
            diagnostics_mode=y["cluster"].get("diagnostics_mode", "per_step"),
        ),
        output=OutputCfg(
            out_dir=y["output"]["out_dir"],
            cat_name=y["output"]["cat_name"],
            target=y["output"].get("target", "0 0"),
        ),
    )

    # Align lC if user set it above lM
    if cfg.algorithm.level_coverage > cfg.algorithm.level_limit:
        cfg.algorithm.level_coverage = cfg.algorithm.level_limit

    return cfg


def main(argv: List[str]):
    ap = argparse.ArgumentParser(
        description="HiPS Catalog Pipeline (Dask, Parquet, coverage-based selection)"
    )
    ap.add_argument("--config", required=True, help="YAML configuration file")
    args = ap.parse_args(argv)
    cfg = load_config(args.config)
    run_pipeline(cfg)


if __name__ == "__main__":
    main(sys.argv[1:])

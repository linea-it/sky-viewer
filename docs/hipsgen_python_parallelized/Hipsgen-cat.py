#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HiPS Catalog Pipeline (Dask, Parquet-only) — CDS-faithful selection + methods
-----------------------------------------------------------------------------

This script generates a HiPS catalog layout matching the CDS tool output
structure (Csv2ProgCatStandalone), while reading only Dask-native formats
(Parquet, optionally CSV/TSV) and scaling out with Dask (LocalCluster/SLURM).

Faithful behaviors covered (no 'id' column required):
- Depth 1 (Norder1): distribute ~n1 rows via density-weighted quotas
- Depth 2 (Norder2): distribute ~n2 rows via density-weighted quotas
- Depth 3 (Norder3): per-pixel quota = clip(round(no * rl3l4), n3_min, n3_max), capped by availability
- Depth ≥ 4        : per-pixel quota limited by nm ≤ quota ≤ nM, capped by availability
- All depths       : if order_desc=True -> keep highest scores; else keep lowest scores
- Allsky.tsv       : contains only the rows actually selected at Norder1 and Norder2
- Completeness     : each tile starts with “# Completeness = {remaining} / {total_in_cell}”
- Methods          : weighting functions for L1/L2 quotas: LINEAR / LOG (default) / ASINH / SQRT / POW2
                     and normalization to hit the global targets exactly (subject to availability)
- l1l2_only        : optional mode to stop after generating Norder1/2 (testing convenience)
- Level bounds     : level_limit lM validated to [4, 11] to mirror CDS tool constraints

Dependencies (minimal):
- dask[dataframe]
- dask-jobqueue (for SLURMCluster)
- pandas, pyarrow (Parquet)
- astropy (FITS, VOTable)
- mocpy (MOC FITS/JSON)
- healpy (HEALPix hashing)

Usage:
    python hips_catalog_pipeline_dask.py --config config.yaml
"""
from __future__ import annotations

import re
import os
import sys
import json
import time
import glob
import dask
import shutil
import hashlib
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple, Optional, Dict, Callable

import yaml
import numpy as np
import pandas as pd
import dask.dataframe as dd
from dask.distributed import Client, LocalCluster, wait

try:
    from dask_jobqueue import SLURMCluster  # optional
except Exception:
    SLURMCluster = None

from astropy.io import fits
from astropy.table import Table
from astropy.io.votable import from_table, writeto as vot_writeto

import healpy as hp
from mocpy import MOC


# =============================================================================
# Configuration dataclasses
# =============================================================================

@dataclass
class AlgoOpts:
    simple_algo: bool        # when True, use the "simple" path: L1/L2 ~ n1/n2; >=L3 use only `no`
    order_desc: bool
    method: str              # 'LINEAR' | 'LOG' | 'ASINH' | 'SQRT' | 'POW2' (default LOG)
    level_limit: int         # lM (must be 4..11 to mirror CDS tool)
    level_coverage: int      # lC
    n1: int
    n2: int
    n3_min: int
    n3_max: int
    no: int                  # base per-pixel quota (>= L3 in simple mode)
    nm: int                  # min per-pixel for depth ≥ 4 (non-simple mode)
    nM: int                  # max per-pixel for depth ≥ 4 (non-simple mode)
    rl3l4: float             # ratio for depth 3 relative to base 'no' (non-simple mode)
    l1l2_only: bool          # stop after producing Norder1 and Norder2
    print_info: bool = False # optional: print informational summaries like -p/--print_info
    tie_buffer: int = 16     # extra candidates per ipix to absorb score ties at the cutoff

@dataclass
class ColumnsCfg:
    ra: str
    dec: str
    # Score expression: a single column or a Python expression using column names
    score: str
    # Optional explicit list of columns to keep in the output tiles.
    # RA/DEC and score dependencies are always kept even if not listed here.
    keep: Optional[List[str]] = None

@dataclass
class InputCfg:
    paths: List[str]            # list of globs for Parquet/CSV/TSV files
    format: str                 # 'parquet' | 'csv' | 'tsv'
    header: bool                # kept for parity; ignored for parquet
    ascii_format: Optional[str] = None  # optional hint: 'CSV' or 'TSV' (others not supported in Dask path)

@dataclass
class ClusterCfg:
    mode: str                  # 'local' | 'slurm'
    n_workers: int
    threads_per_worker: int
    memory_per_worker: str     # e.g., '8GB'
    slurm: Optional[Dict] = None  # e.g., {'queue': 'cpu_dev', 'account': '...', 'job_extra_directives': [...]}

@dataclass
class OutputCfg:
    out_dir: str
    cat_name: str
    target: str                # "<RA0> <DEC0>" for properties

@dataclass
class DebugCfg:
    origin: bool = False              # include __src__ and run debug checks
    ranges_check: bool = False        # per-partition ranges check
    containment_check: bool = False   # selected ⊆ input via hash

@dataclass
class Config:
    input: InputCfg
    columns: ColumnsCfg
    algorithm: AlgoOpts
    cluster: ClusterCfg
    output: OutputCfg
    debug: DebugCfg


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


# -----------------------------------------------------------------------------
# Canonical hash (RA, DEC, MAG) para containment check
# -----------------------------------------------------------------------------
def _canon_hash_pdf(pdf: pd.DataFrame, ra_col: str, dec_col: str, mag_col: str) -> pd.Series:
    ra = pd.to_numeric(pdf[ra_col], errors="coerce").astype(np.float64).round(8).astype(str)
    dec = pd.to_numeric(pdf[dec_col], errors="coerce").astype(np.float64).round(8).astype(str)
    mag = pd.to_numeric(pdf[mag_col], errors="coerce").astype(np.float64).round(6).astype(str)
    s = ra + "|" + dec + "|" + mag
    return s.map(lambda x: hashlib.md5(x.encode()).hexdigest())


# -----------------------------------------------------------------------------
# Score dependency extraction (best-effort)
# -----------------------------------------------------------------------------
_ID_RE = re.compile(r"[A-Za-z_]\w*")

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
        # not an integer, fallback to name
        if spec not in ddf.columns:
            raise KeyError(f"Column '{spec}' not found (header=False).")
        return spec

# -----------------------------------------------------------------------------
# Build Dask DataFrame from a list of files, optionally tagging each row
# with the file it came from as __src__ (debug.origin mode).
# -----------------------------------------------------------------------------
def _build_input_ddf(paths: List[str],
                     cfg: Config,
                     _log: Callable[[str, bool], None]) -> Tuple[dd.DataFrame, str, str, List[str]]:
    """
    Returns:
      ddf       : Dask DataFrame ready to use (optionally with __src__)
      RA_NAME  : resolved RA column name
      DEC_NAME : resolved DEC column name
      keep_cols: final ordered list of columns kept (header order for tiles)
    """
    assert len(paths) > 0, "No input files matched."

    # 1) Base read (no __src__) to discover columns and resolve RA/DEC.
    if cfg.input.format.lower() == "parquet":
        ddf0 = dd.read_parquet(paths, engine="pyarrow")
    elif cfg.input.format.lower() in ("csv", "tsv"):
        ascii_fmt = (cfg.input.ascii_format or "").upper().strip()
        if ascii_fmt in ("CSV", ""):
            sep = ","
        elif ascii_fmt == "TSV":
            sep = "\t"
        else:
            sep = "," if cfg.input.format.lower() == "csv" else "\t"
        if cfg.input.header:
            ddf0 = dd.read_csv(paths, sep=sep, assume_missing=True)
        else:
            ddf0 = dd.read_csv(paths, sep=sep, header=None, assume_missing=True)
    else:
        raise ValueError("Unsupported input.format; use 'parquet', 'csv', or 'tsv'.")

    # Resolve RA/DEC allowing numeric index when header=False on CSV/TSV.
    ra_col = _resolve_col_name(cfg.columns.ra, ddf0, header=(cfg.input.format.lower()=="parquet" or cfg.input.header))
    dec_col = _resolve_col_name(cfg.columns.dec, ddf0, header=(cfg.input.format.lower()=="parquet" or cfg.input.header))
    RA_NAME = ra_col
    DEC_NAME = dec_col

    # 2) Column selection (preserve order; ensure score deps).
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

    # 3) Build final ddf.
    if cfg.debug.origin:
        parts = []
    
        # nunca peça '__src__' ao parquet
        keep_cols_no_src = [c for c in keep_cols if c != "__src__"]
    
        for f in sorted(paths):
            if cfg.input.format.lower() == "parquet":
                # ⛔️ Evita que o Dask empurre '__src__' para dentro do read_parquet
                with dask.config.set({"dataframe.optimize.getitem": False}):
                    dfi = dd.read_parquet(f, engine="pyarrow", columns=keep_cols_no_src)
            else:
                ascii_fmt = (cfg.input.ascii_format or "").upper().strip()
                sep = "," if ascii_fmt in ("CSV", "") and cfg.input.format.lower() == "csv" else \
                      "\t" if ascii_fmt == "TSV" or cfg.input.format.lower() == "tsv" else ","
                dfi = dd.read_csv(
                    f, sep=sep, assume_missing=True,
                    header=(None if not cfg.input.header else "infer"),
                    usecols=keep_cols_no_src
                )
    
            # adiciona a coluna de origem DEPOIS de ler
            dfi = dfi.map_partitions(lambda pdf, src=f: pdf.assign(__src__=src),
                                     meta={**{c: dfi._meta[c] for c in keep_cols_no_src}, "__src__": "object"})
            parts.append(dfi)
    
        ddf = dd.concat(parts, interleave_partitions=True)
    
        # garante ordem estável no cabeçalho dos tiles
        if "__src__" not in keep_cols:
            keep_cols.append("__src__")
        ddf = ddf[keep_cols]
    else:
        ddf = ddf0[keep_cols]

    return ddf, RA_NAME, DEC_NAME, keep_cols


# =============================================================================
# Column report (lightweight JSON akin to cds.catana’s JSONReportWriter)
# =============================================================================

def compute_column_report(ddf: dd.DataFrame, sample_rows: int = 200_000) -> Dict:
    """Build a small column summary on a sample (keeps it fast and scalable)."""
    sample = ddf.sample(
        frac=min(1.0, sample_rows / max(1, len(ddf.columns) * 10_000)),
        replace=False
    ) if isinstance(ddf, dd.DataFrame) else ddf

    pdf = sample.head(200_000, compute=True)
    report = {}
    for c in pdf.columns:
        s = pdf[c]
        col = {"dtype": str(s.dtype), "n_null": int(s.isna().sum())}
        if pd.api.types.is_numeric_dtype(s):
            if len(s):
                col.update({
                    "min": float(np.nanmin(s.values)),
                    "max": float(np.nanmax(s.values)),
                    "mean": float(np.nanmean(s.values)),
                })
            else:
                col.update({"min": np.nan, "max": np.nan, "mean": np.nan})
        else:
            example = next((x for x in s.values if pd.notna(x)), "")
            col.update({"example": str(example)})
        report[c] = col
    return {"columns": report}


# =============================================================================
# HEALPix helpers & density maps
# =============================================================================

def ipix_for_depth(ra_deg: np.ndarray, dec_deg: np.ndarray, depth: int) -> np.ndarray:
    """Return HEALPix NESTED pixel index for a given depth (order)."""
    nside = 1 << depth
    theta = np.deg2rad(90.0 - dec_deg)  # colatitude
    phi = np.deg2rad(ra_deg)            # longitude
    return hp.ang2pix(nside, theta, phi, nest=True)


def densmap_for_depth(ddf: dd.DataFrame, ra_col: str, dec_col: str, depth: int) -> np.ndarray:
    """
    Count rows per HEALPix (NESTED) at 'depth' via per-partition histograms and a
    delayed tree-reduction. This avoids full-dataframe shuffles and works robustly
    with Dask DataFrame partitions.
    """
    # Local imports keep the function self-contained
    import numpy as _np
    from dask import delayed as _delayed

    nside = 1 << depth
    npix = hp.nside2npix(nside)

    def _part_hist(pdf: pd.DataFrame) -> _np.ndarray:
        """Build a fixed-length histogram (npix) for one pandas partition."""
        if pdf is None or len(pdf) == 0:
            return _np.zeros(npix, dtype=_np.int64)
        ip = ipix_for_depth(pdf[ra_col].to_numpy(), pdf[dec_col].to_numpy(), depth)
        # bincount with minlength ensures a fixed-length vector
        return _np.bincount(ip, minlength=npix).astype(_np.int64)

    # Create one delayed histogram task per partition
    part_delayed = ddf.to_delayed()
    hists = [_delayed(_part_hist)(p) for p in part_delayed]

    # Sum all histograms with a delayed reduction (tree-like)
    if len(hists) == 0:
        return _np.zeros(npix, dtype=_np.int64)

    def _sum_vecs(vecs: list[_np.ndarray]) -> _np.ndarray:
        return _np.sum(vecs, axis=0, dtype=_np.int64)

    total = _delayed(_sum_vecs)(hists)
    dens = total.compute()  # materialize as a single numpy vector

    return dens


# =============================================================================
# Coverage helpers (MOC-like percentage per pixel at a given depth)
# =============================================================================

def _coverage_pct_from_lc(depth: int, level_coverage: int, dens_lc: np.ndarray) -> np.ndarray:
    """
    Compute percentage coverage per pixel at 'depth' using the densmap at lC.
    Coverage at depth <= lC is the fraction of its 4^(lC - depth) children at lC that are non-zero.
    For depth > lC we treat coverage as 1.0 (fully covered) for quota shaping.
    """
    if depth < 0:
        raise ValueError("depth must be >= 0")
    if level_coverage < 0:
        raise ValueError("level_coverage must be >= 0")

    if depth > level_coverage:
        nside = 1 << depth
        npix = hp.nside2npix(nside)
        return np.ones(npix, dtype=np.float64)

    nside_d = 1 << depth
    nside_c = 1 << level_coverage
    npix_d = hp.nside2npix(nside_d)

    # Same order: coverage is simply the non-zero mask at lC
    if nside_c == nside_d:
        return (dens_lc > 0).astype(np.float64)

    # Number of children per parent pixel (NESTED): 4^(lC - depth)
    nc = 4 ** (level_coverage - depth)

    mask_c = (dens_lc > 0).astype(np.int32)

    # Fast path: in NESTED indexing, children of parent p occupy [p*nc, (p+1)*nc)
    if mask_c.size == nc * npix_d:
        return mask_c.reshape(npix_d, nc).mean(axis=1).astype(np.float64)

    # Fallback: compute parent indices explicitly (robust to odd layouts)
    factor = 4 ** (level_coverage - depth)
    parent = np.arange(mask_c.size, dtype=np.int64) // factor
    sums = np.bincount(parent, weights=mask_c, minlength=npix_d)
    return (sums / float(factor)).astype(np.float64)


def compute_coverage_pct_for_depths(level_limit: int, level_coverage: int, dens_lc: np.ndarray) -> Dict[int, np.ndarray]:
    """
    Precompute coverage percentage arrays for depths 0..level_limit using the lC densmap.
    Returns a dict depth -> coverage_pct (float64 in [0,1]).
    """
    cov = {}
    for d in range(0, level_limit + 1):
        cov[d] = _coverage_pct_from_lc(d, level_coverage, dens_lc)
    return cov


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

    def write_finalize(self, tmp_path: Path, final_path: Path, completeness_header: str):
        with final_path.open("wb") as fout:
            fout.write(completeness_header.encode("utf-8"))
        with final_path.open("ab") as fout:
            fout.write(self.header_line.encode("utf-8"))
            with tmp_path.open("rb") as tin:
                shutil.copyfileobj(tin, fout)
        tmp_path.unlink(missing_ok=True)


# =============================================================================
# Metadata writers (properties, metadata.xml, MOC, densmaps, arguments)
# =============================================================================

def write_properties(out_dir: Path, label: str, target: str, level_limit: int, n_src: int, tile_format: str = "tsv"):
    now = _now_str()
    buf = []
    buf.append("# Generated by the CDS HiPS tool for catalogues.\n")
    buf.append(f"# {now}\n")
    buf.append(f"publisher_did     = ivo://PRIVATE_USER/{label}\n")
    buf.append("dataproduct_type  = catalog\n")
    buf.append(f"hips_service_url  = {out_dir}{label}\n")  # keep as-is (local path acceptable); change to URL if publishing
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


def write_metadata_xml(out_dir: Path, columns: List[Tuple[str, str, Optional[str]]], ra_idx: int, dec_idx: int):
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

    # write both lower/upper case variants
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
    col = fits.Column(name='VALUE', array=counts.astype(np.int64), format='K')
    hdu1 = fits.BinTableHDU.from_columns([col])
    fits.HDUList([hdu0, hdu1]).writeto(out_dir / f"densmap_o{depth}.fits", overwrite=True)


# =============================================================================
# Weighting methods for L1/L2 + exact-normalization to hit targets
# =============================================================================

def _method_fn(name: str) -> Callable[[np.ndarray], np.ndarray]:
    name = (name or "LOG").upper()
    if name == "LINEAR":
        return lambda x: x.astype(np.float64)
    if name == "LOG":
        return lambda x: np.log1p(x.astype(np.float64))
    if name == "ASINH":
        return lambda x: np.arcsinh(x.astype(np.float64))
    if name == "SQRT":
        return lambda x: np.sqrt(x.astype(np.float64))
    if name == "POW2":
        return lambda x: (x.astype(np.float64) ** 2.0)
    # default
    return lambda x: np.log1p(x.astype(np.float64))


def _allocate_with_caps(counts: np.ndarray, weights: np.ndarray, target_total: int) -> np.ndarray:
    """
    Proportional allocation with flooring + remainder distribution,
    capped by availability (counts). If weights sum to zero or target<=0 -> zeros.
    """
    q = np.zeros_like(counts, dtype=np.int64)
    if target_total <= 0 or counts.sum() == 0:
        return q
    total_w = float(weights.sum())
    if total_w <= 0.0:
        return q

    raw = weights * (target_total / total_w)
    base = np.floor(raw).astype(np.int64)
    q = np.minimum(counts, base)

    missing = target_total - int(q.sum())
    if missing <= 0:
        return q

    frac = raw - base
    order = np.argsort(-frac)  # descending by fractional part
    for idx in order:
        if q[idx] < counts[idx]:
            q[idx] += 1
            missing -= 1
            if missing == 0:
                break
    return q


# =============================================================================
# CDS-faithful quotas per depth
# =============================================================================

def compute_quotas_for_depth(depth: int,
                             counts: np.ndarray,
                             cfg_algo: AlgoOpts,
                             coverage_pct: Optional[np.ndarray] = None) -> np.ndarray:
    """
    Per-pixel integer quotas for a given depth, CDS-like, with coverage shaping.

    Non-simple mode (default):
      - depth == 1: distribute ~n1 using 'method' weights + exact normalization, capped by availability
      - depth == 2: distribute ~n2 likewise
      - depth == 3: per-pixel quota = min(counts[p], clip(round(no * rl3l4), n3_min, n3_max)),
                    then multiplied by coverage percentage and rounded (>=1 where counts>0)
      - depth >= 4: per-pixel quota clipped to [nm, nM], capped by availability,
                    multiplied by coverage percentage and rounded (>=1 where counts>0)

    Simple mode (cfg_algo.simple_algo=True; mirrors '-simple'):
      - depth == 1/2: linear weights to hit n1/n2 (as before)
      - depth >= 3: per-pixel quota = min(counts[p], round(coverage_pct[p] * no))
    """
    npix = len(counts)
    q = np.zeros(npix, dtype=np.int64)
    cov = coverage_pct if coverage_pct is not None else np.ones(npix, dtype=np.float64)
    cov = np.clip(cov, 0.0, 1.0)

    # --- Simple mode ---
    if cfg_algo.simple_algo:
        if depth in (1, 2):
            target_total = int(cfg_algo.n1 if depth == 1 else cfg_algo.n2)
            if target_total <= 0 or counts.sum() == 0:
                return q
            weights = counts.astype(np.float64)
            weights[counts == 0] = 0.0
            return _allocate_with_caps(counts, weights, target_total)
        else:
            base = np.round(cov * float(cfg_algo.no)).astype(np.int64)
            base[base < 0] = 0
            base = np.minimum(counts, base)
            return base

    # --- Non-simple mode (default) ---
    if depth in (1, 2):
        target_total = int(cfg_algo.n1 if depth == 1 else cfg_algo.n2)
        if target_total <= 0 or counts.sum() == 0:
            return q
        wfn = _method_fn(cfg_algo.method)
        weights = wfn(counts)
        weights[counts == 0] = 0.0
        return _allocate_with_caps(counts, weights, target_total)

    if depth == 3:
        base_l3 = int(round(cfg_algo.no * float(cfg_algo.rl3l4)))
        base_l3 = max(cfg_algo.n3_min, min(cfg_algo.n3_max, base_l3))
        base = np.full(npix, base_l3, dtype=np.int64)

        # Coverage shaping
        shaped = np.round(base * cov).astype(np.int64)
        # Guarantee at least 1 where there is availability
        shaped = np.where(counts > 0, np.maximum(shaped, 1), 0)
        np.minimum(counts, shaped, out=q)
        return q

    # depth >= 4
    base = np.minimum(counts, int(cfg_algo.nM)).astype(np.int64)
    nm = int(cfg_algo.nm)
    if nm > 0:
        base = np.where(counts >= nm, np.maximum(base, nm), np.minimum(base, counts))

    shaped = np.round(base * cov).astype(np.int64)
    shaped = np.where(counts > 0, np.maximum(shaped, 1), 0)
    np.minimum(counts, shaped, out=q)
    return q


# =============================================================================
# Partition writer — uses quotas + order_desc
# =============================================================================

def produce_candidates_partition(pdf: pd.DataFrame,
                                 depth: int,
                                 ra_col: str,
                                 dec_col: str,
                                 keep_quotas_npy: str,
                                 order_desc: bool,
                                 columns_plus_score: List[str],
                                 tie_buffer: int) -> pd.DataFrame:
    """
    Partition-level candidate producer (no I/O):
      - compute score
      - compute __ipix__ at the given depth
      - per ipix, keep top (quota + tie_buffer) with stable tie-break keys
      - return only candidates; final selection & writing happen globally
    Returned columns:
      ["__ipix__", "__score__", *tile_columns]
    """
    # Empty fast-path
    if len(pdf) == 0 or len(columns_plus_score) < 1:
        return pd.DataFrame({
            "__ipix__": pd.Series(dtype="int64"),
            "__score__": pd.Series(dtype="float64"),
        })

    columns = columns_plus_score[:-1]
    score_expr = columns_plus_score[-1]

    # Build score series (column or Python expression on existing columns)
    if score_expr in pdf.columns:
        sc = pd.to_numeric(pdf[score_expr], errors="coerce")
    else:
        code = compile(score_expr, "<score>", "eval")
        env = {col: pdf[col] for col in pdf.columns if col in columns}
        out = eval(code, {"__builtins__": {}}, env)
        sc = pd.to_numeric(out, errors="coerce")

    # Make NaNs worst depending on sort direction
    sc = pd.Series(sc.values, index=pdf.index).replace([np.inf, -np.inf], np.nan)
    sc = sc.fillna(-np.inf if order_desc else np.inf)

    # HEALPix index at this depth
    ipix = ipix_for_depth(pdf[ra_col].to_numpy(), pdf[dec_col].to_numpy(), depth)

    # Load quotas (memmap)
    quotas = np.load(keep_quotas_npy, mmap_mode="r")

    # Prepare DF with ipix/score and the tile columns
    tile_cols = [c for c in pdf.columns if c in columns]  # preserve column order
    df2 = pdf[tile_cols].copy()
    df2["__ipix__"] = ipix
    df2["__score__"] = sc

    # Stable tie-break: sort by (score, RA, DEC) asc or desc on score, RA/DEC asc
    # NOTE: RA/DEC must exist among tile columns.
    sort_cols = ["__score__", ra_col, dec_col]
    ascending = [not order_desc, True, True]

    out_parts = []
    for pid, g in df2.groupby("__ipix__"):
        pid = int(pid)
        if pid < 0 or pid >= len(quotas):
            continue
        q = int(quotas[pid])
        if q <= 0 or len(g) == 0:
            continue
        k_buf = q + int(tie_buffer)
        gg = g.sort_values(sort_cols, ascending=ascending, kind="mergesort").head(k_buf)
        out_parts.append(gg)

    if not out_parts:
        return pd.DataFrame({
            "__ipix__": pd.Series(dtype="int64"),
            "__score__": pd.Series(dtype="float64"),
        })

    return pd.concat(out_parts, ignore_index=True)

def reduce_candidates_global(pdf: pd.DataFrame,
                             depth: int,
                             ra_col: str,
                             dec_col: str,
                             quotas_npy: str,
                             order_desc: bool,
                             tie_buffer: int) -> pd.DataFrame:
    """
    Second pass at pandas (single-partition DataFrame):
      For each ipix, keep top (quota + tie_buffer) again, globally.
      Returns a compact set of candidates per ipix for final cut.
    """
    if len(pdf) == 0:
        return pdf

    quotas = np.load(quotas_npy, mmap_mode="r")
    sort_cols = ["__score__", ra_col, dec_col]
    ascending = [not order_desc, True, True]

    out_parts = []
    for pid, g in pdf.groupby("__ipix__"):
        pid = int(pid)
        if pid < 0 or pid >= len(quotas):
            continue
        q = int(quotas[pid])
        if q <= 0 or len(g) == 0:
            continue
        k_buf = q + int(tie_buffer)
        gg = g.sort_values(sort_cols, ascending=ascending, kind="mergesort").head(k_buf)
        out_parts.append(gg)

    if not out_parts:
        return pdf.iloc[0:0]

    return pd.concat(out_parts, ignore_index=True)


def finalize_write_tiles(out_dir: Path,
                         depth: int,
                         header_line: str,
                         ra_col: str,
                         dec_col: str,
                         counts: np.ndarray,
                         selected: pd.DataFrame,
                         order_desc: bool,
                         stage_dir: Optional[Path] = None,
                         allsky_collect: bool = False) -> Tuple[Dict[int, int], Optional[pd.DataFrame]]:
    """
    Write one TSV per HEALPix cell (atomic rename), with a Completeness header
    and a single header line, followed by the selected rows in the SAME column
    order as the header. Uses pandas.to_csv to avoid partial/truncated first line.

    Why this fix:
      - Manual "\t".join(...) can yield truncated first row in rare cases.
      - pandas.to_csv handles quoting/newlines robustly.
      - We derive the exact header order from `header_line`.
    """
    writer = TSVTileWriter(out_dir, depth, header_line)
    npix = len(counts)
    written: Dict[int, int] = {}
    allsky_rows: List[List[str]] = []

    # Derive the header order from header_line (robust and local, no global ddf needed).
    header_cols = header_line.strip("\n").split("\t")

    # Keep only the tile columns, in the exact header order (drop internals).
    tile_cols = [c for c in header_cols if c in selected.columns]

    # Fast exit: nothing to write
    if selected is None or len(selected) == 0 or len(tile_cols) == 0:
        return {}, None

    # We'll group by ipix and write per-cell atomically.
    for pid, g in selected.groupby("__ipix__"):
        ip = int(pid)
        if ip < 0 or ip >= npix:
            continue

        n_src_cell = int(counts[ip])

        # Ensure we have a DataFrame with exactly the columns in header order.
        # Cast to str only at write-time (handled by pandas), not earlier.
        g_tile = g[tile_cols].copy()

        n_written = int(len(g_tile))
        written[ip] = n_written
        n_remaining = max(0, n_src_cell - n_written)

        completeness_header = f"# Completeness = {n_remaining} / {n_src_cell}\n"

        final_path = writer.cell_path(ip)
        final_path.parent.mkdir(parents=True, exist_ok=True)

        # Write to a temp path in the same directory, then atomic replace.
        tmp = final_path.with_name(f".Npix{ip}.tsv.tmp")

        # 1) Write the two header lines in text mode (UTF-8).
        with tmp.open("w", encoding="utf-8", newline="") as f:
            f.write(completeness_header)
            f.write(header_line)

        # Sanitize string columns to avoid embedded tabs/newlines breaking TSV rows
        # (only needed if you keep object/string columns in tiles)
        obj_cols = g_tile.select_dtypes(include=["object", "string"]).columns
        if len(obj_cols) > 0:
            g_tile[obj_cols] = g_tile[obj_cols].replace({r"[\t\r\n]": " "}, regex=True)
        
        # 2) Append rows using pandas.to_csv (robust serialization).
        #    - sep = '\t'
        #    - header = False (we already wrote the header_line)
        #    - index = False
        g_tile.to_csv(
            tmp,
            sep="\t",
            index=False,
            header=False,
            mode="a",
            encoding="utf-8",
            lineterminator="\n",
            float_format=None  # let pandas write full precision as needed
        )

        # 3) Atomic move into place
        os.replace(tmp, final_path)

        if allsky_collect and n_written > 0:
            # Collect for Allsky in the same column order for later to_csv.
            # Store as list of lists (strings are handled at write time).
            allsky_rows.extend(g_tile.values.tolist())

    # Build Allsky DataFrame if requested (for depths 1/2 only).
    allsky_df = None
    if allsky_collect and allsky_rows:
        allsky_df = pd.DataFrame(allsky_rows, columns=tile_cols)

    return written, allsky_df


def build_thresholds(selected: pd.DataFrame,
                     order_desc: bool) -> Dict[int, float]:
    """
    Compute per-ipix thresholds from the final selected rows:
      - ascending  -> threshold = max score among kept
      - descending -> threshold = min score among kept
    """
    thr = {}
    if len(selected) == 0:
        return thr
    if order_desc:
        # higher is better, worst kept is the min
        s = selected.groupby("__ipix__")["__score__"].min()
    else:
        # lower is better, worst kept is the max
        s = selected.groupby("__ipix__")["__score__"].max()
    for k, v in s.items():
        thr[int(k)] = float(v)
    return thr


# =============================================================================
# Remainder filter — propagate only what was NOT selected at current depth
# =============================================================================

def filter_remainder_partition(pdf: pd.DataFrame,
                               depth: int,
                               ra_col: str,
                               dec_col: str,
                               score_expr: str,
                               order_desc: bool,
                               thr_dict: Dict[int, float]) -> pd.DataFrame:
    """
    Given a partition, return only rows that should propagate to the next depth:
      - compute __ipix__ at 'depth' and __score__
      - keep rows that are strictly WORSE than the threshold of their pixel
        * ascending: score > threshold
        * descending: score < threshold
    Rows whose ipix has no threshold (i.e., no selection happened for that pixel)
    are passed through entirely.
    """
    if len(pdf) == 0:
        return pdf

    # Build score
    if score_expr in pdf.columns:
        sc = pd.to_numeric(pdf[score_expr], errors="coerce")
    else:
        code = compile(score_expr, "<score>", "eval")
        env = {col: pdf[col] for col in pdf.columns if col in pdf.columns}
        out = eval(code, {"__builtins__": {}}, env)
        sc = pd.to_numeric(out, errors="coerce")

    sc = sc.replace([np.inf, -np.inf], np.nan)
    if order_desc:
        sc = sc.fillna(-np.inf)
    else:
        sc = sc.fillna(np.inf)

    ipix = ipix_for_depth(pdf[ra_col].values, pdf[dec_col].values, depth)

    # Build mask: pass through if no threshold exists; otherwise apply strict inequality
    thr = np.vectorize(lambda p: thr_dict.get(int(p), None))(ipix)
    if order_desc:
        # Selected kept the highest scores; remainder is strictly lower than threshold
        mask = (thr == None) | (sc.values < np.array([t if t is not None else (np.inf if order_desc else -np.inf) for t in thr], dtype=np.float64))
    else:
        # Selected kept the lowest scores; remainder is strictly higher than threshold
        mask = (thr == None) | (sc.values > np.array([t if t is not None else (-np.inf if order_desc else np.inf) for t in thr], dtype=np.float64))

    return pdf.loc[mask]


# =============================================================================
# Pipeline
# =============================================================================

def run_pipeline(cfg: Config) -> None:
    out_dir = Path(cfg.output.out_dir)
    _mkdirs(out_dir)

    # ------------------ logging local ------------------
    t0 = time.time()
    log_lines: List[str] = []

    def _log(msg: str, always: bool = False):
        """
        Grava mensagem com timestamp em memória e imprime no stdout.
        - always=True: loga mesmo se print_info=False
        - always=False: só loga se print_info=True (para "infos do print")
        """
        if always or cfg.algorithm.print_info:
            line = f"{_ts()} | {msg}"
            print(line)
            log_lines.append(line)

    _log(f"START HiPS pipeline: cat_name={cfg.output.cat_name} out_dir={out_dir}", always=True)
    _log(f"Config -> lM={cfg.algorithm.level_limit} lC={cfg.algorithm.level_coverage} order_desc={cfg.algorithm.order_desc}", always=True)
    # ---------------------------------------------------

    # CDS-like validations
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

    # Input
    paths: List[str] = []
    for p in cfg.input.paths:
        paths.extend(glob.glob(p))
    assert len(paths) > 0, "No input files matched."

    _log(f"Matched {len(paths)} input files", always=True)
    _log("Some input files: " + ", ".join(paths[:3]) + (" ..." if len(paths) > 3 else ""), always=True)

    # Build DDF (optionally with __src__ if debug.origin=True).
    ddf, RA_NAME, DEC_NAME, keep_cols = _build_input_ddf(paths, cfg, _log)

    # --- Optional: stabilize partition sizes and keep data hot in memory ---
    # Aim for ~128–512MB per partition; adjust to your cluster.
    ddf = ddf.repartition(partition_size="256MB").persist()
    wait(ddf)

    # Column report
    nhh_dir = out_dir / "nhhtree"
    _mkdirs(nhh_dir)
    report = compute_column_report(ddf)
    _write_text(nhh_dir / "metadata.info", json.dumps(report, indent=2))

    # Densmaps for depths 0..lM (+ write densmap fits up to 12)
    depths = list(range(0, cfg.algorithm.level_limit + 1))
    densmaps: Dict[int, np.ndarray] = {}
    for d in depths:
        densmaps[d] = densmap_for_depth(ddf, RA_NAME, DEC_NAME, depth=d)
        write_densmap_fits(out_dir, d, densmaps[d])

    dens_lc = densmaps[cfg.algorithm.level_coverage]

    # Optional informational prints (like -p/--print_info)
    if cfg.algorithm.print_info:
        total_rows = int(densmaps[0].sum())
        _log(f"[INFO] Total input rows (depth 0) = {total_rows}")
        for d in depths[1:]:
            cnt = int(densmaps[d].sum())
            nonempty = int((densmaps[d] > 0).sum())
            _log(f"[INFO] Depth {d}: total rows={cnt}, non-empty pixels={nonempty}")

    # MOC
    write_moc(out_dir, cfg.algorithm.level_coverage, dens_lc)

    # metadata.xml / Metadata.xml
    cols = [(c, str(ddf[c].dtype), None) for c in ddf.columns]
    ra_idx = list(ddf.columns).index(RA_NAME)
    dec_idx = list(ddf.columns).index(DEC_NAME)
    write_metadata_xml(out_dir, cols, ra_idx, dec_idx)

    # properties
    n_src_total = int(densmaps[0].sum())
    write_properties(
        out_dir,
        cfg.output.cat_name,
        cfg.output.target,
        cfg.algorithm.level_limit,
        n_src_total,
        tile_format="tsv"
    )

    # arguments
    arg_text = textwrap.dedent(f"""
        # Input/output
        Input files: {paths}
        Input type: {cfg.input.format}
        Output dir: {out_dir}
        # Input data parameters
        Catalogue name: {cfg.output.cat_name}
        RA column name: {RA_NAME}
        DE column name: {DEC_NAME}
        # Selection parameters
        simple_algo: {cfg.algorithm.simple_algo}
        order_desc: {cfg.algorithm.order_desc}
        method: {cfg.algorithm.method}
        level_limit(lM): {cfg.algorithm.level_limit}
        level_coverage(lC): {cfg.algorithm.level_coverage}
        n1: {cfg.algorithm.n1}
        n2: {cfg.algorithm.n2}
        n3_min: {cfg.algorithm.n3_min}
        n3_max: {cfg.algorithm.n3_max}
        nm (>= d4 min): {cfg.algorithm.nm}
        nM (>= d4 max): {cfg.algorithm.nM}
        no (quota base): {cfg.algorithm.no}
        rl3l4 (l3 quota ratio): {cfg.algorithm.rl3l4}
        l1l2_only: {cfg.algorithm.l1l2_only}
        print_info: {cfg.algorithm.print_info}
        tie_buffer: {cfg.algorithm.tie_buffer}
    """).strip("\n")

    write_arguments(out_dir, arg_text + "\n")

    # =========================
    # DEBUG: hashes do input
    # =========================
    input_hash_set = None
    MAG_COL = "MAG_AUTO_I_DERED"  # ajuste se seu score usar outro
    if cfg.debug.containment_check and (MAG_COL in ddf.columns):
        _log("[DEBUG] Building input hash set for containment check...", always=True)
        input_hashes = ddf.map_partitions(
            lambda pdf: _canon_hash_pdf(pdf, RA_NAME, DEC_NAME, MAG_COL),
            meta=("h","object")
        ).dropna().drop_duplicates().compute()
        input_hash_set = set(input_hashes.tolist())
        _log(f"[DEBUG] Input unique hashes: {len(input_hash_set)}", always=True)

    # =========================
    # DEBUG: ranges por partição
    # =========================
    if cfg.debug.ranges_check:
        def _ranges(pdf):
            return pd.DataFrame({
                "RA_min":[pd.to_numeric(pdf[RA_NAME], errors='coerce').min()],
                "RA_max":[pd.to_numeric(pdf[RA_NAME], errors='coerce').max()],
                "DEC_min":[pd.to_numeric(pdf[DEC_NAME], errors='coerce').min()],
                "DEC_max":[pd.to_numeric(pdf[DEC_NAME], errors='coerce').max()],
                "n":[len(pdf)]
            })
        try:
            rng = ddf.map_partitions(_ranges).compute()
            # Exemplo de janela esperada; ajuste conforme seu recorte:
            off = rng[(rng["DEC_max"] > 20.0) | (rng["DEC_min"] < -80.0)]
            if len(off):
                _log(f"[DEBUG] Partitions outside expected DEC window:\n{off.sort_values('DEC_max', ascending=False).head(10)}", always=True)
        except Exception as e:
            _log(f"[DEBUG] ranges_check failed: {type(e).__name__}: {e}", always=True)

    # -------------------------------------------------------------------------
    # Progressive tiling loop (top-down) — 2-pass with global thresholds:
    #   1) produce candidates per partition (top q+buffer per ipix, stable keys)
    #   2) reduce globally per ipix (top q+buffer again)
    #   3) cut exactly q with deterministic tie-break and write once per Npix
    #   4) build thresholds from selected and filter remainder strictly worse
    # -------------------------------------------------------------------------
    
    # Precompute coverage maps from lC densmap
    coverage_by_depth = compute_coverage_pct_for_depths(
        cfg.algorithm.level_limit, cfg.algorithm.level_coverage, dens_lc
    )
    
    # Start with full input as remainder at depth 1
    remainder_ddf = ddf
    
    for depth in range(1, cfg.algorithm.level_limit + 1):
        depth_t0 = time.time()
        _log(f"[DEPTH {depth}] start")
        _log(f"Generating tiles for depth={depth}")
    
        # Densmap for the CURRENT remainder at this depth
        counts = densmap_for_depth(remainder_ddf, RA_NAME, DEC_NAME, depth=depth)
    
        # Compute per-pixel quotas with coverage shaping
        quotas = compute_quotas_for_depth(
            depth, counts, cfg.algorithm, coverage_pct=coverage_by_depth.get(depth)
        )
        keep_quotas_path = str(out_dir / f".keep_quotas_o{depth}.npy")
        np.save(keep_quotas_path, quotas)
    
        # ------------- Pass 1: produce candidates (no I/O) -------------
        # Build a meta DataFrame that matches the *full* output schema:
        # tile columns (same dtypes as ddf) + __ipix__ (int64) + __score__ (float64)
        meta_cols = {c: pd.Series(dtype=ddf[c].dtype) for c in ddf.columns}
        meta_cols["__ipix__"] = pd.Series(dtype="int64")
        meta_cols["__score__"] = pd.Series(dtype="float64")
        meta_cand = pd.DataFrame(meta_cols)
    
        candidates_ddf = remainder_ddf.map_partitions(
            produce_candidates_partition,
            depth,
            RA_NAME,
            DEC_NAME,
            keep_quotas_path,
            cfg.algorithm.order_desc,
            [*list(ddf.columns), cfg.columns.score],
            int(cfg.algorithm.tie_buffer),
            meta=meta_cand,
        )
    
        # ------------- Pass 2 (in-Dask): reduce to (q + buffer) per ipix -------------
        # Keep the same meta used in Pass 1 (tile columns + __ipix__ + __score__)
        def _reduce_group(pdf: pd.DataFrame,
                          quotas_path: str,
                          ra_col: str,
                          dec_col: str,
                          order_desc_flag: bool,
                          tie_buf: int) -> pd.DataFrame:
            if len(pdf) == 0:
                return pdf
            quotas = np.load(quotas_path, mmap_mode="r")
            sort_cols = ["__score__", ra_col, dec_col]
            ascending = [not order_desc_flag, True, True]
            out = []
            for pid, g in pdf.groupby("__ipix__"):
                p = int(pid)
                if p < 0 or p >= len(quotas):
                    continue
                q = int(quotas[p])
                if q <= 0 or len(g) == 0:
                    continue
                k_buf = q + int(tie_buf)
                gg = g.sort_values(sort_cols, ascending=ascending, kind="mergesort").head(k_buf)
                out.append(gg)
            if not out:
                return pdf.iloc[0:0]
            return pd.concat(out, ignore_index=True)

        # Repartition by '__ipix__' for co-location, then reduce inside each partition
        # This prevents the driver from seeing the full candidates set.
        candidates_reduced_ddf = (
            candidates_ddf
            .shuffle("__ipix__", npartitions=max(8, ddf.npartitions))
            .map_partitions(
                _reduce_group,
                keep_quotas_path,
                RA_NAME,
                DEC_NAME,
                cfg.algorithm.order_desc,
                int(cfg.algorithm.tie_buffer),
                meta=meta_cand,  # same schema as Pass 1's meta
            )
        )

        # Bring the already-reduced set to pandas (bounded by q+buffer per ipix)
        reduced_pdf = candidates_reduced_ddf.compute()
    
        # ------------- Final selection: cut exactly q with stable tie-break -------------
        # Sort per ipix with stable key; keep exactly quota q (or <= count)
        sort_cols = ["__score__", RA_NAME, DEC_NAME]
        ascending = [not cfg.algorithm.order_desc, True, True]
        selected_parts = []
        for pid, g in reduced_pdf.groupby("__ipix__"):
            q = int(quotas[int(pid)]) if 0 <= int(pid) < len(quotas) else 0
            if q <= 0 or len(g) == 0:
                continue
            gg = g.sort_values(sort_cols, ascending=ascending, kind="mergesort").head(q)
            selected_parts.append(gg)
        selected_pdf = pd.concat(selected_parts, ignore_index=True) if selected_parts else reduced_pdf.iloc[0:0]

        # =========================
        # DEBUG: containment check
        # =========================
        if cfg.debug.containment_check and (input_hash_set is not None) and (MAG_COL in selected_pdf.columns):
            try:
                sel_h = _canon_hash_pdf(selected_pdf, RA_NAME, DEC_NAME, MAG_COL)
                bad_mask = ~sel_h.isin(list(input_hash_set))
                if bool(bad_mask.any()):
                    bad = selected_pdf.loc[bad_mask, [RA_NAME, DEC_NAME, MAG_COL, "__ipix__"] + ([ "__src__"] if "__src__" in selected_pdf.columns else [])]
                    _log(f"[BUG?] {len(bad)} selected rows are NOT in input. Sample:\n{bad.head(5)}", always=True)
            except Exception as e:
                _log(f"[DEBUG] containment_check failed at depth {depth}: {type(e).__name__}: {e}", always=True)

    
        # ------------- Write once per Npix (atomic) + optional Allsky -------------
        header_line = "\t".join([str(c) for c in ddf.columns]) + "\n"
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
            stage_dir=Path(os.environ.get("SLURM_TMPDIR", "/tmp")),
            allsky_collect=allsky_needed,
        )
    
        # ------------- Allsky.tsv from selected (only L1/L2) -------------
        if allsky_needed and allsky_df is not None and len(allsky_df) > 0:
            norder_dir = out_dir / f"Norder{depth}"
            norder_dir.mkdir(parents=True, exist_ok=True)

            tmp_allsky = norder_dir / ".Allsky.tsv.tmp"
            final_allsky = norder_dir / "Allsky.tsv"

            # Totals for completeness header
            nsrc_tot = int(counts.sum())
            nwritten_tot = int(sum(written_per_ipix.values())) if written_per_ipix else 0
            nremaining_tot = max(0, nsrc_tot - nwritten_tot)
            completeness_header_allsky = f"# Completeness = {nremaining_tot} / {nsrc_tot}\n"

            # Derive the exact header order from the already-written header_line.
            # This guarantees that data columns match the header 1:1.
            header_cols = header_line.strip("\n").split("\t")
            allsky_cols = [c for c in header_cols if c in allsky_df.columns]

            # Keep the Allsky dataframe strictly in the same column order as the header.
            df_as = allsky_df[allsky_cols].copy()

            # 1) Write completeness + header once (text mode)
            with tmp_allsky.open("w", encoding="utf-8", newline="") as f:
                f.write(completeness_header_allsky)
                f.write(header_line)

            # Sanitize string columns to avoid embedded tabs/newlines breaking TSV rows
            obj_cols = df_as.select_dtypes(include=["object", "string"]).columns
            if len(obj_cols) > 0:
                df_as[obj_cols] = df_as[obj_cols].replace({r"[\t\r\n]": " "}, regex=True)

            # 2) Append rows using pandas.to_csv for robust serialization.
            #    - Prevents truncated/partial first data line.
            #    - Uses '\t' separator and '\n' line terminator.
            df_as.to_csv(
                tmp_allsky,
                sep="\t",
                index=False,
                header=False,
                mode="a",
                encoding="utf-8",
                lineterminator="\n",
                float_format=None  # keep pandas' default precision (no forced rounding)
            )

            # 3) Atomic replace to finalize the file
            os.replace(tmp_allsky, final_allsky)
    
        # ------------- Early stop if requested -------------
        if cfg.algorithm.l1l2_only and depth == 2:
            _log("l1l2_only=True → stopping após Norder2.")
            break
    
        # ------------- Build thresholds (from final selected) and filter remainder -------------
        thresholds = build_thresholds(selected_pdf, cfg.algorithm.order_desc)
        if len(thresholds) == 0:
            if cfg.algorithm.print_info:
                _log(f"[INFO] Depth {depth}: no selection happened, stopping.")
            break
    
        remainder_ddf = remainder_ddf.map_partitions(
            filter_remainder_partition,
            depth,
            RA_NAME,
            DEC_NAME,
            cfg.columns.score,
            cfg.algorithm.order_desc,
            thresholds,
            meta=remainder_ddf._meta,
        )
    
        if cfg.algorithm.print_info:
            wrote_total = int(sum(written_per_ipix.values())) if written_per_ipix else 0
            _log(f"[INFO] Depth {depth} finalized: wrote={wrote_total}, available(remainder)={int(counts.sum())}")
    
        depth_elapsed = time.time() - depth_t0
        _log(f"[DEPTH {depth}] done in {_fmt_dur(depth_elapsed)}")

    # graceful shutdown to avoid CommClosedError noise
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
    _log(f"END HiPS pipeline. Elapsed {_fmt_dur(elapsed)} ({elapsed:.3f} s)", always=True)
    
    # Persistir log no arquivo (append para manter histórico de execuções)
    try:
        with (out_dir / "process.log").open("a", encoding="utf-8") as f:
            f.write("\n".join(log_lines) + "\n")
    except Exception as e:
        # Se algo falhar ao gravar o log, ao menos notificar no stdout
        _log(f"{_ts()} | ERROR writing process.log: {type(e).__name__}: {e}")

    _log("Done.")


# =============================================================================
# CLI
# =============================================================================

def load_config(path: str) -> Config:
    with open(path, "r", encoding="utf-8") as f:
        y = yaml.safe_load(f)
    cfg = Config(
        input=InputCfg(
            paths=y["input"]["paths"],
            format=y["input"].get("format", "parquet"),
            header=y["input"].get("header", True),
            ascii_format=y["input"].get("ascii_format"),  # optional; CSV/TSV only
        ),
        columns=ColumnsCfg(
            ra=y["columns"]["ra"],
            dec=y["columns"]["dec"],
            score=y["columns"]["score"],
            keep=y["columns"].get("keep"),
        ),
        algorithm=AlgoOpts(
            simple_algo=y["algorithm"].get("simple_algo", False),
            order_desc=y["algorithm"].get("order_desc", False),
            method=y["algorithm"].get("method", "LOG"),
            level_limit=int(y["algorithm"]["level_limit"]),
            level_coverage=int(y["algorithm"]["level_coverage"]),
            n1=int(y["algorithm"].get("n1", 3000)),
            n2=int(y["algorithm"].get("n2", 6000)),
            n3_min=int(y["algorithm"].get("n3_min", 3)),
            n3_max=int(y["algorithm"].get("n3_max", 30000)),
            no=int(y["algorithm"].get("no", 500)),
            nm=int(y["algorithm"].get("nm", 50)),
            nM=int(y["algorithm"].get("nM", 500)),
            rl3l4=float(y["algorithm"].get("rl3l4", 0.2)),
            l1l2_only=bool(y["algorithm"].get("l1l2_only", False)),
            print_info=bool(y["algorithm"].get("print_info", False)),
            tie_buffer=int(y["algorithm"].get("tie_buffer", 16)),
        ),
        cluster=ClusterCfg(
            mode=y["cluster"].get("mode", "local"),
            n_workers=int(y["cluster"].get("n_workers", 4)),
            threads_per_worker=int(y["cluster"].get("threads_per_worker", 1)),
            memory_per_worker=str(y["cluster"].get("memory_per_worker", "4GB")),
            slurm=y["cluster"].get("slurm"),
        ),
        output=OutputCfg(
            out_dir=y["output"]["out_dir"],
            cat_name=y["output"]["cat_name"],
            target=y["output"].get("target", "0 0"),
        ),
        debug=DebugCfg(
            origin=bool(y.get("debug", {}).get("origin", False)),
            ranges_check=bool(y.get("debug", {}).get("ranges_check", False)),
            containment_check=bool(y.get("debug", {}).get("containment_check", False)),
        ),
    )

    # Sanity check: align lC if user set it above lM
    if cfg.algorithm.level_coverage > cfg.algorithm.level_limit:
        cfg.algorithm.level_coverage = cfg.algorithm.level_limit
    return cfg
    

def main(argv: List[str]):
    import argparse
    ap = argparse.ArgumentParser(description="HiPS Catalog Pipeline (Dask, Parquet, CDS-faithful + methods)")
    ap.add_argument("--config", required=True, help="YAML configuration file")
    args = ap.parse_args(argv)
    cfg = load_config(args.config)
    run_pipeline(cfg)


if __name__ == "__main__":
    main(sys.argv[1:])

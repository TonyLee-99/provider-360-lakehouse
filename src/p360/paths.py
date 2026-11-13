"""Where things live. Override the data root with P360_DATA_DIR (the offline demo and tests do)."""

import os
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
DATA = Path(os.environ.get("P360_DATA_DIR", REPO / "data"))
RAW = DATA / "raw"  # immutable downloads, exactly as published
LANDING = DATA / "landing"  # same rows as raw, as all-string Parquet (no decompressed CSV on disk)
LAKE = DATA / "lake"  # Delta tables: bronze / silver / gold
RESULTS = Path(os.environ.get("P360_RESULTS_DIR", REPO / "results"))
CONTRACTS = REPO / "contracts"
SOURCES = REPO / "sources.yaml"

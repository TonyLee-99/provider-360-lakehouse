"""Landing: raw file (zip member or CSV) -> all-string Parquet, plus a landing manifest.

Why all strings: landing must never lose or reinterpret a value (leading zeros in ZIP codes and NPIs, odd
dates). Typing happens later, in Spark, where a bad value goes to quarantine with a reason instead of
silently becoming NULL here.
Why stream from the zip: the NPPES CSV is 11.8 GB uncompressed; reading it in 64 MB blocks straight from the
archive keeps disk and memory flat.
"""

import fnmatch
import json
import time
import zipfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from p360 import contracts
from p360.paths import LANDING, RAW


@dataclass(frozen=True)
class Dataset:
    name: str  # landing/bronze table name
    source: str  # key in sources.yaml (folder under data/raw)
    contract: str
    member: str | None = None  # glob of the CSV inside a zip; None for plain CSV downloads
    publication: str = ""  # which release of the source this is


DATASETS = [
    Dataset(
        "op_payment_p2026_01",
        "op_general_2024_p2026_01",
        "op_general_payment",
        "OP_DTL_GNRL_*.csv",
        "2026-01",
    ),
    Dataset(
        "op_payment_p2026_06",
        "op_general_2024_p2026_06",
        "op_general_payment",
        "OP_DTL_GNRL_*.csv",
        "2026-06",
    ),
    Dataset(
        "op_removed_p2026_06",
        "op_general_2024_p2026_06",
        "op_removed_deleted",
        "OP_REMOVED_DELETED_*.csv",
        "2026-06",
    ),
    Dataset(
        "op_profile_supplement",
        "op_profile_supplement_p2026_06",
        "op_profile_supplement",
        publication="2026-06",
    ),
    Dataset(
        "op_recipient_nature_summary",
        "op_recipient_nature_2024_p2026_06",
        "op_recipient_nature_summary",
        publication="2026-06",
    ),
    Dataset(
        "partd_prescriber_drug", "partd_prescriber_drug_2024", "partd_prescriber_drug", publication="DY2024"
    ),
    Dataset("partd_prescriber", "partd_prescriber_2024", "partd_prescriber", publication="DY2024"),
    Dataset("nppes_full", "nppes_full_2026_11", "nppes_provider", "npidata_pfile_*[0-9].csv", "2026-11-02"),
    Dataset(
        "nppes_weekly_2026_11_08",
        "nppes_weekly_2026_11_02",
        "nppes_provider",
        "npidata_pfile_*[0-9].csv",
        "2026-11-08",
    ),
]
BY_NAME = {d.name: d for d in DATASETS}


def _raw_file(ds: Dataset, raw_dir: Path) -> Path:
    files = [p for p in (raw_dir / ds.source).iterdir() if not p.name.endswith(".part")]
    if len(files) != 1:
        raise FileNotFoundError(f"expected exactly one raw file in {raw_dir / ds.source}, found {files}")
    return files[0]


@contextmanager
def open_source(ds: Dataset, raw_dir: Path = RAW):
    """Yield (binary stream, member name) for the dataset's CSV, whether zipped or plain."""
    path = _raw_file(ds, raw_dir)
    if ds.member is None:
        with path.open("rb") as f:
            yield f, path.name
        return
    with zipfile.ZipFile(path) as z:
        names = [n for n in z.namelist() if fnmatch.fnmatch(n, ds.member)]
        if len(names) != 1:
            raise FileNotFoundError(f"{path.name}: expected one member matching {ds.member}, got {names}")
        with z.open(names[0]) as f:
            yield f, names[0]


def read_header(ds: Dataset, raw_dir: Path = RAW) -> list[str]:
    with open_source(ds, raw_dir) as (f, _):
        reader = pacsv.open_csv(f, read_options=pacsv.ReadOptions(block_size=1 << 20))
        return reader.schema.names


def land(ds: Dataset, raw_dir: Path = RAW, landing_dir: Path = LANDING) -> dict:
    contract = contracts.load(ds.contract)
    header = read_header(ds, raw_dir)
    drift = contracts.check_header(contract, header)  # raises on breaking drift
    keep = header if contract.keep_all_source_columns else contract.source_names
    out_dir = landing_dir / ds.name
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "data.parquet"
    tmp = out.with_suffix(".parquet.tmp")
    t0 = time.time()
    rows = 0
    with open_source(ds, raw_dir) as (f, member):
        reader = pacsv.open_csv(
            f,
            read_options=pacsv.ReadOptions(block_size=64 << 20),
            # some free-text fields contain quoted newlines
            parse_options=pacsv.ParseOptions(newlines_in_values=True),
            convert_options=pacsv.ConvertOptions(
                include_columns=keep,
                column_types={c: pa.string() for c in keep},
                strings_can_be_null=False,  # keep "" as "", the source has no NULL marker
                quoted_strings_can_be_null=False,
            ),
        )
        schema = pa.schema([(c, pa.string()) for c in keep])
        with pq.ParquetWriter(tmp, schema, compression="zstd") as w:
            for batch in reader:
                w.write_batch(batch.select(keep))
                rows += batch.num_rows
    tmp.rename(out)  # a crash leaves only the .tmp file, never a half-written data.parquet
    report = {
        "dataset": ds.name,
        "source": ds.source,
        "member": member,
        "publication": ds.publication,
        "rows": rows,
        "columns_kept": len(keep),
        "columns_in_source": len(header),
        "header_drift": drift,
        "parquet_bytes": out.stat().st_size,
        "seconds": round(time.time() - t0, 1),
    }
    (out_dir / "_landing.json").write_text(json.dumps(report, indent=2))
    return report


def land_all(names: list[str] | None = None, force: bool = False) -> list[dict]:
    reports = []
    for ds in DATASETS:
        if names and ds.name not in names:
            continue
        marker = LANDING / ds.name / "_landing.json"
        if marker.exists() and not force:
            reports.append(json.loads(marker.read_text()))
            continue
        r = land(ds)
        print(f"landed {ds.name}: {r['rows']:,} rows in {r['seconds']}s")
        reports.append(r)
    return reports

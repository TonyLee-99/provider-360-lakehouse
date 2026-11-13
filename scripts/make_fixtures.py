"""Cut a small, REAL fixture from the landed full data: every source restricted to providers practising in two
Chicago ZIP codes (60637 Hyde Park, 60615 Kenwood), written back in the original raw layout (same zip member
names, same headers) so the offline demo and CI run the whole pipeline from landing onwards.

NPPES fixture files keep only the 52 contract columns (the full file has 330). The CMS dashboard file is copied
as-is; the R1 check is skipped at fixture scope because those totals are for the whole country.

Usage: uv run python scripts/make_fixtures.py   (needs data/landing from a full run)
"""

import shutil
import zipfile
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from p360.download import load_sources, raw_path
from p360.landing import BY_NAME
from p360.paths import LANDING, REPO

OUT = REPO / "fixtures" / "raw"
ZIPS = ("60637", "60615")


def scan(dataset: str, keep) -> pa.Table:
    pf = pq.ParquetFile(LANDING / dataset / "data.parquet")
    parts = [b.filter(keep(b)) for b in pf.iter_batches(batch_size=500_000)]
    return pa.Table.from_batches([p for p in parts if p.num_rows], schema=pf.schema_arrow)


def starts(col, prefixes) -> pa.Array:
    m = None
    for p in prefixes:
        x = pc.starts_with(col, p)
        m = x if m is None else pc.or_(m, x)
    return m


def write(dataset: str, table: pa.Table) -> None:
    ds = BY_NAME[dataset]
    spec = load_sources()[ds.source]
    raw = raw_path(ds.source, spec["url"])
    dest = OUT / ds.source / raw.name
    dest.parent.mkdir(parents=True, exist_ok=True)
    if ds.member is None:
        pacsv.write_csv(table, dest)
    else:
        with zipfile.ZipFile(raw) as z:
            member = [n for n in z.namelist() if Path(n).match(ds.member)][0]
        tmp = dest.with_suffix(".csv.tmp")
        pacsv.write_csv(table, tmp)
        mode = "a" if dest.exists() else "w"
        with zipfile.ZipFile(dest, mode, compression=zipfile.ZIP_DEFLATED) as z:
            z.write(tmp, member)
        tmp.unlink()
    print(f"{dataset}: {table.num_rows:,} rows -> {dest.relative_to(REPO)}")


def main() -> None:
    shutil.rmtree(OUT, ignore_errors=True)
    zip_col = "Provider Business Practice Location Address Postal Code"
    nppes = scan("nppes_full", lambda b: starts(b.column(zip_col), ZIPS))
    npis = pa.array(set(nppes.column("NPI").to_pylist()))
    write("nppes_full", nppes)
    write(
        "nppes_weekly_2026_11_08",
        scan(
            "nppes_weekly_2026_11_08",
            lambda b: pc.or_(pc.is_in(b.column("NPI"), npis), starts(b.column(zip_col), ZIPS)),
        ),
    )

    def op_keep(b):
        return pc.or_(
            pc.is_in(b.column("Covered_Recipient_NPI"), npis), starts(b.column("Recipient_Zip_Code"), ZIPS)
        )

    jan = scan("op_payment_p2026_01", op_keep)
    jun = scan("op_payment_p2026_06", op_keep)
    write("op_payment_p2026_01", jan)
    write("op_payment_p2026_06", jun)  # appended to the same zip as the removed list below
    ids = pa.array(set(jan.column("Record_ID").to_pylist()) | set(jun.column("Record_ID").to_pylist()))
    write("op_removed_p2026_06", scan("op_removed_p2026_06", lambda b: pc.is_in(b.column("Record_ID"), ids)))
    profiles = pa.array(
        {p for t in (jan, jun) for p in t.column("Covered_Recipient_Profile_ID").to_pylist() if p}
    )
    hospitals = pa.array({p for t in (jan, jun) for p in t.column("Teaching_Hospital_ID").to_pylist() if p})
    write(
        "op_profile_supplement",
        scan("op_profile_supplement", lambda b: pc.is_in(b.column("Covered_Recipient_Profile_ID"), profiles)),
    )
    recip = pa.concat_arrays([profiles, hospitals])
    write(
        "op_recipient_nature_summary",
        scan("op_recipient_nature_summary", lambda b: pc.is_in(b.column("Recipient_ID"), recip)),
    )
    write(
        "partd_prescriber_drug",
        scan("partd_prescriber_drug", lambda b: pc.is_in(b.column("Prscrbr_NPI"), npis)),
    )
    write("partd_prescriber", scan("partd_prescriber", lambda b: pc.is_in(b.column("Prscrbr_NPI"), npis)))
    spec = load_sources()["op_summary_dashboard_p2026_06"]
    src = raw_path("op_summary_dashboard_p2026_06", spec["url"])
    (OUT / "op_summary_dashboard_p2026_06").mkdir(parents=True, exist_ok=True)
    shutil.copy(src, OUT / "op_summary_dashboard_p2026_06" / src.name)


if __name__ == "__main__":
    main()

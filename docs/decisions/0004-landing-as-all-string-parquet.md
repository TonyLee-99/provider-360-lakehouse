# 0004 Land as all-string Parquet streamed from the zip; one bronze table per file

**Context.** The NPPES file is 11.8 GB uncompressed with 330 columns; Open Payments is 9 GB per
publication. Disk on the build laptop was ~25 GB free.

**Decision.** Stream each CSV straight out of its zip in 64 MB blocks with pyarrow, write zstd Parquet,
every column as a string ("" kept as ""). Typing happens in Spark against the contract, where a bad
value is quarantined with a reason. For NPPES only the 52 contract columns are landed; for Open Payments
all 91 columns are kept, because change detection must see every published field. Each landed file
becomes its own bronze Delta table (a single partitioned table made Spark sort every row before writing;
docs/INCIDENTS.md #1).

**Rejected.** Unzip to disk and let Spark read CSV: +21 GB of temporary disk, and Spark's CSV reader
with `multiLine` (needed for quoted newlines) cannot split files.

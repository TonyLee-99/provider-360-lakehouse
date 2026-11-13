# Incidents during the build

Real problems hit while building and running the pipeline on the full data. Each entry: symptom, root
cause, fix, prevention.

## 1. Partitioned bronze write spilled ~15 GB and stalled
- **Symptom:** loading the first Open Payments publication (15.4M rows, 91 columns) into a bronze table
  partitioned by `_publication` ran for 8 minutes with 0 bytes written.
- **Root cause:** a thread dump showed every task inside `UnsafeExternalRowSorter.sort`: Spark's planned
  write sorts all rows by the partition column before writing, even though every row had the same value.
  For wide rows that is a full external sort of the whole publication.
- **Fix:** one bronze table per landed file (each publication is an immutable snapshot anyway); no
  partitioned write. Bronze for both publications then loads in the time recorded in `results/run_*.json`.
- **Prevention:** partition only on columns with many values that queries filter on; check `explain()` for
  a `Sort` before large writes.

## 2. Row hash was the bottleneck of the bronze load
- **Symptom:** after fix #1, bronze for one Open Payments publication still took > 15 minutes; tasks ran at
  about 6,500 rows/s per core.
- **Root cause:** a second thread dump showed the time in `UTF8String.concat`: the null-safe hash wrapped each
  of the 91 columns in `coalesce(concat('v:', col), '<null>')` before `concat_ws`, i.e. ~180 string copies per row.
- **Fix:** landing never produces NULLs (every value is a string, "" is kept as ""), so bronze hashes a single
  `concat_ws` of the raw columns. The null-safe version is kept for typed gold tables with few columns.
- **Prevention:** profile with `jstack` before tuning Spark settings; the expensive part is often one expression.

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

## 3. Delta rejected NPPES column names
- **Symptom:** the bronze load of NPPES failed with `DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES` on
  `Entity Type Code`. NPPES headers contain spaces and parentheses.
- **Fix:** bronze tables whose headers contain such characters are created with Delta column mapping
  (`delta.columnMapping.mode = name`), so bronze keeps the published names exactly; silver renames them via
  the contract. Column mapping cannot be combined with a change data feed on overwrite, so bronze tables
  (replaced per file, never read incrementally) have the change feed off.
- **Prevention:** contracts own the mapping from published names to our names; nothing downstream of bronze
  uses a published name.

## 4. Payment dates in year 0002
- **Symptom:** the first full silver MERGE failed with `WRITE_ANCIENT_DATETIME`: Spark refuses by default to
  write dates before 1582 to Parquet.
- **Root cause:** 73 records in the January publication and 75 in June have a `Date_of_Payment` in year 0002
  (program year 2024). These are typos made by the reporting companies, and CMS publishes them as they are.
- **Fix:** write dates as they are (`datetimeRebaseModeInWrite = CORRECTED`). The values are kept, because CMS
  counts these records in its totals and dropping them would break the reconciliation. They are counted in
  `results/identity.json` (`payment_date_outside_program_year`).
- **Prevention:** the fixture test only exercises dates that already parse; this one surfaced only at full
  scale, which is why the full run is part of the acceptance criteria.

## 5. Per-recipient referee collided physician and hospital ids
- **Symptom:** the first per-recipient reconciliation (R2) matched 99.74% of recipients, but every
  mismatch involved a teaching hospital, with differences of tens of millions of dollars (for example, one
  id carried $100 on our side and $188.7M on CMS's side).
- **Root cause:** CMS's grouped file identifies teaching hospitals by their CCN, not by
  `Teaching_Hospital_ID`, and physician profile ids and CCNs share one number space. We had keyed hospitals by
  `Teaching_Hospital_ID` and joined on the id alone, so a hospital could be compared with an unrelated
  physician. The same collision made 208 rows of the CMS file look like duplicate keys.
- **Fix:** the key is now (recipient type, id), with hospitals keyed by `Teaching_Hospital_CCN` (added to the
  contract) and recipient type added to the summary file's primary key. The new numbers are in
  `results/r2_recipient_totals.json`.
- **Prevention:** a referee mismatch is investigated by class before being reported. "99.7% match" was
  hiding a key bug.

## 6. Name + state matching was not precise enough
- **Symptom:** in the provider-disjoint holdout, the identity ladder reached 98.9% precision, below the 99% bar.
- **Root cause:** per-rung numbers on the dev split: name + ZIP5 had 99.66% precision, and name + state had
  97.17% (common names repeat within a state).
- **Fix:** the name + state rung is switched off (`identity.USE_NAME_STATE = False`). The choice was made on the
  dev split; the test split is only reported. The evidence is kept in `results/identity.json`
  (`rung_selection`).
- **Also fixed:** deactivated NPIs were not counted as "known". Their registry rows carry no name or entity type,
  so they fell through to name matching and ended up unresolved. Every registry NPI now counts as known, and
  `provider_360` shows these providers as deactivated.

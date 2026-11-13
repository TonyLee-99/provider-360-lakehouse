# 0001 Delta Lake tables, and one MERGE per republication

**Context.** CMS republishes the whole Open Payments program year every ~6 months. Between two
publications, records are added, corrected, disputed or withdrawn. Downstream tables need "what changed"
without rescanning everything, and a bad file must not silently delete half the table.

**Decision.** Store every layer as Delta tables. Apply each publication with a single MERGE keyed on
`record_id`: update when the content hash differs, insert when new, soft-delete (`is_deleted`) when absent
(`WHEN NOT MATCHED BY SOURCE`, scoped to the program year). Turn on the change data feed so gold can read
only changed rows.

**Guards.** Publications are applied in order (an older one is refused); a publication with < 90% of the
live row count is refused (a truncated download must not delete); rows that fail the contract keep their
previous version instead of being deleted.

**Rejected.** (a) Overwrite silver with the newest file: simplest, but loses "what changed", which the
gold incremental aggregate and the restatement report both need. (b) Use CMS's own `Change_Type` labels to
drive the MERGE: the labels describe publication status, not our content; we compare against them instead
(results/restatement.json). (c) Iceberg: equivalent capabilities; Delta chosen because it is the default on
Databricks and the other project in this portfolio already uses Iceberg.

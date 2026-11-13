# 0002 Dagster assets with blocking checks, publish by Delta version

**Context.** The pipeline is a graph of tables with quality rules between them. A failed rule must stop
the tables that depend on it, and consumers must never read a half-validated gold table.

**Decision.** One Dagster asset per table group, one asset check per rule. Rules that protect consumers
are `blocking=True` (contract quarantine budget, primary keys, CMS totals, incremental == full, PII policy).
The last asset, `published`, runs only if they passed and writes `gold/_published.json` with the Delta
version of each gold table; readers use `versionAsOf` those versions (write-audit-publish via time travel).
All assets run in one process because they share one local Spark session.

**Rejected.** Airflow: task-centric; the same checks would be separate tasks with no notion of which table
they guard. Airflow is used in the sibling `chicago-311-warehouse` project. dbt tests: the transforms here
are PySpark (MERGE, CDF), not SQL models.

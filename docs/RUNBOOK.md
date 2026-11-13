# Runbook

## Commands
| Goal | Command | Notes |
|---|---|---|
| Fresh clone | `make init` | needs JDK 17 (`JAVA_HOME`) and `uv` |
| Offline demo on the real fixture | `make demo` | two Dagster runs (January, then June publication), ~3 min |
| Look at the asset graph / check history | `make ui` | Dagster UI on the demo data, http://localhost:3000 |
| Full data, end to end | `make reproduce` | downloads ~8 GB, ~20 GB disk at peak |
| Re-run only the pipeline | `make reset run experiments report` | raw + landing are kept |

## Schedules this pipeline is built for
- **Open Payments**: CMS republishes every program year around January and June. Add the new publication
  to `sources.yaml` and `landing.DATASETS`, `make download land`, then run with
  `p360 run --publications <all applied ones> <new one>`. Already-applied publications are skipped.
- **NPPES**: monthly full file, weekly incremental files. Weekly files are upserts only; a provider that is
  absent from a weekly file is NOT deleted. A new monthly full file is a rebase: apply it, then any weekly
  files dated after it.
- **Part D**: once a year; silver and gold are rebuilt from the new file.

## What each check failure means
| Check | Blocking | Meaning | What to do |
|---|---|---|---|
| `op_contract_quarantine_budget` | yes | > 0.1% of a publication failed its contract | `silver.quarantine` has the reasons per record; if CMS changed a format, update the contract (new version) |
| `op_primary_key_unique` | yes | duplicate `record_id` in silver | should be impossible (duplicates are quarantined); investigate the MERGE |
| `op_matches_cms_totals` | yes | live count or USD total differs from CMS's dashboard | compare quarantine + soft deletes with the dashboard; do not publish |
| `nppes_primary_key_unique` | yes | duplicate NPI | as above |
| `agg_incremental_equals_full` | yes | the change-feed aggregate drifted from a full recompute | delete `gold/agg_provider_payments` and its checkpoint in `gold/_cdf_checkpoints.json`; next run rebuilds |
| `partd_matches_provider_file` | no (warn) | by-drug claims exceed CMS's by-provider totals for some NPI | read `results/r3_partd_by_provider.json` |
| `public_has_no_direct_pii` | yes | a new gold column has no declared lineage, or carries direct PII | declare it in `governance.PROVIDER_360_LINEAGE` |

## Refusals (raised before anything is written)
- `PublicationRefused: ... older than the last applied publication`: publications must be applied in order.
- `PublicationRefused: ... refusing to soft-delete`: the new file has < 90% of the live rows; check the
  download (size and sha256 in `data/raw/_manifest.jsonl`) before forcing anything.
- `ContractViolation: source is missing contract columns`: CMS renamed or dropped a column.

## Recovery
- A crash anywhere is safe to re-run: bronze reloads are skipped when unchanged, silver MERGEs are no-ops
  for already-applied publications, the gold aggregate re-reads its unconsumed changes.
- Consumers read the versions in `gold/_published.json`; a bad build that never passed its checks is never
  referenced there. To roll back, point the manifest at the previous versions (Delta time travel).

# Column lineage of gold.provider_360

Generated from `p360/governance.py` by `p360 report`. Columns whose sources are direct PII are excluded from `gold.provider_360_public`.

| provider_360 column | sources (contract.column) | PII | in public table |
|---|---|---|---|
| npi | nppes_provider.npi, partd_prescriber.npi | quasi | yes |
| entity_type | nppes_provider.entity_type | - | yes |
| first_name | nppes_provider.first_name | direct | no |
| last_name | nppes_provider.last_name | direct | no |
| org_name | nppes_provider.org_name | - | yes |
| credential | nppes_provider.credential | - | yes |
| primary_taxonomy | nppes_provider.taxonomy_1, nppes_provider.taxonomy_2, nppes_provider.taxonomy_3 ... | - | yes |
| practice_city | nppes_provider.practice_city | - | yes |
| practice_state | nppes_provider.practice_state | - | yes |
| practice_zip5 | nppes_provider.practice_zip | quasi | yes |
| practice_address | nppes_provider.practice_address | direct | no |
| enumeration_date | nppes_provider.enumeration_date | - | yes |
| last_update_date | nppes_provider.last_update_date | - | yes |
| deactivation_date | nppes_provider.deactivation_date | - | yes |
| reactivation_date | nppes_provider.reactivation_date | - | yes |
| registry_publication | derived | - | yes |
| payment_records | op_general_payment.record_id | - | yes |
| payment_usd | op_general_payment.amount_usd | - | yes |
| paying_companies | op_general_payment.manufacturer_id | - | yes |
| first_payment_date | op_general_payment.payment_date | - | yes |
| last_payment_date | op_general_payment.payment_date | - | yes |
| partd_total_claims | partd_prescriber.total_claims | - | yes |
| partd_total_drug_cost | partd_prescriber.total_drug_cost | - | yes |
| partd_prescriber_type | partd_prescriber.prescriber_type | - | yes |
| partd_drug_row_claims | partd_prescriber_drug.total_claims | - | yes |
| partd_brands_observed | partd_prescriber_drug.brand_name | - | yes |
| partd_state | partd_prescriber_drug.state | - | yes |
| partd_last_name | partd_prescriber_drug.last_name | direct | no |
| in_registry | derived | - | yes |
| registry_status | derived | - | yes |
| has_payments | derived | - | yes |
| has_partd | derived | - | yes |
| conflict_state_partd | derived | - | yes |
| conflict_name_partd | derived | - | yes |
| provenance | derived | - | yes |

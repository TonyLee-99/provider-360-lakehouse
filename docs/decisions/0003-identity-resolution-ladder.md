# 0003 Deterministic identity ladder with abstention, evaluated provider-disjoint

**Context.** Payments must be attached to a provider NPI to join with Medicare Part D and the registry.
The NPI on a payment is typed by the paying company and is sometimes missing or wrong.

**Decision.** A ladder of deterministic rules (record NPI with valid check digit and present in NPPES ->
CMS profile mapping -> unique name + ZIP5 -> unique name + state), stopping at the first rung with exactly
one candidate; more than one candidate is an abstention, never a guess. Rungs 3-4 are evaluated on
recipients whose NPI is known, with the NPI hidden, split by Profile_ID so the same provider is never in
both dev and test; we report precision, recall, abstention and Wilson 95% intervals on test.

**Rejected.** Probabilistic matching (Splink/Fellegi-Sunter): more recall, but a reviewer must trust a
model; for a payments-to-provider link, a wrong match is worse than no match. It is the next step if
recall on the unresolved remainder matters.

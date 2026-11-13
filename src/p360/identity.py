"""Identity resolution: which NPI (national provider id) is the recipient of each Open Payments record?

Open Payments identifies recipients by its own Profile_ID and an NPI that the paying company typed in.
That NPI is sometimes missing or wrong. The ladder below tries the strongest evidence first and stops at the
first rung that gives exactly one answer; two or more candidates is an abstention ("ambiguous"), never a guess.

  1. record_npi   NPI on the record has a valid check digit and exists in NPPES
  2. profile_npi  CMS's own profile supplement maps the Profile_ID to a valid NPPES NPI
  3. name_zip     exactly one NPPES individual with the same first + last name and 5-digit ZIP
  4. name_state   exactly one NPPES individual with the same first + last name in the same state
                  (evaluated, then switched OFF: its precision on the dev split was 97.2%, below the 99% bar;
                  see USE_NAME_STATE and results/identity.json "rung_selection")
  -  ambiguous / unresolved

Rungs 3-4 are evaluated on a provider-disjoint holdout in evaluate_matcher(): recipients whose true NPI is
known (rung 1), split by Profile_ID so the same provider never appears in both halves, NPI hidden.
"""

import math

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from p360 import lake

USE_NAME_STATE = False  # chosen on the dev split only; the test split is reported, never used to choose
PERSON_TYPES = ["Covered Recipient Physician", "Covered Recipient Non-Physician Practitioner"]


def _luhn_sum(v: Column) -> Column:
    """NPI check digit sum: Luhn over '80840' + first 9 digits (the prefix contributes 24)."""
    total = F.lit(24)
    for i in range(1, 10):
        d = F.substring(v, i, 1).cast("int")
        total = total + (F.when(d * 2 > 9, d * 2 - 9).otherwise(d * 2) if i % 2 == 1 else d)
    return total


def npi_valid(c: Column) -> Column:
    s = c.cast("string")
    return F.when(
        s.rlike("^[0-9]{10}$"), ((10 - (_luhn_sum(s) % 10)) % 10) == F.substring(s, 10, 1).cast("int")
    ).otherwise(F.lit(False))


def norm_name(c: Column) -> Column:
    """Upper case, letters only, first token (drops middle names, suffixes and punctuation)."""
    up = F.upper(F.regexp_replace(F.coalesce(c, F.lit("")), r"[^A-Za-z ]", ""))
    return F.nullif(F.element_at(F.split(F.trim(up), r"\s+"), 1), F.lit(""))


def zip5(c: Column) -> Column:
    return F.nullif(F.substring(F.regexp_replace(F.coalesce(c, F.lit("")), r"[^0-9]", ""), 1, 5), F.lit(""))


def nppes_people() -> DataFrame:
    """NPPES individuals with normalized name keys, one row per (npi, location key)."""
    n = lake.read("silver", "nppes_provider").where(F.col("entity_type") == 1)
    base = n.select(
        "npi",
        norm_name(F.col("first_name")).alias("fn"),
        norm_name(F.col("last_name")).alias("ln"),
        zip5(F.col("practice_zip")).alias("pz"),
        zip5(F.col("mailing_zip")).alias("mz"),
        F.upper(F.col("practice_state")).alias("ps"),
        F.upper(F.col("mailing_state")).alias("ms"),
    )
    return base


def _candidates(recipients: DataFrame, people: DataFrame, loc: str) -> DataFrame:
    """Unique-candidate lookup on (fn, ln, location). loc = 'zip' or 'state'."""
    if loc == "zip":
        idx = people.select("npi", "fn", "ln", F.col("pz").alias("loc")).unionByName(
            people.select("npi", "fn", "ln", F.col("mz").alias("loc"))
        )
        rkey = "zip5"
    else:
        idx = people.select("npi", "fn", "ln", F.col("ps").alias("loc")).unionByName(
            people.select("npi", "fn", "ln", F.col("ms").alias("loc"))
        )
        rkey = "state"
    idx = idx.where(F.col("loc").isNotNull() & F.col("fn").isNotNull() & F.col("ln").isNotNull()).distinct()
    grouped = idx.groupBy("fn", "ln", "loc").agg(
        F.countDistinct("npi").alias("n_candidates"), F.min("npi").alias("candidate_npi")
    )
    return recipients.join(
        grouped,
        (recipients.fn == grouped.fn) & (recipients.ln == grouped.ln) & (recipients[rkey] == grouped.loc),
        "left",
    ).select(recipients["*"], grouped.n_candidates, grouped.candidate_npi)


def distinct_recipients(payments: DataFrame) -> DataFrame:
    """One row per distinct recipient identity as written on payment records (persons only)."""
    return (
        payments.where(F.col("recipient_type").isin(PERSON_TYPES))
        .select(
            "recipient_profile_id",
            F.col("recipient_npi_raw"),
            norm_name(F.col("recipient_first_name")).alias("fn"),
            norm_name(F.col("recipient_last_name")).alias("ln"),
            zip5(F.col("recipient_zip")).alias("zip5"),
            F.upper(F.col("recipient_state")).alias("state"),
        )
        .distinct()
    )


def resolve(recipients: DataFrame, use_npi: bool = True, use_state: bool | None = None) -> DataFrame:
    """Run the ladder. With use_npi=False rungs 1-2 are skipped (that is the holdout evaluation mode)."""
    people = nppes_people()
    # every NPI in the registry counts as known, including deactivated ones (their rows carry no name or
    # entity type, so they are not in `people`); provider_360 then shows them as deactivated
    known = lake.read("silver", "nppes_provider").select(F.col("npi").alias("k_npi"))
    r = recipients
    if use_npi:
        profile = lake.read("silver", "op_profile").select(
            F.col("profile_id").alias("recipient_profile_id"), F.col("npi_raw").alias("profile_npi_raw")
        )
        r = r.join(profile, "recipient_profile_id", "left")
        r = r.withColumn(
            "rec_npi",
            F.when(npi_valid(F.col("recipient_npi_raw")), F.col("recipient_npi_raw").cast("bigint")),
        ).withColumn(
            "prof_npi", F.when(npi_valid(F.col("profile_npi_raw")), F.col("profile_npi_raw").cast("bigint"))
        )
        r = r.join(
            known.withColumnRenamed("k_npi", "rec_npi_known"),
            F.col("rec_npi") == F.col("rec_npi_known"),
            "left",
        )
        r = r.join(
            known.withColumnRenamed("k_npi", "prof_npi_known"),
            F.col("prof_npi") == F.col("prof_npi_known"),
            "left",
        )
    else:
        r = r.withColumn("rec_npi_known", F.lit(None).cast("bigint")).withColumn(
            "prof_npi_known", F.lit(None).cast("bigint")
        )
    z = (
        _candidates(r, people, "zip")
        .withColumnRenamed("n_candidates", "n_zip")
        .withColumnRenamed("candidate_npi", "zip_npi")
    )
    s = (
        _candidates(z, people, "state")
        .withColumnRenamed("n_candidates", "n_state")
        .withColumnRenamed("candidate_npi", "state_npi")
    )
    use_state = USE_NAME_STATE if use_state is None else use_state
    if not use_state:
        s = s.withColumn("n_state", F.lit(None).cast("long"))
    method = (
        F.when(F.col("rec_npi_known").isNotNull(), "record_npi")
        .when(F.col("prof_npi_known").isNotNull(), "profile_npi")
        .when(F.col("n_zip") == 1, "name_zip")
        .when(F.col("n_zip") > 1, "ambiguous")
        .when(F.col("n_state") == 1, "name_state")
        .when(F.col("n_state") > 1, "ambiguous")
        .otherwise("unresolved")
    )
    resolved = (
        F.when(F.col("rec_npi_known").isNotNull(), F.col("rec_npi_known"))
        .when(F.col("prof_npi_known").isNotNull(), F.col("prof_npi_known"))
        .when(F.col("n_zip") == 1, F.col("zip_npi"))
        .when(F.col("n_zip").isNull() & (F.col("n_state") == 1), F.col("state_npi"))
    )
    return s.select(*recipients.columns, method.alias("method"), resolved.alias("resolved_npi"))


def wilson(k: int, n: int, z: float = 1.96) -> list[float]:
    """95% Wilson score interval for a proportion k/n."""
    if n == 0:
        return [0.0, 0.0]
    p = k / n
    d = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / d
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return [round(centre - half, 5), round(centre + half, 5)]


def evaluate_matcher(resolutions: DataFrame, test_mod: int = 5, use_state: bool | None = None) -> dict:
    """Provider-disjoint holdout: truth = record NPI (rung 1); hide it and run rungs 3-4 only.

    The split is by true NPI, so a provider is entirely in dev or entirely in test. Rules were chosen
    on dev only; the reported numbers are test. Entity level = one row per distinct recipient identity.
    """
    truth = resolutions.where(F.col("method") == "record_npi").select(
        "recipient_profile_id",
        "recipient_npi_raw",
        "fn",
        "ln",
        "zip5",
        "state",
        F.col("resolved_npi").alias("true_npi"),
    )
    # split by the true NPI: one provider is entirely dev or entirely test (several Profile_IDs or name
    # spellings of the same person stay together); a row id carries truth through the matcher, so rows with
    # NULL names or ZIPs are scored too (as misses) instead of falling out of a join
    truth = truth.withColumn(
        "split", F.when(F.abs(F.xxhash64("true_npi")) % test_mod == 0, "test").otherwise("dev")
    ).withColumn("_eid", F.monotonically_increasing_id())
    truth = truth.cache()
    pred = resolve(truth.drop("true_npi", "split"), use_npi=False, use_state=use_state)
    joined = pred.select("_eid", "method", "resolved_npi").join(
        truth.select("_eid", "true_npi", "split"), "_eid"
    )
    n_truth, n_joined = truth.count(), joined.count()
    if n_truth != n_joined:
        raise AssertionError(f"holdout lost rows: {n_truth} truth vs {n_joined} scored")
    out = {}
    rows = (
        joined.groupBy("split")
        .agg(
            F.count("*").alias("n"),
            F.sum(F.when(F.col("resolved_npi").isNotNull(), 1).otherwise(0)).alias("predicted"),
            F.sum(F.when(F.col("resolved_npi") == F.col("true_npi"), 1).otherwise(0)).alias("correct"),
            F.sum(F.when(F.col("method") == "ambiguous", 1).otherwise(0)).alias("abstained"),
            F.sum(F.when(F.col("method") == "name_zip", 1).otherwise(0)).alias("by_name_zip"),
            F.sum(
                F.when(
                    (F.col("method") == "name_zip") & (F.col("resolved_npi") == F.col("true_npi")), 1
                ).otherwise(0)
            ).alias("correct_name_zip"),
            F.sum(F.when(F.col("method") == "name_state", 1).otherwise(0)).alias("by_name_state"),
            F.sum(
                F.when(
                    (F.col("method") == "name_state") & (F.col("resolved_npi") == F.col("true_npi")), 1
                ).otherwise(0)
            ).alias("correct_name_state"),
        )
        .collect()
    )
    for r in rows:
        n, pr, c = r["n"], r["predicted"], r["correct"]
        out[r["split"]] = {
            "entities": n,
            "predicted": pr,
            "correct": c,
            "abstained": r["abstained"],
            "precision": round(c / pr, 5) if pr else None,
            "precision_ci95": wilson(c, pr),
            "recall": round(c / n, 5) if n else None,
            "recall_ci95": wilson(c, n),
            "abstention_rate": round(r["abstained"] / n, 5) if n else None,
            "by_rung": {
                rung: {
                    "predicted": r[f"by_{rung}"],
                    "correct": r[f"correct_{rung}"],
                    "precision": round(r[f"correct_{rung}"] / r[f"by_{rung}"], 5)
                    if r[f"by_{rung}"]
                    else None,
                }
                for rung in ("name_zip", "name_state")
            },
        }
    return out

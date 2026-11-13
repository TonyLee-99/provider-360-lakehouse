"""Turn results/*.json into results/headline.json, the README numbers, docs/LINEAGE.md and a static dashboard.

README numbers are written as <!--n:key-->value<!--/n-->; `p360 report` re-renders them from headline.json and
tests/test_readme_numbers.py fails if they ever drift.
"""

import html
import json
import re

from p360 import governance
from p360.paths import REPO, RESULTS


def _r(name: str) -> dict:
    p = RESULTS / f"{name}.json"
    return json.loads(p.read_text()) if p.exists() else {}


def _fmt(n) -> str:
    return f"{int(n):,}"


def headline() -> dict:
    h: dict = {}
    land = _r("landing")
    if land:
        h["rows_landed"] = _fmt(land["total_rows"])
        h["raw_gb"] = f"{land['raw_bytes'] / 1e9:.1f}"
        for d in land["datasets"]:
            h[f"rows_{d['dataset']}"] = _fmt(d["rows"])
    r1 = _r("r1_open_payments_totals")
    if r1:
        h["r1_cms_records"] = _fmt(r1["cms_records"])
        h["r1_our_records"] = _fmt(r1["our_live_records"])
        h["r1_cms_usd"] = f"{float(r1['cms_usd']):,.2f}"
        h["r1_usd_diff"] = r1["usd_diff"]
        h["r1_records_diff"] = str(r1["records_diff"])
    r2 = _r("r2_recipient_totals")
    if r2:
        h["r2_recipients"] = _fmt(r2["recipients_compared"])
        h["r2_exact_pct"] = str(r2["exact_pct"])
    r3 = _r("r3_partd_by_provider")
    if r3:
        h["r3_npis"] = _fmt(r3["npis_compared"])
        h["r3_exact_pct"] = str(r3["exact_pct"])
        h["r3_unexplained"] = _fmt(r3["unexplained_npis"])
        c = r3["classes"]
        prov = sum(v["provider_file_claims"] for v in c.values())
        drug = sum(v["drug_file_claims"] for v in c.values())
        h["r3_claims_coverage_pct"] = f"{100 * drug / prov:.1f}"
        h["r3_lower_npis"] = _fmt(c.get("lower_consistent_with_suppression", {}).get("npis", 0))
        h["r3_no_drug_rows_npis"] = _fmt(c.get("provider_has_no_drug_rows", {}).get("npis", 0))
    rs = _r("restatement")
    if rs:
        h["restatement_records"] = _fmt(rs["records"])
        h["restatement_agree_pct"] = str(rs["agree_pct"])
        m = rs["merge_metrics"].get("2026-06", {})
        h["june_inserted"] = _fmt(m.get("numTargetRowsInserted", 0))
        h["june_changed"] = _fmt(m.get("numTargetRowsMatchedUpdated", 0))
        h["june_soft_deleted"] = _fmt(m.get("numTargetRowsNotMatchedBySourceUpdated", 0))
        h["june_copied_unchanged"] = _fmt(m.get("numTargetRowsCopied", 0))
        w = (rs.get("nppes_weekly") or {}).get("metrics", {})
        h["nppes_weekly_source"] = _fmt(w.get("numSourceRows", 0))
        h["nppes_weekly_inserted"] = _fmt(w.get("numTargetRowsInserted", 0))
        h["nppes_weekly_updated"] = _fmt(w.get("numTargetRowsUpdated", 0))
    idem = _r("idempotency")
    if idem:
        h["reapply_changed_rows"] = str(
            sum(
                int(v)
                for part in ("op_reapply", "nppes_weekly_reapply")
                for k, v in idem[part].items()
                if k.startswith("num")
            )
        )
    ide = _r("identity")
    if ide:
        t = ide["holdout"]["test"]
        h["er_test_entities"] = _fmt(t["entities"])
        h["er_precision_pct"] = f"{100 * t['precision']:.2f}"
        h["er_recall_pct"] = f"{100 * t['recall']:.2f}"
        h["er_abstain_pct"] = f"{100 * t['abstention_rate']:.2f}"
        h["er_precision_ci_low_pct"] = f"{100 * t['precision_ci95'][0]:.2f}"
        pm = ide["payments_by_method"]
        total = sum(v["records"] for v in pm.values())
        rec = pm.get("record_npi", {}).get("records", 0)
        h["er_payments_total"] = _fmt(total)
        h["er_payments_rescued"] = _fmt(
            sum(pm.get(k, {}).get("records", 0) for k in ("profile_npi", "name_zip", "name_state"))
        )
        h["er_payments_unresolved"] = _fmt(
            sum(pm.get(k, {}).get("records", 0) for k in ("ambiguous", "unresolved"))
        )
        h["er_record_npi_pct"] = f"{100 * rec / total:.2f}" if total else "0"
    perf = _r("performance")
    if perf:
        z = perf["zorder_point_lookup"]
        h["zorder_files_before"] = _fmt(z["before"].get("numFiles", 0))
        h["zorder_files_after"] = _fmt(z["after"].get("numFiles", 0))
        a = perf.get("agg_incremental_vs_full")
        if a:
            h["agg_incremental_s"] = str(a["incremental_seconds"])
            h["agg_full_s"] = str(a["full_seconds"])
            h["agg_npis_recomputed"] = _fmt(a["npis_recomputed"])
            h["agg_rows"] = _fmt(a["rows"])
        j = perf["join_strategy"]
        h["join_smj_s"] = str(j["sort_merge"]["seconds"])
        h["join_bcast_s"] = str(j["broadcast"]["seconds"])
    for tag in ("1_january", "2_june"):
        run = _r(f"run_{tag}")
        if run:
            h[f"run_{tag}_minutes"] = f"{run['seconds'] / 60:.1f}"
            h[f"run_{tag}_checks_passed"] = str(sum(c["passed"] for c in run["checks"]))
            h[f"run_{tag}_checks"] = str(len(run["checks"]))
    p360 = _r("provider_360")
    if p360:
        h.update(
            {k: (_fmt(v) if isinstance(v, int) else str(v)) for k, v in p360.items() if k != "recorded_at"}
        )
    return h


def render_readme(h: dict) -> None:
    p = REPO / "README.md"
    if not p.exists():
        return
    text = p.read_text()

    def sub(m):
        key = m.group(1)
        return f"<!--n:{key}-->{h.get(key, m.group(2))}<!--/n-->"

    p.write_text(re.sub(r"<!--n:([a-z0-9_]+)-->(.*?)<!--/n-->", sub, text))


def dashboard(h: dict) -> None:
    rows = "".join(f"<tr><td>{html.escape(k)}</td><td>{html.escape(str(v))}</td></tr>" for k, v in h.items())
    tables = []
    for name in ("r3_partd_by_provider", "r2_recipient_totals"):
        r = _r(name)
        if r:
            body = "".join(
                f"<tr><td>{html.escape(k)}</td>"
                + "".join(f"<td>{html.escape(str(x))}</td>" for x in v.values())
                + "</tr>"
                for k, v in r["classes"].items()
            )
            hdr = "".join(f"<th>{html.escape(c)}</th>" for c in next(iter(r["classes"].values())).keys())
            tables.append(f"<h2>{name}</h2><table><tr><th>class</th>{hdr}</tr>{body}</table>")
    rs = _r("restatement")
    if rs:
        cols = sorted({c for v in rs["crosstab"].values() for c in v})
        body = "".join(
            f"<tr><td>{html.escape(k)}</td>" + "".join(f"<td>{v.get(c, 0):,}</td>" for c in cols) + "</tr>"
            for k, v in sorted(rs["crosstab"].items())
        )
        tables.append(
            "<h2>Restatement: CMS Change_Type (rows) vs our MERGE (columns)</h2><table><tr><th></th>"
            + "".join(f"<th>{c}</th>" for c in cols)
            + f"</tr>{body}</table>"
        )
    page = f"""<!doctype html><html><head><meta charset="utf-8"><title>provider-360-lakehouse results</title>
<style>body{{font:14px system-ui;margin:24px;max-width:1100px}}table{{border-collapse:collapse;margin:8px 0 24px}}
td,th{{border:1px solid #ccc;padding:4px 8px;text-align:right}}td:first-child,th:first-child{{text-align:left}}
h1{{font-size:20px}}h2{{font-size:16px;margin-top:24px}}</style></head><body>
<h1>provider-360-lakehouse: measured results</h1>
<p>Generated by <code>p360 report</code> from <code>results/*.json</code>.</p>
{"".join(tables)}<h2>All headline numbers</h2><table>{rows}</table></body></html>"""
    out = REPO / "docs" / "dashboard.html"
    out.write_text(page)


def build() -> None:
    h = headline()
    (RESULTS / "headline.json").write_text(json.dumps(h, indent=2) + "\n")
    (REPO / "docs" / "LINEAGE.md").write_text(
        "# Column lineage of gold.provider_360\n\nGenerated from `p360/governance.py` by `p360 report`. "
        "Columns whose sources are direct PII are excluded from `gold.provider_360_public`.\n\n"
        + governance.lineage_table()
    )
    render_readme(h)
    dashboard(h)
    print(f"headline.json: {len(h)} numbers")

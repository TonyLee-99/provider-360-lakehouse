"""Command line: `p360 <command>`. The Makefile wraps these."""

import argparse
import json
import os
import resource
import shutil
import sys
import time

from p360.paths import DATA, LAKE, RESULTS


def _run_pipeline(publications: list[str], tag: str) -> dict:
    from p360.defs import defs

    t0 = time.time()
    job = defs.resolve_job_def("pipeline")
    res = job.execute_in_process(
        run_config={"ops": {"silver_op_payment": {"config": {"publications": publications}}}},
        raise_on_error=False,
    )
    checks = []
    for ev in res.all_events:
        if ev.event_type_value == "ASSET_CHECK_EVALUATION":
            d = ev.event_specific_data
            checks.append({"check": d.check_name, "asset": d.asset_key.to_user_string(), "passed": d.passed})
    mats = [
        ev.asset_key.to_user_string()
        for ev in res.all_events
        if ev.event_type_value == "ASSET_MATERIALIZATION"
    ]
    return {
        "tag": tag,
        "publications": publications,
        "success": res.success,
        "seconds": round(time.time() - t0, 1),
        "assets_materialized": mats,
        "checks": checks,
    }


def cmd_run(args) -> None:
    from p360.results import record

    out = _run_pipeline(args.publications, args.tag)
    run_log = LAKE / "_run_log.jsonl"
    out["steps"] = [json.loads(line) for line in run_log.read_text().splitlines()] if run_log.exists() else []
    out["peak_rss_mb_python"] = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e6, 1)
    if args.tag:
        record(f"run_{args.tag}", out)
    if run_log.exists():
        run_log.unlink()  # per-run step log
    print(json.dumps({k: out[k] for k in ("tag", "success", "seconds")}, indent=2))
    for c in out["checks"]:
        print(("PASS " if c["passed"] else "FAIL ") + c["check"])
    if not out["success"]:
        sys.exit(1)


def cmd_download(args) -> None:
    from p360.download import download

    download(args.names or None)


def cmd_land(args) -> None:
    from p360.landing import land_all

    for r in land_all(args.names or None, force=args.force):
        print(f"{r['dataset']}: {r['rows']:,} rows")


def cmd_experiments(args) -> None:
    from p360 import experiments

    experiments.run_all(args.only or None)


def cmd_report(args) -> None:
    from p360 import report

    report.build()


def cmd_reset(args) -> None:
    for p in [LAKE]:
        shutil.rmtree(p, ignore_errors=True)
    print(f"removed {LAKE}")


def main() -> None:
    os.environ.setdefault("DAGSTER_HOME", str(DATA / ".dagster_home"))
    os.makedirs(os.environ["DAGSTER_HOME"], exist_ok=True)
    p = argparse.ArgumentParser(prog="p360")
    sub = p.add_subparsers(required=True)
    s = sub.add_parser("download")
    s.add_argument("names", nargs="*")
    s.set_defaults(fn=cmd_download)
    s = sub.add_parser("land")
    s.add_argument("names", nargs="*")
    s.add_argument("--force", action="store_true")
    s.set_defaults(fn=cmd_land)
    s = sub.add_parser("run", help="materialize every asset with Dagster (in-process) and run all checks")
    s.add_argument("--publications", nargs="+", default=["2026-01", "2026-06"])
    s.add_argument("--tag", default="")
    s.set_defaults(fn=cmd_run)
    s = sub.add_parser("experiments", help="idempotency, replay, restatement, identity eval, performance")
    s.add_argument("only", nargs="*")
    s.set_defaults(fn=cmd_experiments)
    s = sub.add_parser("report", help="dashboard + README numbers from results/")
    s.set_defaults(fn=cmd_report)
    s = sub.add_parser("reset", help="delete the lake (keeps raw + landing)")
    s.set_defaults(fn=cmd_reset)
    args = p.parse_args()
    RESULTS.mkdir(parents=True, exist_ok=True)
    args.fn(args)


if __name__ == "__main__":
    main()

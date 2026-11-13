"""Download pinned source files into data/raw/<source>/ (resumable) and record them in a manifest.

Why a manifest: every later number can be traced to an exact file (url, bytes, sha256, Last-Modified).
Why resumable: the files are 0.3-4 GB; a dropped connection should not restart from zero.
"""

import hashlib
import json
import sys
import time
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

import yaml

from p360.paths import RAW, SOURCES

USER_AGENT = "provider-360-lakehouse/0.1 (portfolio data pipeline; polite single-stream downloads)"


def load_sources() -> dict:
    return yaml.safe_load(SOURCES.read_text())["sources"]


def raw_path(name: str, url: str) -> Path:
    return RAW / name / url.rsplit("/", 1)[-1]


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _fetch(url: str, dest: Path, retries: int = 5) -> dict:
    part = dest.with_suffix(dest.suffix + ".part")
    for attempt in range(1, retries + 1):
        have = part.stat().st_size if part.exists() else 0
        req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        if have:
            req.add_header("Range", f"bytes={have}-")
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                if have and r.status != 206:  # server ignored Range: start over
                    have = 0
                    part.unlink(missing_ok=True)
                headers = dict(r.headers)
                with part.open("ab") as f:
                    while chunk := r.read(4 << 20):
                        f.write(chunk)
            part.rename(dest)
            return headers
        except Exception as e:  # network errors: back off and resume from the partial file
            wait = min(60, 2**attempt)
            print(f"  {dest.name}: attempt {attempt} failed ({e}); retry in {wait}s", file=sys.stderr)
            time.sleep(wait)
    raise RuntimeError(f"download failed after {retries} attempts: {url}")


def download(names: list[str] | None = None) -> None:
    sources = load_sources()
    manifest = RAW / "_manifest.jsonl"
    RAW.mkdir(parents=True, exist_ok=True)
    for name, spec in sources.items():
        if names and name not in names:
            continue
        dest = raw_path(name, spec["url"])
        if dest.exists():
            print(f"skip {name} (already downloaded)")
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        t0 = time.time()
        print(f"get  {name} <- {spec['url']}")
        headers = _fetch(spec["url"], dest)
        rec = {
            "source": name,
            "url": spec["url"],
            "file": str(dest.relative_to(RAW)),
            "bytes": dest.stat().st_size,
            "sha256": sha256(dest),
            "last_modified": headers.get("Last-Modified"),
            "downloaded_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "seconds": round(time.time() - t0, 1),
        }
        with manifest.open("a") as f:
            f.write(json.dumps(rec) + "\n")
        print(f"done {name}: {rec['bytes']:,} bytes in {rec['seconds']}s")


if __name__ == "__main__":
    download(sys.argv[1:] or None)

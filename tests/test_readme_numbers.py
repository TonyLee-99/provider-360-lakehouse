"""Every number in README.md between <!--n:key--> and <!--/n--> must equal results/headline.json[key]."""

import json
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]


def test_readme_numbers_match_results():
    headline = json.loads((REPO / "results" / "headline.json").read_text())
    readme = (REPO / "README.md").read_text()
    found = re.findall(r"<!--n:([a-z0-9_]+)-->(.*?)<!--/n-->", readme)
    assert found, "README has no tracked numbers"
    wrong = [(k, v, headline.get(k)) for k, v in found if str(headline.get(k)) != v]
    assert not wrong, wrong

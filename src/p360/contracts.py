"""Data contracts: what each source must look like before anything downstream may read it.

A contract (contracts/<name>.yml) lists the columns we depend on, their target types, which are required,
which hold personal data (pii: direct | quasi), and the primary key. Two kinds of checks use it:

- header check (landing): a contract column missing from the file is BREAKING -> the load stops.
  A new column CMS added is ADDITIVE -> recorded, load continues.
- row checks (bronze -> silver): type casts, required fields, accepted values and key uniqueness;
  failing rows go to a quarantine table with a reason, and the asset check fails if the rate is over budget.
"""

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from p360.paths import CONTRACTS


class ContractViolation(Exception):
    """Raised when a source breaks its contract in a way that must stop the pipeline."""


@dataclass
class Column:
    name: str  # name in the source file
    alias: str  # name in our tables
    type: str
    required: bool = False
    pii: str | None = None
    accepted: list[str] | None = None
    format: str | None = None


@dataclass
class Contract:
    name: str
    version: int
    primary_key: list[str]
    columns: list[Column]
    keep_all_source_columns: bool = False
    hash_exclude: list[str] = field(default_factory=list)

    @property
    def source_names(self) -> list[str]:
        return [c.name for c in self.columns]

    def pii_aliases(self, level: str) -> set[str]:
        return {c.alias for c in self.columns if c.pii == level}


def load(name: str, contracts_dir: Path = CONTRACTS) -> Contract:
    raw = yaml.safe_load((contracts_dir / f"{name}.yml").read_text())
    cols = [
        Column(
            name=c["name"],
            alias=c["as"],
            type=c["type"],
            required=c.get("required", False),
            pii=c.get("pii"),
            accepted=c.get("accepted"),
            format=c.get("format"),
        )
        for c in raw["columns"]
    ]
    return Contract(
        name=raw["name"],
        version=raw["version"],
        primary_key=raw["primary_key"],
        columns=cols,
        keep_all_source_columns=raw.get("keep_all_source_columns", False),
        hash_exclude=raw.get("hash_exclude", []),
    )


def check_header(contract: Contract, header: list[str]) -> dict:
    """Compare a file header with the contract. Raises on breaking drift, returns the drift report."""
    have = set(header)
    missing = [c for c in contract.source_names if c not in have]
    added = [h for h in header if h not in set(contract.source_names)]
    report = {"contract": contract.name, "version": contract.version, "missing": missing, "added": added}
    if missing:
        raise ContractViolation(f"{contract.name}: source is missing contract columns {missing}")
    return report

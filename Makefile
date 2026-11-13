# provider-360-lakehouse
# OFFLINE (committed real fixture): init, lint, test, check, demo, ui
# FULL DATA (downloads ~8 GB from CMS, ~45 min on a 16 GB laptop): download, land, run, experiments, report, reproduce
UV ?= uv
RUN = $(UV) run
DEMO = .demo
DEMO_ENV = P360_DATA_DIR=$(DEMO)/data P360_RESULTS_DIR=$(DEMO)/results P360_SCOPE=fixture

.PHONY: init lint test check demo ui download land run experiments report reproduce clean-demo reset

init:          ## fresh clone -> ready (locked deps; needs JDK 17 for Spark)
	$(UV) sync --locked

lint:
	$(RUN) ruff check src tests scripts
	$(RUN) ruff format --check src tests scripts

test:          ## unit + end-to-end tests on fixtures/raw (two Dagster runs, all checks)
	$(RUN) pytest -q

check: lint test  ## what CI runs
	$(RUN) dagster definitions validate -m p360.defs
	$(RUN) pytest -q tests/test_readme_numbers.py

demo: clean-demo  ## offline: run the whole pipeline on the fixture (January publication, then June)
	mkdir -p $(DEMO)/data && cp -R fixtures/raw $(DEMO)/data/raw
	$(DEMO_ENV) $(RUN) p360 run --publications 2026-01
	$(DEMO_ENV) $(RUN) p360 run --publications 2026-01 2026-06
	@echo "Delta tables in $(DEMO)/data/lake (bronze/ silver/ gold/); published versions in gold/_published.json"

ui:            ## Dagster UI on the demo data (asset graph = lineage, check history)
	$(DEMO_ENV) DAGSTER_HOME=$(PWD)/$(DEMO)/data/.dagster_home $(RUN) dagster dev -m p360.defs

clean-demo:
	rm -rf $(DEMO)

# ---------- full data ----------
download:      ## pinned CMS files -> data/raw (+ sha256 manifest)
	$(RUN) p360 download

land:          ## zip/CSV -> all-string Parquet in data/landing (contract header checks)
	$(RUN) p360 land

run:           ## two Dagster runs: as of the January publication, then apply June incrementally
	$(RUN) p360 run --publications 2026-01 --tag 1_january
	$(RUN) p360 run --publications 2026-01 2026-06 --tag 2_june

experiments:   ## restatement vs CMS labels, identity holdout, R2, performance, guards, idempotency
	$(RUN) p360 experiments

report:        ## dashboard + README numbers from results/*.json
	$(RUN) p360 report

reproduce: download land reset run experiments report

reset:         ## delete the lake (raw + landing kept)
	$(RUN) p360 reset

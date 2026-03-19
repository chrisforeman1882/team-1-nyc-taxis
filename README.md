# NYC Yellow Taxi — Data & AI Lakehouse Project

End-to-end Lakehouse pipeline built during the Data Academy 5-day project sprint. The project ingests NYC Yellow Taxi trip data, refines it through Bronze → Silver → Gold layers, trains a predictive ML model, and wraps everything in a RAG-enabled demo.

**Dataset:** [NYC Yellow Taxi Trip Data (Kaggle)](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data/data)

---

## Project Structure

```
team-1-nyc-taxis/
├── data/                   # Raw CSV files (not committed — see .gitignore)
├── notebooks/              # Jupyter / Databricks notebooks (numbered by layer)
├── src/                    # Shared Python library
│   ├── constants.py        # Column names, accepted values, layer paths
│   ├── ingest.py           # Bronze ingestion helpers
│   ├── transforms.py       # Silver cleaning & feature transforms
│   └── validators.py       # Data quality checks
├── tests/                  # pytest test suite (224 tests total)
│   ├── conftest.py             # Shared fixtures (local_spark + Databricks)
│   ├── test_constants.py       # Unit: lookup maps, thresholds, column lists
│   ├── test_transforms.py      # Unit: all I-03/04/05 + A-01 Gold transforms
│   ├── test_validators.py      # Unit: pass/fail for every validator function
│   ├── test_int01_bronze.py    # INT-01: Bronze layer validation
│   ├── test_int01_silver.py    # INT-01: Silver layer validation (I-03/04/05/06)
│   ├── test_int01_gold.py      # INT-01: Gold layer + dims + dashboard readiness
│   ├── test_int01_pipeline.py  # INT-01: Cross-layer reconciliation
│   └── run_int01_tests.py      # Databricks notebook to run full suite
├── .github/workflows/      # GitHub Actions CI & deployment
│   ├── CI.yml              # Lint, security, unit & integration tests
│   └── deploy.yml          # Deploy ETL bundle to dev on merge
├── databricks.yml          # Databricks Asset Bundle config (ETL pipeline)
├── .pre-commit-config.yaml # Local pre-commit hooks
├── .secrets.baseline        # detect-secrets baseline
├── pyproject.toml          # Build config & pytest settings
├── requirements.txt        # Python dependencies
└── ruff.toml               # Linter / formatter config
```

---

## Quickstart

### 1. Clone and set up Python environment

```bash
git clone <repo-url>
cd team-1-nyc-taxis
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
pip install -e .
```

### 2. Install pre-commit hooks

```bash
pre-commit install
```

> **Note:** The `gitleaks` hook requires a local binary.
> Install with: `brew install gitleaks` (macOS) or see [gitleaks releases](https://github.com/gitleaks/gitleaks/releases).

### 3. Add your data

Place the raw CSV files into `data/`:

```
data/
  yellow_tripdata_2015-01.csv
  yellow_tripdata_2016-01.csv
  yellow_tripdata_2016-02.csv
  yellow_tripdata_2016-03.csv
```

These are excluded from version control by `.gitignore`. Download them from Kaggle (link above).

### 4. Run tests

See the [Running Tests](#running-tests) section below for full details.

---

## Running Tests

The test suite has two tiers: **101 unit tests** (local / CI) and **123 integration tests** (Databricks only).

### Unit tests (local / CI / Databricks)

These test `src/` functions using synthetic DataFrames. They run anywhere a SparkSession is available — locally, in CI, or on Databricks.

| Module | Tests | Coverage |
|--------|-------|----------|
| `test_constants.py` | 32 | Lookup maps, thresholds, column lists, table paths |
| `test_transforms.py` | 51 | All I-03, I-04, I-05, and A-01 Gold transform functions |
| `test_validators.py` | 18 | Pass and fail cases for all 8 validator functions |

```bash
# Local / CI (creates a local SparkSession automatically)
pytest tests/test_constants.py tests/test_transforms.py tests/test_validators.py -v
```

On Databricks, run from a notebook cell:

```python
import sys, os, importlib
sys.dont_write_bytecode = True
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"

project_root = "/Workspace/Users/<your-user>/team-1-nyc-taxis"
if project_root not in sys.path:
    sys.path.insert(0, project_root)
os.chdir(project_root)

import src.constants, src.transforms, src.validators
importlib.reload(src.constants)
importlib.reload(src.transforms)
importlib.reload(src.validators)

import pytest
pytest.main([
    "tests/test_constants.py",
    "tests/test_transforms.py",
    "tests/test_validators.py",
    "-v", "--tb=short", "-p", "no:cacheprovider",
    "--import-mode=importlib",
])
```

### INT-01 integration tests (Databricks only)

These validate the full Bronze → Silver → Gold → Dashboard pipeline against the **persisted Delta tables**. They require an active Spark session connected to Unity Catalog.

| Module | Tests | Coverage |
|--------|-------|----------|
| `test_int01_bronze.py` | 23 | Table exists, raw schema intact, min row count (≥90M) |
| `test_int01_silver.py` | 56 | I-03 structure, I-04 business rules, I-05 derived columns, I-06 referential integrity |
| `test_int01_gold.py` | 38 | Fact schema, grain uniqueness, metric validity, dim_time/dim_location, dashboard readiness |
| `test_int01_pipeline.py` | 6 | Bronze→Silver row reduction, Silver→Gold trip count & revenue reconciliation |

#### Option A: Run via notebook (recommended)

Open `tests/run_int01_tests` in Databricks and click **Run All**. The notebook installs pytest, configures the Spark session, and runs the full suite. It will fail with an `AssertionError` if any test fails.

#### Option B: Run from any notebook cell

Same pattern as unit tests above, but include the `test_int01_*` files in the pytest arguments.

### Databricks-specific notes

> **Why `pytest.main()` instead of `python -m pytest`?**
> Databricks Spark Connect sessions are tied to the notebook kernel process. Subprocesses (shell commands) cannot access the active session, so tests must run in-process.

> **Why `sys.dont_write_bytecode = True`?**
> Databricks workspace directories do not support `__pycache__` writes. This flag prevents Python from attempting to create `.pyc` files.

> **Why `--import-mode=importlib`?**
> Forces pytest to use fresh module imports on each run, avoiding stale cached versions when files are edited between runs.

---

## CI Pipeline (GitHub Actions)

The `.github/workflows/CI.yml` pipeline runs on every push and pull request to `main`:

### Job 1: `lint-and-security` (GitHub-hosted runner)

| Step | Tool | Purpose |
|---|---|---|
| Lint | `ruff check` | Style & error checking |
| Format | `ruff format --check` | Consistent formatting |
| Secrets | `detect-secrets` | Baseline secret scan |
| Secrets | `gitleaks` | Full history secret scan |
| Security | `snyk` | Dependency vulnerability scan |
| Tests | `pytest` | Unit test suite (101 tests) |

### Job 2: `integration-tests` (Databricks cluster)

Runs after `lint-and-security` passes. Uses the `databricks/run-notebook` action to execute `tests/run_int01_tests` on a Databricks cluster, validating the full Bronze → Silver → Gold → Dashboard pipeline (123 tests).

### Required GitHub Secrets

Add these in **Settings → Secrets → Actions** on your repository:

| Secret | Description |
|---|---|
| `SNYK_TOKEN` | API token from [snyk.io](https://snyk.io) |
| `DATABRICKS_HOST` | Workspace URL, e.g. `https://adb-xxxx.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | Personal access token for the workspace |
| `DATABRICKS_CLUSTER_ID` | Cluster ID to run integration tests on |

> `GITHUB_TOKEN` is provided automatically by GitHub Actions — no configuration needed.

---

## ETL Pipeline (Databricks Asset Bundles)

The ETL pipeline is defined as a **Databricks Asset Bundle** in `databricks.yml`. It creates a multi-task Databricks Workflow that runs the notebooks in sequence:

```
bronze_ingest → silver_cleaning → silver_dq → gold_analytics → gold_dashboard
```

| Task | Notebook | Layer |
|---|---|---|
| `bronze_ingest` | `01_ingest` | Raw CSV → Bronze Delta |
| `silver_cleaning` | `02_silver_cleaning` | Clean, cast, derive → Silver Delta |
| `silver_dq` | `02_silver_dq` | Data quality checks |
| `gold_analytics` | `03_gold_analytics` | Fact table, dim_time, dim_location |
| `gold_dashboard` | `03_gold_dashboard` | Dashboard aggregations |

### Automatic deployment

The `.github/workflows/deploy.yml` workflow triggers on every merge to `main`:

1. **Validate** — `databricks bundle validate -t dev`
2. **Deploy** — `databricks bundle deploy -t dev` (syncs notebooks + creates/updates the Workflow)
3. **Run** — `databricks bundle run -t dev nyc-taxi-etl` (triggers the ETL pipeline)

### Manual deployment (local)

```bash
# Install Databricks CLI: https://docs.databricks.com/dev-tools/cli/install.html
databricks bundle validate -t dev
databricks bundle deploy  -t dev
databricks bundle run     -t dev nyc-taxi-etl
```

### Targets

| Target | Mode | Catalog | Schema |
|---|---|---|---|
| `dev` (default) | development | `students_data` | `chris-foreman` |
| `prod` | production | `students_data` | `chris-foreman` |

---


## Five-Day Project Plan

| Day | Task | Key Outputs |
|---|---|---|
| 1 | Planning | Project plan, roles, milestones |
| 2 | Ingestion & Foundation | Bronze & Silver Delta tables, data dictionary |
| 3 | Modelling & Analytics | Gold table, KPI dashboard |
| 4 | Machine Learning | MLflow experiment, trained model |
| 5 | RAG & Final Demo | RAG notebook, integrated capability demo |

See [Data Academy Project Overview](Data%20Academy%20Project%20Overview.md) for full task descriptions.

---

## Notebook Naming Convention

```
00_explore.ipynb          # Initial EDA
01_ingest.ipynb           # Bronze ingestion
02_silver_*.ipynb         # Silver cleaning & DQ
03_gold_*.ipynb           # Gold / analytical layer
04_ml_*.ipynb             # Machine learning
05_rag_*.ipynb            # RAG workflow
```

---

## Contributing

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Make changes, ensuring pre-commit hooks pass locally
3. Open a pull request against `main` — CI must pass before merging

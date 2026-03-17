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
├── tests/                  # pytest unit tests
├── .github/workflows/      # GitHub Actions CI pipeline
├── .pre-commit-config.yaml # Local pre-commit hooks
├── .secrets.baseline       # detect-secrets baseline
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

```bash
pytest tests/ -v
```

---

## CI Pipeline (GitHub Actions)

The `.github/workflows/CI.yml` pipeline runs on every push and pull request to `main`:

| Step | Tool | Purpose |
|---|---|---|
| Lint | `ruff check` | Style & error checking |
| Format | `ruff format --check` | Consistent formatting |
| Secrets | `detect-secrets` | Baseline secret scan |
| Secrets | `gitleaks` | Full history secret scan |
| Security | `snyk` | Dependency vulnerability scan |
| Tests | `pytest` | Unit test suite |

### Required GitHub Secrets

Add these in **Settings → Secrets → Actions** on your repository:

| Secret | Description |
|---|---|
| `SNYK_TOKEN` | API token from [snyk.io](https://snyk.io) |

> `GITHUB_TOKEN` is provided automatically by GitHub Actions — no configuration needed.

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

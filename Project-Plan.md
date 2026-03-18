# NYC Yellow Taxi — Data & AI Lakehouse Project Plan

> **Team Size:** 4 · **Duration:** 5 Days · **Dataset:** [NYC Yellow Taxi Trip Data (Kaggle)](https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data/data)

---

## Business Questions & End Goals

Before diving into tickets, the team should align on **what we want to demonstrate to stakeholders**. The NYC Yellow Taxi dataset is rich enough to answer real operational and commercial questions.

### Primary Business Questions

| # | Question | Layer | Why It Matters |
|---|----------|-------|----------------|
| BQ-1 | **Where and when is taxi demand highest?** | Analytics / Dashboard | Helps fleet operators position vehicles and plan shift patterns |
| BQ-2 | **What drives fare revenue — distance, time-of-day, or location?** | Analytics / Gold KPI | Enables pricing strategy and revenue forecasting |
| BQ-3 | **Can we predict the total fare of a trip before it starts?** | ML Pipeline | Gives passengers upfront estimates; helps dispatchers plan |
| BQ-4 | **Which factors influence whether a passenger tips, and how much?** | ML Pipeline (stretch) | Insight for driver incentive programmes |
| BQ-5 | **Can a conversational interface answer ad-hoc questions about NYC taxi operations & policy?** | RAG | Demonstrates AI-assisted decision support for non-technical stakeholders |

### End-Goal Demo Narrative

> *"We built a complete Data & AI platform that ingests raw taxi trip records, cleans and models them into business-ready tables, surfaces KPIs on a live dashboard, predicts trip fares using ML, and lets anyone ask natural-language questions about NYC taxi data and policy — all inside Databricks."*

---

## Phases & Tickets

### Legend

- **Blocked by →** means this ticket cannot start until the listed ticket(s) are done.
- **Parallel with ‖** means tickets can be worked on at the same time by different people.
- Priority: 🔴 Must have · 🟡 Should have · 🟢 Nice to have (stretch)

---

## Phase 1 — Ingestion & Data Foundation (Day 1)

| Ticket | Title | Blocked by | Priority |
|--------|-------|------------|----------|
| **I-01** | Ingest raw CSV/Parquet into **Bronze** Delta table (append-only, no transforms) | - | 🔴 |
| **I-02** | Explore & profile Bronze data (row counts, nulls, outliers, value distributions) | I-01 | 🔴 |
| **I-03** | Build **Silver** table — clean column names, cast types, drop corrupt rows | I-02 | 🔴 |

> **I-03 note:** Transform functions in `src/transforms.py`, constants in `src/constants.py`, notebook `notebooks/02_silver_cleaning.ipynb` (I-03 section).

| **I-04** | Handle missing values & outlier trips (e.g. $0 fares, 0-distance, negative amounts) | I-03 | 🔴 |
| **I-05** | Add derived columns: `trip_duration_min`, `hour_of_day`, `day_of_week`, `is_weekend` | I-03 | 🔴 |
| **I-06** | Write data quality checks / assertions on Silver table (non-null key fields, fare > 0) | I-04 | 🔴 |

> **I-06 note:** Validator functions in `src/validators.py`, DQ checks + I-07 assumptions log in `notebooks/02_silver_dq.ipynb`.

| **I-07** | Document assumptions & data quality log in a markdown cell or notebook | I-06 | 🔴 |

**Parallel work while ingestion is in progress:**

| Ticket | Title | Blocked by | Priority |
|--------|-------|------------|----------|
| **R-01** | Collect & prepare RAG corpus documents (TLC rules, taxi FAQs, pricing policy) | - | 🔴 |
| **R-02** | Chunk documents into retrieval-friendly segments | R-01 | 🔴 |
| **ML-01** | Research & select ML algorithm candidates (LinearRegression, GBT, RF) | - | 🔴 |

### Dependency Map — Phase 1

```
I-01 ──► I-02 ──► I-03 ──┬──► I-04 ──► I-06 ──► I-07
                         │
                         └──► I-05 (can run ‖ with I-04)

R-01 ──► R-02          (independent of ingestion)
ML-01                   (independent of ingestion)
```

### Day 2 Milestone: ✅ Bronze & Silver Delta tables exist. Silver is clean & documented. RAG corpus drafted.

---

## Phase 2 — Modelling, Analytics & ML Prep (Day 2)

| Ticket | Title | Blocked by | Priority |
|--------|-------|------------|----------|
| **A-01** | Design Gold analytical table / star schema (fact_trips + dim_location + dim_time) | I-05 | 🔴 |
| **A-02** | Build **Gold** fact table with pre-aggregated metrics (avg fare, trip count, avg duration per zone/hour) | A-01 | 🔴 |
| **A-03** | Compute KPI 1: **Revenue per zone per hour** | A-02 | 🔴 |
| **A-04** | Compute KPI 2: **Average trip duration by day-of-week** | A-02 | 🔴 |
| **A-05** | Build initial dashboard — demand heatmap + KPI cards | A-03, A-04 | 🔴 |
| **A-06** | Add contextual narrative to dashboard ("Why this matters") | A-05 | 🔴 |

| Ticket | Title | Blocked by | Priority |
|--------|-------|------------|----------|
| **ML-02** | Build ML feature table from Silver (pickup loc, hour, day, distance, passenger count) | I-05 | 🔴 |
| **ML-03** | Train **baseline model** (e.g. Linear Regression) for fare prediction | ML-02 | 🔴 |
| **ML-04** | Train **improved model** (e.g. Gradient Boosted Trees) | ML-03 | 🔴 |
| **ML-05** | Log both models, parameters & metrics (RMSE, MAE, R²) to **MLflow** | ML-03, ML-04 | 🔴 |

| Ticket | Title | Blocked by | Priority |
|--------|-------|------------|----------|
| **R-03** | Generate embeddings for RAG corpus & store as vector table | R-02 | 🔴 |
| **R-04** | Build retrieval function — query embeddings, return top-k chunks | R-03 | 🔴 |

### Dependency Map — Phase 2

```
I-05 ──┬──► A-01 ──► A-02 ──┬──► A-03 ──┐
       │                    └──► A-04 ──┼──► A-05 ──► A-06
       │                                │
       └──► ML-02 ──► ML-03 ──┬──► ML-04
                              └──► ML-05 (after both models done)

R-02 ──► R-03 ──► R-04       (independent of analytics/ML)
```

### Day 3 Milestone: ✅ Gold table & dashboard draft exist. Baseline + improved ML models logged. RAG vector store populated.

---

## Phase 3 — RAG, Integration & Polish (Day 3)

| Ticket | Title | Blocked by | Priority |
|--------|-------|------------|----------|
| **R-05** | Build RAG notebook — prompt template + retrieval + LLM answer generation | R-04 | 🔴 |
| **R-06** | Test RAG with 5-10 sample questions & document quality of answers | R-05 | 🔴 |
| **ML-06** | Register best model in MLflow Model Registry | ML-05 | 🔴 |
| **ML-07** | Write model interpretation summary (feature importance, what the model learned) | ML-05 | 🔴 |
| **D-01** | Finalise dashboard — layout, titles, filters, colour scheme | A-06 | 🔴 |
| **D-02** | Add ML prediction output as a dashboard element (predicted vs actual scatter) | ML-06, D-01 | 🟡 |
| **INT-01** | End-to-end integration test: Bronze → Silver → Gold → Dashboard pipeline runs cleanly | A-02, I-06 | 🔴 |
| **INT-02** | Write project **README** — architecture diagram, how to run, assumptions | INT-01 | 🔴 |

### Dependency Map — Phase 3

```
R-04 ──► R-05 ──► R-06

ML-05 ──┬──► ML-06
        └──► ML-07

A-06 ──► D-01 ──┐
ML-06 ──────────┼──► D-02
                │
I-06 + A-02 ──► INT-01 ──► INT-02
```

### Day 4 Milestone: ✅ RAG working. Dashboard polished. ML model registered. Pipeline validated end-to-end. README drafted.

---

## Phase 4 — Demo Assembly & Presentation (Day 4)

| Ticket | Title | Blocked by | Priority |
|--------|-------|------------|----------|
| **DEMO-01** | Build demo script / run-order notebook linking all components | INT-01, R-06, ML-06 | 🔴 |
| **DEMO-02** | Rehearse demo — team walks through the full narrative end-to-end | DEMO-01 | 🔴 |
| **DEMO-03** | Final bug fixes & polish from rehearsal feedback | DEMO-02 | 🔴 |
| **DEMO-04** | Deliver stakeholder demo | DEMO-03 | 🔴 |

### Day 5 Milestone: ✅ Stakeholder demo delivered. Project complete.

---

## Stretch Goal Tickets (if time allows)

| Ticket | Title | Blocked by | Priority | Stretch Goal |
|--------|-------|------------|----------|--------------|
| **S-01** | Add DLT expectation rules on Silver table for ongoing quality monitoring | I-06 | 🟢 | A — Data Quality |
| **S-02** | Build a data quality scorecard dashboard | S-01 | 🟢 | A — Data Quality |
| **S-03** | Simulate incremental / streaming ingestion with Auto Loader | I-01 | 🟢 | B — Streaming |
| **S-04** | Add freshness & latency metrics to dashboard | S-03, D-01 | 🟢 | B — Streaming |
| **S-05** | Engineer extra features (weather API join, lag features, zone clustering) | ML-02 | 🟢 | C — Extended ML |
| **S-06** | Compare 3+ algorithms & produce a comparison table | ML-04 | 🟢 | C — Extended ML |
| **S-07** | Add fairness or drift analysis on ML model | ML-06 | 🟢 | C — Extended ML |
| **S-08** | Experiment with chunking strategies & retrieval top-k tuning | R-04 | 🟢 | D — Advanced RAG |
| **S-09** | Build a small Q&A evaluation set and score RAG accuracy | R-06 | 🟢 | D — Advanced RAG |
| **S-10** | Add a safety / guardrail filter to RAG responses | R-05 | 🟢 | D — Advanced RAG |

---

## Full Dependency Graph (Simplified)

```

DAY 2 (Ingestion)
  I-01 ──► I-02 ──► I-03 ──► I-04 ──► I-06 ──► I-07
                                   └──► I-05 ─────────────────────┐
  R-01 ──► R-02                                         │
  ML-01                                                  │
                                                                  │
DAY 3 (Analytics + ML + RAG vectors)                              │
  I-05 ──► A-01 ──► A-02 ──► A-03 + A-04 ──► A-05 ──► A-06      │
  I-05 ──► ML-02 ──► ML-03 ──► ML-04 ──► ML-05                   │
  R-02 ──► R-03 ──► R-04                                         │
                                                                  │
DAY 4 (Integration)                                               │
  R-04 ──► R-05 ──► R-06                                         │
  ML-05 ──► ML-06 + ML-07                                        │
  A-06 ──► D-01 ──► D-02                                         │
  I-06 + A-02 ──► INT-01 ──► INT-02                              │
                                                                  │
DAY 5 (Demo)                                                      │
  INT-01 + R-06 + ML-06 ──► DEMO-01 ──► DEMO-02 ──► DEMO-03 ──► DEMO-04
```

---

## Conflict & Risk Notes

| Risk | Tickets Affected | Mitigation |
|------|-----------------|------------|
| Silver table delayed → blocks **everything** downstream | A-01, ML-02, I-05, I-06 | Swarm on ingestion — multiple people pair on I-01→I-04 if needed |
| ML model training takes too long on large dataset | ML-03, ML-04 | Use a sampled subset (e.g. 1 month) for training; scale up only if time permits |
| RAG corpus is thin / low quality | R-01, R-05, R-06 | Prepare fallback: use the data dictionary + project README as part of the corpus |
| Dashboard tooling issues (permissions, SQL warehouse) | A-05, D-01 | Set up SQL warehouse on Day 1 (P-02); have a notebook-based fallback with `displayHTML` |
| Integration test reveals data issues on Day 4 | INT-01 | Run a lightweight smoke test after Day 2 as well — don't wait until Day 4 |
| Someone finishes a ticket and nothing is unblocked yet | Any | Grab a stretch ticket (S-*), pair with someone in-progress, or write docs/tests |

---

## Ticket Summary

| Phase | Core Tickets | Stretch Tickets | Total |
|-------|-------------|-----------------|-------|
| 0 — Planning | 7 | 0 | 7 |
| 1 — Ingestion | 10 | 0 | 10 |
| 2 — Analytics & ML | 10 | 0 | 10 |
| 3 — RAG & Integration | 8 | 0 | 8 |
| 4 — Demo | 4 | 0 | 4 |
| Stretch | 0 | 10 | 10 |
| **Total** | **39** | **10** | **49** |
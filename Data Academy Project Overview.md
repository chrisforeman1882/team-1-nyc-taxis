# Core Project (5 Days) - “End-to-End Lakehouse with ML & RAG Demonstrator”

## Purpose of the Project

Over five days, participants build a simplified but coherent end-to-end Data & AI product using all key skills from the first eight days of the Academy. The project is intentionally structured to be achievable for entry-level engineers while still reflecting real industry workflows.

The project follows five pillars:

1. **Ingestion (Bronze)**
2. **Refinement (Silver)**
3. **Analytics & Dashboarding**
4. **Machine Learning (MLflow-based experiment)**
5. **Retrieval-Augmented Generation (RAG)**

The outcome is a **complete stakeholder-ready demo** showing an integrated Data & AI solution.

## Dataset

For this project we will use the following dataset:  
https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data/data

## Task 1 – Planning Stage

**Learning Goals**
- Project planning

**Tasks**
- Build project plan covering all key activities
- Define key milestones
- Assign project roles and responsibilities

**Outputs**
- Clearly defined and documented project plan covering all key activities

## Task 2 - Ingestion & Data Foundation

**Learning Goals**
- Understand how raw data enters a Lakehouse
- Build clean and reliable tables for downstream processing

**Tasks**
- Ingest raw dataset(s) into a Bronze area
- Apply schema and initial validations
- Transform into a Silver layer with basic cleaning
- Handle missing values, fix simple inconsistencies
- Document assumptions & data quality considerations

**Outputs**
- Bronze and Silver Delta tables
- Data dictionary detailing schema design, entity relationships

## Task 3 - Modelling & Analytics Layer

**Learning Goals**
- Move from raw data to insight
- Understand simple dimensional or analytical design patterns

**Tasks**
- Develop a curated analytical table (or small star schema subset)
- Compute at least one meaningful KPI
- Build a dashboard tile visualising business insight
- Add light contextual explanation (“Why this matters”)

**Outputs**
- Curated Silver/Gold table
- Dashboard with 1-2 KPIs and basic storyline

## Task 4 - Machine Learning Mini-Pipeline

**Learning Goals**
- Understand the process of model training, evaluation and logging
- Produce a simple predictive capability

**Tasks**
- Define a prediction problem using the dataset (e.g., churn, propensity, classification)
- Train a simple model (baseline + one improved candidate)
- Log model, parameters and metrics with MLflow
- Interpret outcomes at a high level

**Outputs**
- Registered ML experiment with metrics
- Short explanation of the modelling approach

## Task 5 - RAG Workflow & Final Integrated Demo

**Learning Goals**
- Introduce retrieval-based NLP workflows
- Connect unstructured content to the analytical and ML components

**Tasks**
- Prepare document corpus (product descriptions, FAQs, policy text)
- Create embeddings and store them as a searchable vector table
- Build a RAG notebook answering simple product or business queries
- Assemble the solution into a coherent, consultant-style demo

**Outputs**
- RAG demonstration notebook
- Final Data & AI capability demo (structured, value-oriented)

## Stretch Extensions

Optional extensions for those who have time to expand the solution.

### Stretch Goal A : Enhanced Data Quality & Monitoring
- Add additional validation rules
- Create quality scorecards or dashboard indicators
- Document possible data issues and mitigation strategies

**Outcome:** Stronger data governance & reliability mindset.

### Stretch Goal B : Incremental or Streaming Ingestion
- Introduce incremental updates or replay sample streaming data
- Add time-based logic: freshness, late-arriving data, ingestion latency
- Update KPIs to show real-time or semi-streaming outputs

**Outcome:** Early exposure to production-like ingestion patterns.

### Stretch Goal C : Extended ML Exploration
- Engineer additional features
- Compare multiple algorithms
- Perform fairness, stability or drift analysis at awareness level

**Outcome:** Deeper understanding of how ML models can be improved responsibly.

### Stretch Goal D : Advanced RAG Techniques
- Experiment with chunking and retrieval configurations
- Create a simple evaluation set for Q&A relevance
- Add a safety filter or query guardrail

**Outcome:** Understanding of how RAG systems behave, fail and can be improved.

## Expected Final Deliverables (for management overview)

### Mandatory (5-Day Base Project)
- Bronze & Silver tables
- Curated analytical table or KPIs
- ML experiment logged in MLflow
- Functional RAG demonstration
- Standard Data & AI capability demo
- Project README and minimal technical documentation

### Optional
Depending on chosen stretch goals:
- Quality scorecards
- Incremental/streaming ingestion
- Enhanced ML pipeline
- RAG evaluation or guardrails
- Spec-Kit AI-assisted design artefacts
- Expanded dashboard & stakeholder narrative
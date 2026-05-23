# DATA QUALITY MONITORING PIPELINE
## System Design & Implementation Roadmap

---

## HOW TO DESCRIBE THIS PROJECT (To An Interviewer)

**The short version (30 seconds):**
"I built an end-to-end data quality monitoring pipeline. A fake data producer
simulates real e-commerce traffic and intentionally injects bad data — nulls,
spikes, duplicates. That data flows into Kafka, gets picked up by Airflow,
loaded into Postgres, and scanned by a quality check engine. When an anomaly
is detected, I feed it to a local LLM via Ollama, which generates a plain-English
explanation of what went wrong and why. Everything is visible on a Metabase
dashboard. The entire system runs on Docker — zero cloud cost."

**The longer version (if they ask you to go deeper):**
"The core problem I wanted to solve is that raw anomaly logs are useless for
non-technical stakeholders. Telling someone 'null_rate = 0.62' means nothing.
But telling them 'The email column had a 62% null rate between 2am and 3am,
likely due to an upstream form validation failure after a recent deployment'
is immediately actionable.

Architecturally, I kept the components loosely coupled. The producer only knows
about Kafka — it never touches the database. That means I could swap out Postgres
for BigQuery or Redshift and the producer wouldn't change at all. Airflow sits
in the middle as the brain — it handles scheduling, retries, and task dependencies.
The quality check engine is stateless — it reads from Postgres, writes anomalies
back to Postgres, and that's it. The LLM layer is fully async — it runs after
detection, so slow inference never blocks ingestion.

I chose Ollama specifically to avoid OpenAI API costs and to keep sensitive
business data local. llama3:8b runs fast enough on a laptop and the prompt
output is good enough for operational use."

**What this project demonstrates:**
- You understand why data quality matters, not just how to move data
- You can design loosely coupled systems (each component is independently replaceable)
- You know how to integrate an LLM in a way that adds real value, not just decoration
- You can operate production-grade tools: Kafka, Airflow, Postgres, Docker
- You think about failure modes (what happens if Ollama is slow? if Airflow crashes?)

---

## WHAT THIS SYSTEM DOES

Watches a database for bad data (nulls, spikes, duplicates, schema violations),
automatically explains what went wrong in plain English using a local LLM,
and shows everything on a dashboard.

---

## PROJECT FOLDER STRUCTURE

```
data-quality-pipeline/
│
├── docker-compose.yml          # Defines and wires all 6 services together
├── .env                        # Secrets: DB password, Kafka broker URL, etc.
├── README.md                   # Architecture diagram + setup instructions
│
├── postgres/
│   └── init.sql                # Creates all 4 tables on first boot
│
├── producer/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── fake_data_producer.py   # Generates orders + injects anomalies → Kafka
│
├── airflow/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── dags/
│       ├── ingest_dag.py        # DAG 1: Kafka → Postgres (runs every 5 min)
│       └── quality_check_dag.py # DAG 2: Checks + LLM (runs every 10 min)
│
├── quality_engine/
│   └── checks.py               # All check functions (null rate, spikes, dupes, etc.)
│                               # Imported by quality_check_dag.py
│
└── llm_service/
    └── explain.py              # Builds prompt, calls Ollama API, returns summary
                                # Imported by quality_check_dag.py
```

**What each folder is responsible for:**

| Folder           | Responsibility                                                  |
|------------------|-----------------------------------------------------------------|
| postgres/        | Schema only. No business logic lives here.                      |
| producer/        | Data simulation. Knows only about Kafka, never about Postgres.  |
| airflow/dags/    | Orchestration. Calls quality_engine and llm_service as modules. |
| quality_engine/  | Pure check logic. Stateless. Easy to unit test in isolation.    |
| llm_service/     | LLM integration. Single responsibility: prompt in, summary out. |

---

## ARCHITECTURE

```
[Fake Data Producer]
        |
        v
   [Apache Kafka]         → durable message buffer
        |
        v
  [Apache Airflow]        → orchestrates all jobs
     |         |
     v         v
[Postgres]  [Quality Check Engine]
(raw data)       |
                 v
           [Ollama / LLM]   → generates plain-English anomaly summaries
                 |
                 v
        [Postgres: anomaly_reports]
                 |
                 v
        [Metabase Dashboard]
```

---

## SYSTEM DESIGN: KEY DECISIONS

### Why Kafka (not writing directly to Postgres)?
- Decouples producer from storage. If Postgres is slow/down, data is not lost.
- Kafka retains messages on disk. Airflow can replay from any point if it crashes.
- Real systems never let upstream services write directly to a DB — too fragile.
- Enables multiple consumers from one topic without coordination.

### Why Airflow (not a cron job)?
- DAG-based dependency: ingest must finish before quality checks run.
- Built-in retry on failure with configurable delay.
- Web UI shows which exact task failed and why.
- Scales to complex multi-step pipelines.

### Why store anomalies in Postgres (not just log files)?
- Metabase can query structured tables, not log files.
- You can JOIN anomalies with raw data to trace root cause.
- You can trend anomalies over time (are Mondays always worse?).

### Why Ollama (not OpenAI API)?
- Runs entirely on your machine. Zero cost, zero data sent externally.
- llama3:8b is fast enough on a normal laptop.
- Shows you understand LLM integration without vendor lock-in.

---

## POSTGRES SCHEMA (4 tables, know these cold)

| Table              | Purpose                                          |
|--------------------|--------------------------------------------------|
| raw_orders         | All incoming data from Kafka                     |
| anomalies          | Each failed quality check, with severity         |
| anomaly_reports    | LLM-generated explanation for each anomaly       |
| pipeline_runs      | Metadata: when each DAG ran, how many rows, etc  |

---

## PHASES: WHAT TO BUILD

---

### PHASE 1 — INFRASTRUCTURE (Docker)

**Goal:** All services running locally with one command.

**What to create:**
- `docker-compose.yml` defining: Zookeeper, Kafka, Postgres, Airflow, Ollama, Metabase
- `postgres/init.sql` — creates the 4 tables on first boot
- `.env` file for secrets (DB password, Kafka broker URL)

**How to verify it works:**
- Run `docker-compose up -d`
- Check all containers are healthy
- Connect to Postgres and confirm tables exist
- Open Airflow UI at localhost:8080
- Open Metabase at localhost:3000

**Key design point:**
Services depend on each other. Airflow depends on Postgres. Producer depends on Kafka.
Docker Compose `depends_on` handles startup order.

---

### PHASE 2 — FAKE DATA PRODUCER

**Goal:** Simulate a real upstream system sending e-commerce orders into Kafka.

**What to create:**
- `producer/fake_data_producer.py` — runs in a loop, generates order records
- `producer/Dockerfile` — containerizes it

**What the producer does:**
- Generates realistic orders: order_id, customer_email, amount, product, status
- Every Nth batch it intentionally injects anomalies:
  - Null email (simulates form validation failure)
  - Amount spike to $10,000+ (simulates fraud or bug)
  - Duplicate order_id (simulates double-submit)
  - Invalid status value (simulates schema change)
- Publishes each order as a JSON message to Kafka topic: `orders`

**How to verify:**
- Run kafka-console-consumer and watch messages arrive live

**Key design point:**
Producer never touches Postgres directly. It only knows about Kafka.
This is intentional — loose coupling. If you swap out Postgres for BigQuery tomorrow,
the producer doesn't change at all.

---

### PHASE 3 — AIRFLOW DAG 1: INGEST

**Goal:** Pull messages from Kafka and load them into Postgres every 5 minutes.

**What to create:**
- `airflow/dags/ingest_dag.py`

**What the DAG does:**
- Scheduled every 5 minutes
- Connects to Kafka topic `orders` as a consumer group
- Reads all new messages since last run
- Inserts each record into `raw_orders` table
- Records how many rows were loaded (for pipeline_runs table)

**Key design points:**
- Consumer group ID ensures Airflow tracks its own offset — if it restarts, it picks up where it left off
- `auto_offset_reset=earliest` means on first run it reads from the beginning
- This gives you at-least-once delivery (rare duplicates possible — handled in quality checks)

**How to verify:**
- Trigger the DAG manually in Airflow UI
- Query raw_orders in Postgres and confirm rows are there

---

### PHASE 4 — AIRFLOW DAG 2: QUALITY CHECKS

**Goal:** Scan recently ingested data, detect anomalies, write results to DB.

**What to create:**
- `airflow/dags/quality_check_dag.py`
- `quality_engine/checks.py` (the actual check logic, imported by the DAG)

**What checks to implement:**

| Check               | What It Detects                        | Threshold (example)   |
|---------------------|----------------------------------------|-----------------------|
| Null rate           | % of nulls in a required column        | > 10% = flag it       |
| Amount spike        | Average order value way above baseline | > $1000 avg = flag it |
| Duplicate order_id  | Same ID appearing more than once       | Any duplicate = flag  |
| Invalid status      | Status value not in allowed list       | Any invalid = flag    |
| Data freshness      | No new records in last 15 minutes      | Silence = flag it     |

**For each anomaly found:**
- Write one row to the `anomalies` table
- Include: check name, table, column, measured value, threshold, severity, batch ID

**Severity levels:** LOW / MEDIUM / HIGH / CRITICAL
(based on how far the metric exceeds the threshold)

**Key design point:**
DAG 2 runs after DAG 1 using Airflow's task dependency system.
Checks only look at data from the last 10–15 minutes (the latest batch).
This scopes the checks to what just arrived, not the full historical table.

**How to verify:**
- Make the producer inject anomalies
- Run the DAG
- Query the anomalies table and confirm rows appear

---

### PHASE 5 — LLM EXPLANATIONS (Ollama)

**Goal:** For each anomaly, generate a plain-English explanation and save it.

**What to create:**
- `llm_service/explain.py` — wraps the Ollama API call
- Add an LLM task at the end of quality_check_dag.py

**How it works:**
1. After quality checks run, read the anomaly IDs from that batch
2. For each anomaly, build a structured prompt (check name, column, value, severity)
3. POST to Ollama API at `http://ollama:11434/api/generate`
4. Save the response text to `anomaly_reports` table

**Prompt strategy:**
- Tell the model it's a data quality expert
- Give it the anomaly in structured form
- Ask for 2–3 sentences with a likely root cause
- Bounded output = usable in a dashboard card

**Model to use:** `llama3` (pull it once with `ollama pull llama3`)

**Key design point:**
LLM task is the last step — non-blocking for ingestion.
If Ollama is slow, the pipeline keeps running. Reports just arrive a bit later.
Anomaly detection and ingestion are never held up by LLM latency.

**How to verify:**
- curl the Ollama API manually with a test anomaly
- Run the full DAG end-to-end
- Query anomaly_reports and read the summaries

---

### PHASE 6 — METABASE DASHBOARD

**Goal:** A visual UI showing pipeline health, anomalies, and LLM summaries.

**Setup:**
- Connect Metabase to Postgres (host, db, user, password)
- Build 4 queries as dashboard cards:

| Card                        | Query targets               | Chart type  |
|-----------------------------|-----------------------------|-------------|
| Anomalies over time         | anomalies table             | Line chart  |
| Anomalies by severity       | anomalies table             | Pie chart   |
| Recent LLM summaries        | anomaly_reports + anomalies | Table       |
| Data volume over time       | raw_orders table            | Bar chart   |

**No code needed** — Metabase has a point-and-click query builder.
Write the SQL directly in the "native query" editor for the JOINed card.

---

## BUILD ORDER (Week by Week)

### Week 1 — Foundation
- Day 1: docker-compose.yml + init.sql → get all services running
- Day 2: Fake data producer → verify Kafka receives messages
- Day 3: Ingest DAG → verify raw_orders fills up

### Week 2 — Intelligence
- Day 4: Quality check DAG → verify anomalies table gets populated
- Day 5: Ollama setup + LLM task → verify anomaly_reports fills
- Day 6: Metabase → build all 4 dashboard cards

### Week 3 — Polish
- Add more checks (freshness, cross-column, trend-based)
- Write a proper README with architecture diagram
- Record a 2-minute demo of a full anomaly cycle end-to-end
- Push to GitHub

---

## WHAT TO SAY IN AN INTERVIEW

**The one-liner:**
"I built a data quality monitoring pipeline using Kafka for event ingestion,
Airflow for orchestration, Postgres as the storage layer,
and Ollama to generate plain-English anomaly explanations."

**If they ask why Kafka:**
"To decouple producers from consumers. Data is durable even if downstream is slow or down."

**If they ask why Airflow:**
"DAG-based task dependencies, retry logic, and a UI for observability — 
things you lose with a plain cron job."

**If they ask about the LLM part:**
"Raw anomaly metrics are useless to non-technical stakeholders.
The LLM turns 'null_rate=0.62' into a sentence a product manager can act on."

**If they ask what you'd change at scale:**
"Replace LocalExecutor with CeleryExecutor in Airflow for parallel tasks.
Partition the Kafka topic by region or product category.
Move the LLM call to an async worker queue so it doesn't block the DAG."

---

*Focus: understand the "why" behind each component. The code is secondary.*

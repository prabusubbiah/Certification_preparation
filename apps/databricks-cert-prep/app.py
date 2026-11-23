"""Databricks Cert Prep — trainer for the Databricks Data Engineer
Associate and Data Engineer Professional certifications, plus the
Generative AI Engineer Associate certification.

Three areas:
  1. Learn         — flashcards by certification and exam domain
  2. Practice Exams — timed mock exams with scoring and answer review
  3. Tips & Tricks  — exam strategy and common pitfalls

The question bank (workspace.cert_prep.questions / .flashcards) and every
exam attempt (workspace.cert_prep.attempts) live in Unity Catalog, so the
bank keeps growing and results can be analyzed over time.
"""

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

from dash import ALL, Dash, Input, Output, State, callback_context as ctx
from dash import dcc, html, no_update
from flask import request as flask_request

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("cert-prep")

# ----------------------------------------------------------------- config
CATALOG, SCHEMA = "workspace", "cert_prep"
QUESTIONS_TABLE = f"{CATALOG}.{SCHEMA}.questions"
FLASHCARDS_TABLE = f"{CATALOG}.{SCHEMA}.flashcards"
ATTEMPTS_TABLE = f"{CATALOG}.{SCHEMA}.attempts"

LEVELS = [
    {"label": "Data Engineer Associate", "value": "associate"},
    {"label": "Data Engineer Professional", "value": "professional"},
    {"label": "Generative AI Engineer Associate", "value": "genai"},
]
PASS_PCT = 70.0
EXAM_DEFAULTS = {"associate": 45, "professional": 60, "genai": 45}

# ------------------------------------------------------------------ spark
_spark = None


def get_spark():
    """Serverless Spark session via Databricks Connect (app credentials)."""
    global _spark
    if _spark is None:
        from databricks.connect import DatabricksSession
        from databricks.sdk import WorkspaceClient

        try:
            # Apps preconfigures host + client credentials in the environment;
            # databricks-connect requires serverless to be enabled explicitly.
            _spark = DatabricksSession.builder.serverless(True).getOrCreate()
        except Exception:
            _spark = (
                DatabricksSession.builder
                .serverless(True)
                .sdkConfig(WorkspaceClient().config)
                .getOrCreate()
            )
    return _spark


# ------------------------------------------------------------- question bank
_BANK = None


def get_bank(force=False):
    """Load questions + flashcards from Unity Catalog (cached in memory)."""
    global _BANK
    if _BANK is None or force:
        try:
            spark = get_spark()
            questions = []
            for r in spark.table(QUESTIONS_TABLE).collect():
                questions.append(
                    {
                        "id": r["id"],
                        "cert_level": r["cert_level"],
                        "domain": r["domain"],
                        "difficulty": r["difficulty"],
                        "question": r["question"],
                        "options": json.loads(r["options"]),
                        "correct_index": r["correct_index"],
                        "explanation": r["explanation"],
                        "source": r.asDict().get("source"),
                    }
                )
            flashcards = []
            for r in spark.table(FLASHCARDS_TABLE).collect():
                flashcards.append(
                    {
                        "id": r["id"],
                        "cert_level": r["cert_level"],
                        "domain": r["domain"],
                        "front": r["front"],
                        "back": r["back"],
                    }
                )
            _BANK = {"questions": questions, "flashcards": flashcards, "error": None}
        except Exception as e:  # keep the app usable even if UC is unreachable
            log.exception("Failed to load question bank")
            _BANK = {"questions": [], "flashcards": [], "error": str(e)}
    return _BANK


def current_user_email():
    """Identity of the logged-in user, propagated by the Apps frontend."""
    try:
        return (
            flask_request.headers.get("X-Forwarded-Email")
            or flask_request.headers.get("X-Forwarded-Preferred-Username")
            or "unknown"
        )
    except RuntimeError:
        return "unknown"


# ------------------------------------------------------------------- styles
CSS = """
body { font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; background:linear-gradient(180deg,#f7f9fc,#eef2f7) fixed; margin:0; color:#1f2937; font-size:15px; line-height:1.6; -webkit-font-smoothing:antialiased; }
.container { max-width: 1080px; margin: 0 auto; padding: 22px 20px 44px; }
::selection { background:#1a73ba; color:#fff; }
a { color:#1a73ba; }
.hdr { position:relative; overflow:hidden; background:linear-gradient(120deg,#0e1420,#2b3444 55%,#374663); color:#fff; padding:32px 34px; border-radius:16px; border-bottom:4px solid #ff3621; box-shadow:0 6px 24px rgba(16,24,40,.18); }
.hdr::after { content:""; position:absolute; right:-70px; top:-70px; width:260px; height:260px; border-radius:50%; background:radial-gradient(circle,rgba(255,54,33,.25),transparent 70%); pointer-events:none; }
.hdr h1 { margin:0; font-size:28px; letter-spacing:.3px; }
.hdr p { margin:8px 0 0; color:#c7d0dd; max-width:72ch; }
.panel { background:#fff; border:1px solid #e3e8ef; border-radius:16px; padding:24px 26px; margin-top:16px; box-shadow:0 2px 12px rgba(16,24,40,.06); }
.panel h3 { color:#15243b; letter-spacing:.2px; }
.controls { display:flex; gap:16px; flex-wrap:wrap; align-items:flex-end; }
.controls .field { display:flex; flex-direction:column; gap:4px; min-width:280px; flex:1 1 280px; max-width:460px; }
label { font-size:12px; font-weight:600; color:#667085; text-transform:uppercase; letter-spacing:.4px; }
button { cursor:pointer; border:none; border-radius:8px; padding:9px 16px; font-weight:600; font-size:14px; background:#eef1f5; color:#1f2937; }
button:hover { filter:brightness(.96); }
.btn-primary { background:#1a73ba; color:#fff; }
.btn-accent { background:#ff3621; color:#fff; }
.flashcard { background:linear-gradient(180deg,#ffffff,#f8fafc); border:1px solid #e3e8ef; border-radius:12px; padding:28px; min-height:150px; margin:14px 0; font-size:17px; }
.card-front { font-weight:600; }
.card-divider { border-top:2px dashed #d5dbe4; margin:16px 0; }
.card-back { color:#1a73ba; font-weight:600; }
.chip { display:inline-block; background:#e8f1fa; color:#1a73ba; border-radius:999px; padding:3px 12px; font-size:12px; font-weight:600; margin-left:10px; }
.q-count { font-weight:700; }
.exam-top { display:flex; justify-content:space-between; align-items:center; margin-bottom:8px; }
.timer { font-size:24px; font-weight:700; font-variant-numeric:tabular-nums; }
.timer.low { color:#d92d20; }
.q-text { font-size:17px; font-weight:600; margin:10px 0 14px; }
.nav-btn { min-width:38px; padding:6px 8px; background:#eef1f5; }
.nav-btn.answered { background:#1a73ba; color:#fff; }
.nav-btn.current { outline:3px solid #ffb5aa; }
.q-grid { display:flex; flex-wrap:wrap; gap:6px; margin-top:14px; }
table.domains { border-collapse:collapse; width:100%; margin-top:8px; }
table.domains th, table.domains td { padding:8px 10px; text-align:left; border-bottom:1px solid #e3e8ef; font-size:14px; }
.score { font-size:44px; font-weight:800; }
.badge { display:inline-block; border-radius:999px; padding:6px 18px; font-weight:700; }
.badge.pass { background:#e6f4ea; color:#1e7e34; }
.badge.fail { background:#fee4e2; color:#b42318; }
.review-item { border:1px solid #e3e8ef; border-radius:10px; padding:14px 16px; margin-bottom:12px; }
.review-item .ok { color:#1e7e34; font-weight:700; }
.review-item .ko { color:#b42318; font-weight:700; }
.explain { background:#f8fafc; border-left:4px solid #1a73ba; padding:10px 14px; margin-top:10px; border-radius:0 8px 8px 0; font-size:14px; }
.tips h3 { margin-top:26px; }
.muted { color:#667085; font-size:14px; }
.upload-zone { border:2px dashed #b6c2d2; border-radius:12px; padding:22px; text-align:center; color:#667085; background:#f8fafc; }
.upload-zone:hover { border-color:#1a73ba; color:#1a73ba; }
.upload-status { font-weight:600; margin-top:10px; }

/* ------------- learn lesson card, typography & progress ------------- */
.lesson-card { background:#fff; border:1px solid #e3e8ef; border-left:6px solid #1a73ba; border-radius:14px; padding:26px 30px; margin-top:16px; box-shadow:0 2px 10px rgba(16,24,40,.06); animation:rise .25s ease; }
@keyframes rise { from { opacity:.25; transform:translateY(6px); } to { opacity:1; transform:none; } }
.lesson-card h3 { margin:10px 0 6px; font-size:22px; color:#15243b; }
.markdown { font-size:15.5px; line-height:1.75; color:#2b3a4e; }
.markdown p { margin:10px 0; }
.markdown strong { color:#15243b; }
.markdown code { background:#eef2f8; color:#1a4d8f; padding:1px 6px; border-radius:6px; font-size:13.5px; }
.markdown pre { background:#101b2d; border-radius:10px; padding:14px 18px; overflow-x:auto; margin:12px 0; }
.markdown pre code { background:transparent; color:#e8eef7; padding:0; }
.markdown blockquote { border-left:4px solid #ff3621; background:#fff5f3; padding:10px 16px; border-radius:0 10px 10px 0; margin:12px 0; color:#4a3230; }
.markdown blockquote p { margin:0; }
.markdown table { border-collapse:collapse; margin:12px 0; }
.markdown th, .markdown td { border:1px solid #e3e8ef; padding:7px 12px; font-size:14px; text-align:left; }
.markdown th { background:#eef2f8; }
.progress-wrap { margin-top:16px; display:flex; flex-direction:column; gap:7px; }
.bar { height:8px; background:#e6ebf3; border-radius:999px; overflow:hidden; }
.bar-fill { height:100%; background:linear-gradient(90deg,#1a73ba,#ff3621); border-radius:999px; transition:width .35s ease; }
button { transition:all .15s ease; }
button:hover { transform:translateY(-1px); box-shadow:0 3px 10px rgba(16,24,40,.14); filter:none; }

/* ---------------- tab bar ---------------- */
.tabbar { display:flex; gap:4px; margin-top:16px; padding:6px; background:#fff; border:1px solid #e3e8ef; border-radius:14px; box-shadow:0 1px 4px rgba(16,24,40,.06); }
.tab-item { padding:10px 18px; border-radius:9px; font-weight:600; font-size:14.5px; color:#667085; cursor:pointer; transition:all .15s ease; user-select:none; }
.tab-item:hover { color:#15243b; background:#eef2f8; }
.tab-item--selected { background:linear-gradient(120deg,#1a73ba,#28344a); color:#fff; box-shadow:0 2px 8px rgba(26,115,186,.35); }

/* ---------------- exam view polish ---------------- */
.q-text { font-size:17px; font-weight:600; margin:12px 0 16px; line-height:1.65; }
.nav-btn { min-width:38px; padding:6px 8px; background:#eef1f5; border-radius:9px; font-size:13px; }
.nav-btn.answered { background:#1a73ba; color:#fff; }
.nav-btn.current { outline:3px solid #ffb5aa; box-shadow:0 0 0 4px rgba(255,181,170,.35); }
.timer { font-size:26px; font-weight:700; font-variant-numeric:tabular-nums; letter-spacing:.5px; }
.timer.paused { opacity:.6; }
table.domains th { background:#eef2f8; font-weight:700; }
.score { font-size:48px; font-weight:800; background:linear-gradient(120deg,#1a73ba,#15243b); -webkit-background-clip:text; background-clip:text; -webkit-text-fill-color:transparent; }
.review-item { border:1px solid #e3e8ef; border-radius:12px; padding:16px 18px; margin-bottom:14px; box-shadow:0 1px 4px rgba(16,24,40,.04); }
.flashcard { background:linear-gradient(180deg,#ffffff,#f8fafc); border:1px solid #e3e8ef; border-radius:14px; padding:30px; min-height:150px; margin:16px 0; font-size:17px; box-shadow:0 2px 8px rgba(16,24,40,.05); }
.tips h3 { margin-top:28px; padding-left:12px; border-left:4px solid #ff3621; }
label { font-size:11.5px; font-weight:700; color:#667085; text-transform:uppercase; letter-spacing:.6px; }
::-webkit-scrollbar { width:10px; height:10px; }
::-webkit-scrollbar-thumb { background:#c9d3e0; border-radius:8px; border:2px solid #f4f6fa; }
"""

# --------------------------------------------------------- AI PREP GUIDE
# Full content of the official "AI Prep Guide: Databricks Certification"
# (databricks.com), with the Step 4 "Orient" learning concepts and the Step 5
# hands-on checklist executed for the Data Engineer Associate and
# Professional certifications. Step 2's prime prompt and Step 4's loop
# prompts are quoted from the official guide.

PRIME_PROMPT = (
    "I am preparing for the Databricks certification described in the attached "
    "exam guide. When teaching me, use only current Databricks product names "
    "and behaviors, and do not invent objectives that aren't in the guide. "
    "Ground everything you teach in official Databricks sources — "
    "docs.databricks.com and Databricks Academy — not third-party blogs, "
    "tutorials, or forum posts. If you can browse the web, search "
    "docs.databricks.com and cite the doc page URL for each key claim. If you "
    "cannot confirm a claim in the official docs — or cannot browse at all — "
    "tell me so explicitly rather than guessing, and point me to where in the "
    "docs I should verify it myself. If you cannot accept PDF uploads, tell me "
    "and I will paste the guide's \"Sections and Objectives\" as text instead."
)

STEP1 = """**You need:**

* A **registered exam date** — book at webassessor.com/databricks (your deadline; used in Step 6)
* Your **exam guide PDF** — download from the certification page on databricks.com; re-check it two weeks before your exam
* An **LLM** — ChatGPT, Claude, Copilot, Genie Code, Gemini, or any other tool you have access to (the free tier is enough)
* A **Databricks account** — Free Edition or a Trial; most exams cannot be passed by reading alone, you need hands-on practice
* **Databricks Academy** — the official free learning platform with on-demand courses and learning paths (academy.databricks.com)
* A **study journal** — a doc or plain text file where you track weak spots and unfamiliar terms"""

STEP3 = """Every major AI was trained before the recent Databricks platform changes, so it confidently uses old product names. Run the guide's prompt once, save the table, and paste it at the start of every session. Key renames for the Data Engineer exams:

| Old name an AI might use | Current name | What it does |
| --- | --- | --- |
| Delta Live Tables (DLT) | Lakeflow Spark Declarative Pipelines (SDP) | Declarative pipelines: streaming tables, materialized views, expectations |
| Workflows | Lakeflow Jobs | Orchestration: tasks, dependencies, schedules, run-if |
| Databricks Asset Bundles (DABs) | Declarative Automation Bundles | Deploy jobs, pipelines, dashboards as code with dev/prod targets |
| Partner Connect | Lakeflow Connect | Managed connectors to SaaS sources (Salesforce, Workday, …) |
| MLflow Model Registry | Unity Catalog models | Model lifecycle governance in Unity Catalog |
| Model Serving | Mosaic AI Model Serving | Host models on REST endpoints |
| Genie Space | Genie Agent | Natural-language Q&A over governed data |
| SQL endpoint | SQL warehouse | Serverless SQL compute for queries and BI |

*Verify any rename you are unsure about at docs.databricks.com — that is exactly what the guide asks your AI to do.*"""

STEP4_LOOP = """For each section in your exam guide, paste these prompts in order:

| # | Step | Prompt (paste, fill in the bracket) |
| --- | --- | --- |
| 1 | Orient | "Give me a plain-English overview of the objectives from [SECTION] of the exam guide I shared. One sentence per objective." |
| 2 | Diagnose | "Quiz me with 10 multiple-choice questions on [SECTION], in the style of an actual Databricks certification exam. Don't show answers until I respond." |
| 3 | Deep dive | "Teach me [SPECIFIC OBJECTIVE I MISSED, COPY-PASTED VERBATIM FROM THE EXAM GUIDE], using only official Databricks documentation (docs.databricks.com) and Databricks Academy materials as your sources — not third-party blogs or forums. If you can browse, cite the exact doc page URL next to each claim; if you can't verify something in the official docs, say so rather than guessing. Include: core concept, how it works on Databricks, when to use vs. alternatives, common mistakes, one code example I can run." |
| 4 | Practice | "Generate a full mock exam matching the number of scored questions specified in the exam guide, distributed across the objectives in the proportions the guide implies. Don't reveal answers until I submit all responses." |
| 5 | Repair | "Here are the questions I got wrong: [PASTE]. For each: explain my misconception, explain why the right answer is right, and give me a related question that tests the same concept differently." |

Step 1 (Orient) is executed for you below — the learning concepts for every exam section of your certification. For Steps 2 and 4, use this app's **Practice Exams** tab; for Steps 3 and 5, read every wrong answer's explanation in the results review."""

CONCEPTS = {
    "associate": """### Orient — learning concepts by exam section (Associate)

#### Databricks Intelligence Platform (6%)
* Workspace anatomy: notebooks, queries, AI/BI dashboards, and Git folders — notebooks sync to Git as standard `.ipynb` files
* Compute types: all-purpose clusters (shared, interactive), job clusters (created per run, isolated, auto-terminated), serverless compute, and SQL warehouses

#### Data Ingestion and Loading (21%)
* Auto Loader (`cloudFiles` format) ingests new files incrementally and scales to millions of files, with schema evolution and file-notification modes
* COPY INTO is the simpler, idempotent, reprocessable load — best for smaller file sets
* Delta Lake = Parquet files + the `_delta_log` transaction log, giving ACID commits and serializable writes
* MERGE INTO is the key-based upsert (CDC) pattern; Delta adds ACID, schema enforcement, and time travel over plain Parquet

#### Data Transformation and Modeling (22%)
* Lazy evaluation: transformations build a plan, actions (count, collect, write) trigger it
* Narrow vs wide transformations — a shuffle is the expensive part; prefer built-ins over Python UDFs (Catalyst-optimized, no serialization)
* DataFrames, `createOrReplaceTempView`, `spark.sql`, and window functions (running totals, rankings)
* Medallion architecture: bronze (raw, as-is) → silver (validated, deduplicated, conformed) → gold (aggregated business models)
* Time travel: `VERSION AS OF` / `TIMESTAMP AS OF`, `DESCRIBE HISTORY`, `RESTORE TABLE`

#### Working with Lakeflow Jobs (16%)
* Jobs are tasks with dependencies; "run if" conditions trigger a task on upstream failure (alerting and error handling)
* Job clusters vs all-purpose clusters; schedules and trigger types

#### Implementing CI/CD (10%)
* Git folders: clone a repo, work on a branch, push, open a pull request
* Declarative Automation Bundles: resources defined as code with environment targets (dev → prod)

#### Troubleshooting, Monitoring, and Optimization (10%)
* OPTIMIZE compacts small files; ZORDER co-locates similar values for data skipping; VACUUM reclaims storage past retention
* Query profile and `df.explain()` to find slow stages, shuffles, and spills

#### Governance and Security (15%)
* Unity Catalog: catalog → schema → table, one permission model across workspaces
* Grant chain: USE CATALOG → USE SCHEMA → SELECT
* Managed tables (UC controls storage; DROP deletes data) vs external tables (data survives DROP); lineage is captured automatically

""",
    "professional": """### Orient — learning concepts by exam section (Professional)

#### Developing Code for Data Processing using Python and SQL (22%)
* Idiomatic Spark: window functions, `when`/`otherwise` branching, higher-order functions; built-ins over UDFs
* Streaming code: `withWatermark` bounds late data so state can be evicted; `trigger(availableNow=True)` drains all available data then stops (incremental backfills)
* `foreachBatch` applies custom logic per micro-batch (non-Delta sinks, idempotent MERGE); checkpoint location = resumability + effectively-once into Delta

#### Data Ingestion & Acquisition (7%)
* Change data feed: `table_changes()` returns row-level inserts/updates/deletes
* Streaming tables (incremental, append-only) vs materialized views (fully recomputed on refresh) in Lakeflow pipelines
* Append flows combine multiple sources or one-time backfills into one streaming table; Auto Loader is the file-ingestion pattern

#### Data Transformation, Cleansing, and Quality (10%)
* Expectations: `expect` keeps bad records and reports metrics; `expect_or_drop` removes them
* Constraints: NOT NULL and CHECK enforced on write
* Generated columns (`GENERATED ALWAYS AS`) compute values at write time — clustering/partitioning on derived values

#### Data Sharing and Federation (5%)
* Delta Sharing: shares, providers, and recipients — live governed data without copying
* Foreign catalogs federate external databases through standard connections

#### Monitoring and Alerting (10%)
* System tables: query history, lineage, audit; job notifications and alerting
* Predictive optimization runs OPTIMIZE/VACUUM automatically on managed tables

#### Cost & Performance Optimisation (13%)
* AQE re-optimizes at runtime: broadcast join conversion, shuffle partition coalescing, skew handling
* Broadcast small dimension tables; salt hot keys; cache only what you reuse
* Deletion vectors make DML cheap; liquid clustering (`CLUSTER BY`) replaces static partitioning/ZORDER

#### Ensuring Data Security and Compliance (10%)
* Row filters (boolean function) and column masks (masking function) with caller context (`is_account_group_member`)
* Service principals and OAuth for non-interactive access

#### Data Governance (7%)
* Automatic lineage; ownership and grants; tags for classification and PII

#### Debugging and Deploying (10%)
* Query profile for plans and stage timings; reading `EXPLAIN` output
* Declarative Automation Bundle targets: same bundle, per-environment values, `bundle deploy --target prod`

""",
    "genai": """### Orient — learning concepts by exam section (Generative AI Engineer)

#### Generative AI Concepts (25%)
* LLM fundamentals: tokens, context windows, temperature; embeddings turn text into vectors for similarity
* Transformers predict the next token; hallucination, grounding, and prompt roles (system / user / developer)
* The RAG pattern: retrieve relevant context, then generate; chunking strategies and their trade-offs
* Choosing the approach: prompt vs RAG vs fine-tune vs agent

#### The Databricks AI Platform (10%)
* Model Serving endpoints: foundation models, external models, custom models
* AI Playground and Agent Builder: from no-code exploration to low-code agents
* AI functions in SQL (`ai_query`); MLflow for LLM applications

#### Mosaic AI Vector Search (15%)
* Vector Search endpoints and the two index types: Delta table sync vs direct vector index
* Embedding models and the managed embedding pipeline; continuous vs triggered sync
* Hybrid search (keyword + vector), similarity metrics, and metadata filters

#### Mosaic AI Agent Framework (20%)
* Agent Builder (no-code) vs code-first agents (Mosaic AI Agent Framework / LangChain)
* Driver agents and tools: UC functions, endpoints, parameterized SQL, Genie spaces
* Structured outputs and review app; debugging agents in the playground

#### Mosaic AI Agent Evaluation (10%)
* Evaluation sets: requests + expected responses, expected facts, and guidelines
* Deterministic metrics (retrieval precision / recall) vs LLM-as-judge (correctness, groundedness)
* MLflow tracing: every step of every agent run; exporting and comparing evaluations

#### Fine-tuning and Custom Models (10%)
* Approaches: continued pretraining, supervised fine-tuning, preference optimization
* Data prep, training runs, registering and deploying the custom model
* When fine-tuning actually helps (domain language, strict format compliance) vs RAG

#### AI Governance and Deployment (10%)
* Mosaic AI Gateway: rate limits, routing, guardrails, usage tracking
* Unity Catalog governance for agents, functions, and models
* Secrets and PII handling; external models and BYO endpoints

""",
}

HANDSON = {
    "associate": """Almost every Databricks certification assesses practical skills, not just factual recall. Build every item below in your workspace (Free Edition or a free trial is enough; each task takes under 30 minutes):

1. Upload a CSV into a Unity Catalog volume and read it with Spark — *Databricks Intelligence Platform*
2. Create a bronze streaming table from cloud files using Auto Loader — *Data Ingestion and Loading*
3. Build a silver table with SQL: join, dedup, and a window function — *Data Transformation and Modeling*
4. MERGE a change file into a Delta table on a key (upsert) — *Data Ingestion and Loading*
5. Query `VERSION AS OF`, run `DESCRIBE HISTORY`, then `RESTORE TABLE` — *Data Transformation and Modeling*
6. Run OPTIMIZE and VACUUM on a Delta table — *Troubleshooting, Monitoring, and Optimization*
7. Build a two-task Lakeflow Job with a run-if failure path and a schedule — *Working with Lakeflow Jobs*
8. Grant a group the USE CATALOG → USE SCHEMA → SELECT chain and verify access — *Governance and Security*
9. Clone a repo in a Git folder and make a change on a branch — *Implementing CI/CD*

Any task you can't complete fluently is your next study target.""",
    "professional": """Build every item below in your workspace (Free Edition or a free trial is enough; each task takes under 30 minutes):

1. Build a pipeline with expectations (`expect` + `expect_or_drop`) and inspect the metrics — *Data Transformation, Cleansing, and Quality*
2. Create one streaming table and one materialized view in the same pipeline — *Data Ingestion & Acquisition*
3. Run a streaming aggregation with a watermark on rate data — *Developing Code using Python and SQL*
4. Backfill incrementally with `trigger(availableNow=True)` and a durable checkpoint — *Developing Code using Python and SQL*
5. Read changes with `table_changes()` after an UPDATE — *Data Ingestion & Acquisition*
6. Apply a row filter and a column mask using `is_account_group_member` — *Ensuring Data Security and Compliance*
7. Create a table with liquid clustering + deletion vectors and compare MERGE cost — *Cost & Performance Optimisation*
8. Initialize and deploy a bundle with `--target dev` — *Debugging and Deploying*
9. Compare a shuffle join vs a broadcast join plan with `explain()` — *Cost & Performance Optimisation*
10. Share a table with an internal recipient via Delta Sharing — *Data Sharing and Federation*

Any task you can't complete fluently is your next study target.""",
    "genai": """Build every item below in your workspace (Free Edition or a free trial is enough; each task takes under 30 minutes):

1. Open the AI Playground, chat with a foundation model, and change the system prompt + temperature — *Generative AI Concepts*
2. Create a Model Serving endpoint for a foundation model and call it with a REST request — *The Databricks AI Platform*
3. Call an AI function from SQL (`ai_query`) against a small table — *The Databricks AI Platform*
4. Create a Vector Search endpoint and a vector index over a Delta table; run a similarity query with a filter — *Mosaic AI Vector Search*
5. Build a RAG chatbot over a few documents in a volume (Knowledge Assistant or Agent Builder) — *Mosaic AI Agent Framework*
6. Create an agent with a UC function tool and inspect its steps — *Mosaic AI Agent Framework*
7. Run an evaluation with an AI judge on a small evaluation set — *Mosaic AI Agent Evaluation*
8. Review MLflow traces for an agent run and export the results — *Mosaic AI Agent Evaluation*
9. Check AI Gateway rate limits and usage tracking for an endpoint — *AI Governance and Deployment*
10. Deploy an agent or assistant to a Model Serving endpoint — *AI Governance and Deployment*

Any task you can't complete fluently is your next study target.""",
}

PACE = """* Book the exam date first, then work backward in **2-week sprints** (more time → extra sprints on the middle row)
* **First quarter** of your time: Diagnose every section; identify your weakest 2–3
* **Middle half**: deep-dive into weak objectives and build the hands-on labs
* **Final quarter**: full mock exams (Step 4 / this app's Practice Exams tab) to surface weak spots and build stamina — they are not a calibrated readiness check; your real signal is full coverage of the exam guide objectives plus hands-on confidence

**Short on time? The quick five:** prime your AI every session with the exam guide + renamed-product table · diagnose before you study · run every code snippet and verify at docs.databricks.com (no URL means it's guessing) · cross-check key topics with a second AI · build the hands-on list — reading alone will not pass any Databricks certification."""


# Deep-dive concept lessons, keyed by (cert_level, domain). Each lesson has a
# detailed explanation, a runnable example, and an exam-style question with the
# correct answer. Other domains fall back to their guide concepts below.
LESSONS = {
    (
        "associate",
        "Data Transformation and Modeling (22%)",
    ): [
        {
            "title": "Concept 1 — Lazy evaluation: transformations vs actions",
            "section": "Data Transformation and Modeling (22%)",
            "body": """**Why it matters for the exam:** the single most-tested Spark idea. Spark does nothing when you transform data — it only builds a logical plan. **Transformations** (`select`, `filter`, `withColumn`, `groupBy`, `join`, `dropDuplicates`) are *lazy*: they add steps to the plan. **Actions** (`count`, `collect`, `show`, `take`, `.write`, `toPandas`, `first`) submit a job and actually run the plan. Because the full plan is known before execution, Catalyst optimizes the whole query — predicate pushdown, column pruning, and even skipping files that can't match a filter.

**Simple example**
```python
df = spark.read.csv("s3://bucket/sales.csv")                 # lazy
clean = df.filter("amount > 0").select("user", "amount")     # still lazy
clean.count()   # ACTION — only now does Spark read and compute
```

**How the exam asks it**
> A data engineer runs `df.filter(df.amount > 0).select("user")`. No job appears in the Spark UI. What is the most likely reason?

✅ **`filter` and `select` are lazy transformations — Spark executes only when an action such as `count()` or `write()` is called.** Trap options: "the cluster is under-provisioned", "the command failed silently", "results are served from cache".

Bonus: the chain of transformations is the **lineage**; it lets Spark recompute any lost partition — laziness is also what enables fault tolerance.""",
        },
        {
            "title": "Concept 2 — Narrow vs wide transformations (shuffles)",
            "section": "Data Transformation and Modeling (22%)",
            "body": """**Why it matters for the exam:** know which operations move data between partitions — a **shuffle** writes partitions to disk and sends them over the network. **Narrow** transformations (`filter`, `select`, `withColumn`, `map`) use only the data already in each output partition's input — no shuffle, very fast. **Wide** transformations (`groupBy`, `join`, `orderBy`, `distinct`, `repartition`, window functions) require data with the same key to be brought together — a shuffle, the number-one cost in Spark jobs.

**Simple example**
```python
s = df.filter("amount > 0")           # narrow — free
s = s.withColumn("vat", F.col("amount") * 0.2)   # narrow — free
agg = s.groupBy("country").sum("amount")         # WIDE — shuffle by country
```

**How the exam asks it**
> A query is slow. The Spark UI shows a stage with a long *shuffle read*. Which operation most likely caused it?

✅ **A wide transformation such as `groupBy`, `join`, or `orderBy`** — they must exchange rows across partitions. Also asked directly: "Which TWO operations trigger a shuffle?" (answers: `groupBy`, `join`, `distinct`, `orderBy` — not `filter`/`select`/`withColumn`).

Exam habit: filter and select columns **early** (before joins), and keep join/group keys aligned with the table's clustering/partitioning to shrink shuffles.""",
        },
        {
            "title": "Concept 3 — Built-in functions vs Python UDFs",
            "section": "Data Transformation and Modeling (22%)",
            "body": """**Why it matters for the exam:** a favorite scenario question. **Built-in functions** (`pyspark.sql.functions`: `upper`, `coalesce`, `when`, `date_add`, `regexp_replace`, aggregations, `split`…) run inside the JVM and are understood and optimized by Catalyst. A **Python UDF** serializes every row to a Python worker process and back (pickling) — the optimizer can't look inside it, so it blocks pushdown/pruning and is dramatically slower. Rule: always prefer a built-in; if you must use Python, use a **pandas UDF** (Arrow-batched, vectorized) rather than a row-at-a-time UDF.

**Simple example**
```python
# GOOD — built-in, optimized
df = df.withColumn("name_clean", F.upper(F.trim(F.col("name"))))

# SLOW — Python UDF, row-by-row serialization
@F.udf("string")
def upper_udf(x): return x.strip().upper()
df = df.withColumn("name_clean", upper_udf(F.col("name")))
```

**How the exam asks it**
> A job using a Python UDF to format a column takes far longer than expected. What should the data engineer do to improve performance?

✅ **Replace the UDF with the equivalent built-in Spark SQL function** (or a pandas UDF if no built-in exists). Trap options: "increase the cluster size", "cache the DataFrame", "convert to RDD". UDFs also break predicate pushdown — a filter applied *after* a UDF column can no longer be pushed into the read.""",
        },
        {
            "title": "Concept 4 — DataFrames and Spark SQL work together (temp views)",
            "section": "Data Transformation and Modeling (22%)",
            "body": """**Why it matters for the exam:** you must know the two APIs are interchangeable — same engine, same optimizer, same results. `df.createOrReplaceTempView("v")` registers a **session-scoped** view (no data is copied or materialized, it's just a name for the lazy plan). Then `spark.sql("SELECT … FROM v …")` queries it like a table. The view disappears when the session/cluster ends; it is *not* persisted to storage. Also know: SQL and DataFrame code can be mixed freely, and both stay lazy until an action.

**Simple example**
```python
df.createOrReplaceTempView("orders")
top = spark.sql(
    "SELECT user_id, SUM(amount) AS total "
    "FROM orders GROUP BY user_id ORDER BY total DESC LIMIT 10"
)
top.show()   # action
```

**How the exam asks it**
> A data engineer has a DataFrame `df` and wants to query it using SQL in the same notebook. What should they do?

✅ **`df.createOrReplaceTempView("orders")`, then run `spark.sql("SELECT …")`**. Follow-ups: "How long does a temp view exist?" → for the lifetime of the Spark session; "Does it store a copy of the data?" → no, it's a pointer to the plan.""",
        },
        {
            "title": "Concept 5 — Window functions (rank, running totals, lag)",
            "section": "Data Transformation and Modeling (22%)",
            "body": """**Why it matters for the exam:** classic "how would you compute X" questions. A window function computes a value per **row** using related rows, *without collapsing* them the way `groupBy` does. Three parts: `partitionBy` (the window group — e.g. per customer), `orderBy` (row order inside the window), and the frame (`rowsBetween`/`rangeBetween`, default for rank-like functions: all rows up to the current one). Typical uses: `row_number`, `rank`, `dense_rank` (top-N per group), running totals, moving averages, `lag`/`lead` (compare to the previous row).

**Simple example**
```python
w = Window.partitionBy("customer_id").orderBy("ts")
s = (df.withColumn("rn", F.row_number().over(w))
      .withColumn("running", F.sum("amount").over(w.rowsBetween(Window.unboundedPreceding, 0))))
```

**How the exam asks it**
> Which approach computes a *running total per customer* while keeping every individual order row?

✅ **A window function: `SUM(amount) OVER (PARTITION BY customer_id ORDER BY ts)`** — `groupBy`+`sum` would collapse rows to one per customer, losing detail. Also asked: dedup by keeping `row_number() = 1` (see Concept 8), or `lag` for month-over-month change. Note: window functions are **wide** — they shuffle by the partition key.""",
        },
        {
            "title": "Concept 6 — Medallion architecture: bronze, silver, gold",
            "section": "Data Transformation and Modeling (22%)",
            "body": """**Why it matters for the exam:** Databricks' signature data design; expect 2–3 questions. Data flows through layers of increasing quality. **Bronze** = raw, as-is ingestion — keep the source shape, append-only, replayable (you can always rebuild downstream from bronze). **Silver** = cleaned, validated, deduplicated, conformed — schemas enforced, records joined to reference data, one row per business entity (via MERGE/upsert). **Gold** = business-level aggregates and metrics — the tables dashboards and ML consume. Each layer adds quality and speed for its consumers; you reprocess rather than fix in place.

**Simple example**
```sql
-- silver: dedup + validate
CREATE OR REPLACE TABLE silver_orders AS
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY updated_at DESC) rn
  FROM bronze_orders)
WHERE rn = 1 AND amount > 0;

-- gold: business aggregate
CREATE OR REPLACE TABLE gold_daily_sales AS
SELECT order_date, region, SUM(amount) AS sales FROM silver_orders GROUP BY 1, 2;
```

**How the exam asks it**
> Raw CDC data lands unmodified in which layer? ✅ **Bronze.** Where should duplicate records be removed and schemas enforced? ✅ **Silver.** Which layer do BI dashboards typically query? ✅ **Gold.** Trap: "dedup happens in bronze" or "gold stores raw data for replay" — no, bronze does.""",
        },
        {
            "title": "Concept 7 — Time travel: VERSION AS OF, DESCRIBE HISTORY, RESTORE",
            "section": "Data Transformation and Modeling (22%)",
            "body": """**Why it matters for the exam:** every write to a Delta table creates a new **version** in the transaction log. You can query the past with **time travel**: `SELECT * FROM t VERSION AS OF 3` or `TIMESTAMP AS OF '2026-11-01'` — great for debugging and comparing before/after a bad job. `DESCRIBE HISTORY t` lists every version, operation, and timestamp. `RESTORE TABLE t TO VERSION AS OF 3` makes an old version the *current* one (it doesn't delete history; it adds a restore entry). Limits: `VACUUM` physically removes files older than the retention period (default 7 days) — after vacuum you cannot time travel past what was removed.

**Simple example**
```sql
DESCRIBE HISTORY sales;                                -- find the last good version
SELECT * FROM sales VERSION AS OF 12;                 -- inspect it
RESTORE TABLE sales TO VERSION AS OF 12;              -- roll back
```

**How the exam asks it**
> A job accidentally overwrote yesterday's data in a Delta table. What is the fastest way to recover it?

✅ **`RESTORE TABLE … TO VERSION AS OF` the previous version** — no re-ingestion needed. Trap options: "re-run the ingestion pipeline", "use MERGE to reinsert" (slow, unnecessary). Follow-up: "what limits how far back you can query?" → VACUUM / retention period.""",
        },
        {
            "title": "Concept 8 — Deduplication and join patterns (silver-table classics)",
            "section": "Data Transformation and Modeling (22%)",
            "body": """**Why it matters for the exam:** the standard "keep the latest row per key" pattern appears in some form on nearly every Data Engineer exam. CDC and re-ingestion deliver the same business row many times; silver tables must keep **one row per key, the newest version**. The canonical pattern: `ROW_NUMBER() OVER (PARTITION BY key ORDER BY updated_at DESC)` then keep `rn = 1`. Know also `dropDuplicates(["key"])` (nondeterministic *which* duplicate survives — the window version is explicit) and `MERGE INTO … WHEN MATCHED THEN UPDATE` for incremental upserts into an existing silver table.

**Simple example**
```sql
CREATE OR REPLACE TABLE silver_customers AS
SELECT * EXCEPT (rn) FROM (
  SELECT c.*,
         ROW_NUMBER() OVER (PARTITION BY customer_id
                            ORDER BY updated_at DESC) AS rn
  FROM bronze_customers) WHERE rn = 1;
```

**How the exam asks it**
> Bronze contains multiple versions of each customer. Which approach produces one row per customer, keeping the most recent record for each?

✅ **A window function ranking by `updated_at DESC per customer_id` and filtering rank = 1.** Trap: plain `dropDuplicates()` — it keeps *an arbitrary* row unless you rank it yourself; also `groupBy(customer_id)` without an aggregation collapses data instead of selecting the latest row. Pair this with MERGE when the silver table must be updated incrementally rather than rebuilt.""",
        },
        {
            "title": "Concept 9 — Complex scenario: CDC upserts with MERGE (and the multi-match failure)",
            "section": "Data Transformation and Modeling (22%)",
            "body": """**Why it matters for the exam:** Think of MERGE as a bouncer matching incoming rows to existing rows by a key — one seat per person. If the source has three changes for the same customer, MERGE refuses to guess which one wins, and the whole load fails. Fix it before MERGE sees the data: keep only the newest change per key (rank changes by their sequence number, keep rank 1), then MERGE.

**Worked example (scenario)**
```python
from pyspark.sql import functions as F, Window
w = Window.partitionBy("customer_id").orderBy(F.col("_seq").desc())
latest = (spark.table("bronze.cdc")
          .withColumn("rn", F.row_number().over(w))
          .filter("rn = 1").drop("rn"))
(latest.alias("s")
 .merge(spark.table("silver.customers").alias("t"),
        "t.customer_id = s.customer_id")
 .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
```

**How the exam asks it**
> A MERGE fails with multiple source rows matched on the same target row. The feed contains several updates per key per batch and only the latest should win.

✅ **Before merging, keep only the newest change per customer — rank the changes by sequence and keep rank 1.** Traps: "replace the whole table" (wipes other rows), "randomly keep one row" (may keep the wrong one), "add more memory" (the problem is duplicate keys, not memory).""",
        },
        {
            "title": "Concept 10 — Complex scenario: SCD Type 2 history that still answers current-state queries",
            "section": "Data Transformation and Modeling (22%)",
            "body": """**Why it matters for the exam:** Most tables keep only the latest truth: update a customer's city and the old value is gone forever. Sometimes the exam needs both — today's value AND what it was last month. The trick is to never overwrite: add a new row for each change, stamped with start and end dates. That is SCD Type 2. "Current" simply means the row with no end date yet.

**Worked example (scenario)**
```python
dlt.apply_changes(
    target="dim_customers",
    source="cdc_customers",
    keys=["customer_id"],
    sequence_by=F.col("_seq"),
    stored_as_scd_type=2,     # full history with start/end dates
)
# current state = filter __END_AT IS NULL
```

**How the exam asks it**
> A dimension must preserve every historical version of each customer while still serving current-state queries.

✅ **SCD Type 2 — every change gets its own row with start/end dates, so old versions survive.** Traps: "update rows in place" (old values are lost), "reload daily and keep a copy of yesterday", "keep history in plain Parquet files next to the table".""",
        },
        {
            "title": "Concept 11 — Complex scenario: data skew — the one-task straggler",
            "section": "Data Transformation and Modeling (22%)",
            "body": """**Why it matters for the exam:** Picture 200 workers, each given one box to carry. 199 boxes are light, but one weighs as much as all the rest combined — and the job only finishes when the slowest worker does. That is skew: too many rows share one key value (often a flood of NULLs). Fix: carry the heavy box separately (filter those rows out first), or let Spark (AQE) automatically split it into smaller ones.

**Worked example (scenario)**
```python
facts = facts.filter("cust_id IS NOT NULL")   # stop NULLs skewing one task
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
joined = facts.join(broadcast(dims), "cust_id")
```

**How the exam asks it**
> 199 tasks finish in about 2 minutes, one runs 40 minutes, and 70% of rows share one customer_id.

✅ **Deal with the crowded key before the shuffle — pull those rows out and handle them separately, or let Spark (AQE) split them automatically.** Traps: "make the cluster 10x bigger", "shrink everything to one file", "turn off the Spark UI to save memory".""",
        },
        {
            "title": "Concept 12 — Complex scenario: window functions for running totals and latest-row-per-key",
            "section": "Data Transformation and Modeling (22%)",
            "body": """**Why it matters for the exam:** Two everyday needs show up in gold tables: a running total (sales so far this month) and the newest row per customer. GROUP BY can't do either — it squashes rows into one. A window function instead walks row by row inside each group: it can add up everything so far (running total) or rank rows and keep #1 (latest). If the question says "alongside", "so far", or "latest per" — think window function.

**Worked example (scenario)**
```sql
SELECT customer_id, month,
  SUM(revenue)                                         AS month_total,
  SUM(revenue) OVER (PARTITION BY customer_id, month ORDER BY day
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)  AS mtd_running
FROM daily_revenue;
```

**How the exam asks it**
> Finance needs each customer's cumulative month-to-date revenue alongside monthly revenue in one query.

✅ **A window function — inside each customer and month, walk the days in order and sum everything from the month's start up to each day.** Traps: "GROUP BY with sum and max" (squashes rows — no running total), "self-join on earlier days" (works but slow), "broadcast self-join with a rollup".""",
        },
        {
            "title": "Concept 13 — Complex scenario: time-travel surgery after a bad batch",
            "section": "Data Transformation and Modeling (22%)",
            "body": """**Why it matters for the exam:** Delta keeps every old version of your table, like save-points in a game. A bad batch at 10:00 doesn't mean starting over: you can read the table as it was before 10:00 (time travel) and copy just the damaged rows back in. Careful — RESTORE alone would also erase the good data loaded after 11:00, so when the question says "keep the later data", the answer is a small targeted MERGE from the old version, not a full restore.

**Worked example (scenario)**
```python
good = (spark.read.option("versionAsOf", 41).table("sales")
        .filter("region = 'EMEA'"))          # rows as of before the bad batch
(good.alias("s")
 .merge(spark.table("sales").alias("t"), "t.order_id = s.order_id")
 .whenMatchedUpdateAll().execute())          # surgical repair only
```

**How the exam asks it**
> Revert only the 10:00 corruption while keeping everything loaded after 11:00.

✅ **Read the table as it was before 10:00 (time travel) and MERGE only the damaged rows back in.** Traps: "RESTORE the whole table" (also erases the good 11:00 data), "VACUUM immediately", "OPTIMIZE with ZORDER" (neither repairs data).""",
        },
        {
            "title": "Concept 14 — Complex scenario: liquid clustering vs over-partitioning",
            "section": "Data Transformation and Modeling (22%)",
            "body": """**Why it matters for the exam:** Partitioning splits a table into folders, one per value. Great for a big table partitioned by date. But partitioning a small table by a column with many values (like customer_id) creates thousands of near-empty folders — tiny files everywhere, slow queries. Liquid clustering is the modern fix: it physically groups rows by the columns you actually filter on, works at any table size, and keeps tidying itself up in the background.

**Worked example (scenario)**
```sql
CREATE OR REPLACE TABLE sales.gold_orders (
  order_id BIGINT, customer_id BIGINT, order_date DATE)
CLUSTER BY (customer_id);

OPTIMIZE sales.gold_orders;   -- incremental recluster
```

**How the exam asks it**
> A few thousand rows per day partitioned by date; queries filter on customer_id (millions of values).

✅ **Switch to liquid clustering on the columns people filter on, like customer_id.** Traps: "add more partition columns" (more tiny folders), "partition by customer_id" (one folder per customer — far worse), "turn off statistics collection".""",
        },
    ],
    (
        "associate",
        "Databricks Intelligence Platform (6%)",
    ): [
        {
            "title": "Concept 1 — Workspace anatomy: where everything lives",
            "section": "Databricks Intelligence Platform (6%)",
            "body": """**What it is:** the workspace is your project space — a folder tree that holds **notebooks** (code + results), **SQL queries** (saved queries in the SQL editor), **AI/BI dashboards** (visualizations), and **files**. Know which tool for which job: multi-language exploration → notebook; pure SQL analysis → query editor; sharing visual results with business users → dashboard. Your code lives in the **workspace**; your data lives in **Unity Catalog** — that separation is itself a common exam point.

**Simple example:** create a notebook `01_explore_orders`, attach compute, run `%sql SELECT * FROM workspace.cert_prep.questions LIMIT 5`. Then click a dashboard to see how results are shared.

**How the exam asks it**
> An analyst wants to explore data with both Python and SQL, keeping code and output together in one document. What should they use?

✅ **A Databricks notebook.** A SQL query holds only SQL; a dashboard is read-only for consumers.""",
        },
        {
            "title": "Concept 2 — Notebooks: languages, magics, and cell execution",
            "section": "Databricks Intelligence Platform (6%)",
            "body": """**What it is:** every notebook has a default language, and you switch per cell with **magics**: `%sql`, `%python`, `%md` (markdown), `%r`, `%scala`. Cells run top-to-bottom with *Run all*; variables are shared between cells of the same language. Notebooks save as standard **`.ipynb`** files — which is exactly why they sync cleanly with Git folders.

**Simple example**
```python
%sql
SELECT region, SUM(amount) FROM sales GROUP BY region
```

**How the exam asks it**
> A data engineer's notebook uses SQL as the default language. How can they run one Python cell inside it?

✅ **Start the cell with `%python`.** Follow-up classic: "Notebook files exported to Git are in what format?" → `.ipynb` (standard, open format).""",
        },
        {
            "title": "Concept 3 — Compute types: which one for which job",
            "section": "Databricks Intelligence Platform (6%)",
            "body": """**What it is:** four compute flavors, and the exam loves "which should they use?": **All-purpose clusters** — interactive, shared between users, run until you stop them. **Job clusters** — created fresh for each job run, fully isolated, auto-terminated when the job ends (cheapest and most reproducible for scheduled work). **Serverless compute** — starts in seconds, no cluster management at all. **SQL warehouses** — SQL-specific compute for queries, dashboards, and BI tools.

**Simple example:** a daily 6am load job → job compute. An analyst exploring interactively → all-purpose or serverless. A dashboard refresh → SQL warehouse.

**How the exam asks it**
> A scheduled daily job must minimize cost and not leave compute running. Which compute should it use?

✅ **Job clusters** — they start with the run and are terminated automatically afterwards. Trap: "all-purpose cluster, stop it manually" (wasteful, forgettable, never reproducible).""",
        },
        {
            "title": "Concept 4 — Code in the workspace, data in the catalog",
            "section": "Databricks Intelligence Platform (6%)",
            "body": """**What it is:** a small but recurring exam distinction. **Workspace** stores notebooks, queries, dashboards, files, and Git repos — with folder-level permissions. **Unity Catalog** stores data objects (catalogs, schemas, tables, views, volumes) — with a completely separate, unified permission model (grants). You cannot `GRANT` a notebook; you cannot query a workspace folder. Keep the two planes apart in your head and several questions answer themselves.

**Simple example:** `USE CATALOG workspace; USE SCHEMA cert_prep; SELECT * FROM questions;` — data objects only. Sharing a notebook with a colleague happens through workspace folder permissions, not grants.

**How the exam asks it**
> Where does a data engineer manage access to tables across multiple workspaces?

✅ **Unity Catalog** — one governance layer for data, wherever the code that reads it lives (notebook, job, or dashboard).""",
        },
        {
            "title": "Concept 5 — Complex scenario: one lakehouse for ML and audited ETL",
            "section": "Databricks Intelligence Platform (6%)",
            "body": """**Why it matters for the exam:** Two teams, one table. The ML team wants files any tool can open. The audit team wants guaranteed safety: no half-written data, and a record of every change. The lakehouse gives both from one copy of the data — open file formats on top, a transaction log underneath that makes writes safe and history visible. That one sentence is the exam answer.

**Worked example (scenario)** the same Delta table serves both worlds:
```python
# ML: direct open-format access from notebooks / MLflow runs
df = spark.table("lake.video_features")

# Governed ETL: ACID writes plus a full audit trail
spark.table("lake.video_features").history().show()
```

**How the exam asks it**
> A data architect designs a model that works for video-based ML workloads and highly audited batch ETL/ELT workloads. How does a lakehouse help?

✅ **One copy of the data serves both teams — ML tools read the open files directly, while the Delta log underneath gives ETL safe writes and a full audit trail.** Traps: "ML must copy data into a proprietary format first", "the lakehouse only does batch ETL", "audit history is removed to make ML faster".""",
        },
        {
            "title": "Concept 6 — Complex scenario: matching compute to workload on a budget",
            "section": "Databricks Intelligence Platform (6%)",
            "body": """**Why it matters for the exam:** Paying for compute nobody is using is the classic waste, so pick compute shaped like the workload. Dashboards → a SQL warehouse that sleeps when idle. Scheduled nightly ETL → job compute that exists only for the run and then disappears. Someone typing in a notebook → interactive (all-purpose/serverless) compute. The exam describes teams and asks who should use what — always match the shape, never "one big shared cluster for everyone".

**Worked example (scenario)** three teams, three compute choices:
```text
Analysts    -> serverless SQL warehouse (auto-stop after 10 min idle)
Nightly ETL -> job compute (terminates when the run ends)
ML session  -> all-purpose / serverless interactive compute
```

**How the exam asks it**
> A startup has BI analysts, scheduled ETL engineers, and one ad-hoc Python data scientist on a tight budget.

✅ **Match the shape: dashboards → SQL warehouse (sleeps when idle), nightly ETL → job compute (exists only during the run), the Python session → interactive/serverless compute.** Traps: "one shared cluster running 24/7", "job clusters for interactive work to save money", "run the ETL on SQL warehouses".""",
        },
    ],
    (
        "associate",
        "Data Ingestion and Loading (21%)",
    ): [
        {
            "title": "Concept 1 — Auto Loader: incremental file ingestion",
            "section": "Data Ingestion and Loading (21%)",
            "body": """**What it is:** the go-to way to ingest files from cloud storage into Delta. Auto Loader (`format("cloudFiles")`) streams **newly arriving files** into a table incrementally — it remembers what it already processed in a **checkpoint**, so re-runs never duplicate data. It scales to millions of files and supports **schema evolution** (new columns in source files are handled, not fatal). Two discovery modes: **directory listing** (polls the folder) and **file notification** (uses cloud event notifications — faster and cheaper at huge scale).

**Simple example**
```python
(spark.readStream.format("cloudFiles")
     .option("cloudFiles.format", "csv")
     .option("cloudFiles.schemaLocation", chk)
     .load("s3://bucket/landing/")
     .writeStream.option("checkpointLocation", chk)
     .toTable("bronze_orders"))
```

**How the exam asks it**
> Files land continuously in an S3 folder; each file must be ingested exactly once as it arrives. What should the engineer use?

✅ **Auto Loader** — incremental, exactly-once via checkpoint, schema evolution built in.""",
        },
        {
            "title": "Concept 2 — Auto Loader vs COPY INTO: when to use which",
            "section": "Data Ingestion and Loading (21%)",
            "body": """**What it is:** the most-asked ingestion comparison. **COPY INTO** is a simple SQL statement that bulk-loads files from a folder; it is **idempotent** — safe to re-run, it just skips already-loaded files. It re-lists the folder on every run, so it works best with *small-to-moderate* file counts (≤ tens of thousands). **Auto Loader** is streaming, tracks progress in a checkpoint, handles **millions** of files, and gives near-real-time availability. Rule: ad-hoc/moderate loads and SQL-only pipelines → COPY INTO; continuous or huge-scale → Auto Loader.

**Simple example**
```sql
COPY INTO bronze_orders
FROM 's3://bucket/landing/'
FILEFORMAT = CSV;
```

**How the exam asks it**
> A team ingests ~50 CSVs per week and wants a simple, re-runnable SQL load. Which is the best fit? ✅ **COPY INTO.** Flip it: "millions of small files, continuous arrival" → Auto Loader.""",
        },
        {
            "title": "Concept 3 — The Delta transaction log and ACID",
            "section": "Data Ingestion and Loading (21%)",
            "body": """**What it is:** Delta Lake = Parquet files + a **`_delta_log`** folder of JSON commit files. Every write (insert/update/delete/optimize) creates a new **commit/version** that records exactly which files changed. Readers pick a snapshot version — so **readers never block writers, writers never block readers** (snapshot isolation), and concurrent writers are serialized safely. This is what makes Delta *ACID* on a data lake.

**Simple example:** `DESCRIBE HISTORY orders;` lists each commit: version, operation (WRITE/MERGE/OPTIMIZE), timestamp, user.

**How the exam asks it**
> What provides ACID guarantees in Delta Lake? ✅ **The transaction log.** Scenario flip: "A long-running query started before a big MERGE completed. What does the query see?" → its snapshot at start — old data, no errors, no blocking.""",
        },
        {
            "title": "Concept 4 — MERGE INTO: upserts and CDC",
            "section": "Data Ingestion and Loading (21%)",
            "body": """**What it is:** `MERGE INTO` is *the* change-data pattern: match source rows to target rows on a key, then `WHEN MATCHED THEN UPDATE` / `WHEN NOT MATCHED THEN INSERT` (plus optional DELETE). It updates existing records and inserts new ones in a single statement — a true **upsert**, idempotent when keyed correctly. This is how silver tables stay "one row per business key" as CDC arrives.

**Simple example**
```sql
MERGE INTO silver_customers t
USING cdc_customers s ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

**How the exam asks it**
> A nightly CDC file contains inserts and updates for existing rows. Which operation keeps the target correct in one statement? ✅ **`MERGE INTO`**. Traps: `INSERT INTO` (duplicates keys), `append` mode (same problem), `overwrite` (destroys untouched rows).""",
        },
        {
            "title": "Concept 5 — Managed vs external tables",
            "section": "Data Ingestion and Loading (21%)",
            "body": """**What it is:** **managed table** — Databricks stores the data under the catalog's own storage; `DROP TABLE` deletes the metadata *and the data*. **external table** — the table points at data you control via an external location; `DROP` removes only the metadata, files stay. Managed = simplest lifecycle; external = when data must stay put (shared, produced elsewhere, compliance). Accessing external paths requires catalog-level **storage credentials / external locations** — another favorite exam detail.

**Simple example**
```sql
CREATE TABLE orders;                                   -- managed
CREATE TABLE orders_ext LOCATION 's3://bucket/path/';   -- external
```

**How the exam asks it**
> An engineer runs `DROP TABLE` on an external table. What happens to the data? ✅ **Nothing — files remain; only the catalog metadata is removed.** Managed-table flip: both metadata and data are deleted.""",
        },
        {
            "title": "Concept 6 — Schema enforcement and schema evolution",
            "section": "Data Ingestion and Loading (21%)",
            "body": """**What it is:** Delta **enforces** the table schema on write — a source file with an unexpected column *fails* rather than silently corrupting the table (a Parquet-append weakness the exam likes to contrast). But you can allow controlled change: `MERGE` / `write` with **`mergeSchema = true`** evolves the schema by adding new columns; `overwriteSchema` replaces it. Auto Loader similarly has `addNewColumns` / `rescue` options. Enforcement = safety by default; evolution = opt-in.

**Simple example**
```python
df.write.option("mergeSchema", "true").mode("append").saveAsTable("orders")
```

**How the exam asks it**
> A new column appears in source data; the pipeline should add it to the Delta table without failing. What should the engineer enable? ✅ **Schema evolution (`mergeSchema`)**. Default behavior if not enabled → the write **fails** (it does not silently drop or rename).""",
        },
        {
            "title": "Concept 7 — Complex scenario: Auto Loader at scale (millions of files)",
            "section": "Data Ingestion and Loading (21%)",
            "body": """**Why it matters for the exam:** COPY INTO is fine for hundreds of files, but with millions it drowns — every run it has to work out what is new. Auto Loader is built for this: the cloud itself tells it when a file lands (no scanning), a checkpoint remembers what was already loaded (nothing loads twice), and with availableNow it can even run on a schedule — swallow everything waiting, then stop and cost nothing until next time.

**Worked example (scenario)**
```python
df = (spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.useNotifications", "true")
      .schema(schema)
      .load(path))
(df.writeStream
   .option("checkpointLocation", chk)
   .trigger(availableNow=True)      # drain the backlog, then stop
   .toTable("bronze.events"))
```

**How the exam asks it**
> Several million small JSON files per day; nightly COPY INTO takes hours and does not keep up.

✅ **Switch to Auto Loader — let the cloud announce new files (notification mode), keep a checkpoint so nothing loads twice, and let the schema grow on its own.** Traps: "give COPY INTO a bigger warehouse", "re-read the whole directory and overwrite", "ask the source to send fewer, bigger files".""",
        },
        {
            "title": "Concept 8 — Complex scenario: schema evolution and the rescue column",
            "section": "Data Ingestion and Loading (21%)",
            "body": """**Why it matters for the exam:** A stream dies the moment its data stops looking the way it expects — unless you tell it what to do instead. New column appeared? Teach it to evolve (addNewColumns, with a schema location to remember the evolving shape). Some rows have the wrong type? Send just those rows to a rescue column (_rescued_data) and let the good ones keep flowing. Deleting the checkpoint to "fix" it means re-reading everything — the trap answer.

**Worked example (scenario)**
```python
.option("cloudFiles.schemaLocation", chk_schema)
.option("cloudFiles.schemaEvolutionMode", "rescue")
.option("cloudFiles.rescueDataColumn", "_rescued_data")
```

**How the exam asks it**
> Mid-week a source adds a column and the pipeline crashes; later a column's type changes from string to integer.

✅ **Let the schema grow (addNewColumns plus a schema location to remember it), and send wrong-type rows to a rescue column instead of crashing.** Traps: "delete the checkpoint to relearn the schema" (re-reads everything), "switch to CSV so everything is a string", "hardcode the original schema forever".""",
        },
        {
            "title": "Concept 9 — Complex scenario: Auto CDC with deletes and full audit history",
            "section": "Data Ingestion and Loading (21%)",
            "body": """**Why it matters for the exam:** A CDC feed is a change log: inserts, updates, and deletes, often several per customer. To replay it correctly you need exactly two things — the key (which row a change belongs to) and the order (which change happened last). Give Auto CDC those two and it maintains the table for you, including keeping every old version (SCD Type 2) when the question also says "audit".

**Worked example (scenario)**
```python
dlt.create_streaming_table("silver_customers")
dlt.apply_changes(
    target="silver_customers",
    source="bronze_cdc_raw",
    keys=["customer_id"],
    sequence_by=F.col("_seq"),
    stored_as_scd_type=2,       # keeps history for audit
)
```

**How the exam asks it**
> An operational database's CDC feed includes deletes; downstream analysts also need full change history for audit.

✅ **Load the feed with Auto Loader, then let apply_changes do the work — give it the key (which customer), the sequence (which change came last), and ask it to keep every old version (SCD Type 2).** Traps: "dropDuplicates on the keys" (history and deletes vanish), "load it raw and let analysts figure it out", "just filter out deletes".""",
        },
        {
            "title": "Concept 10 — Complex scenario: watermarks, late data, and runaway state",
            "section": "Data Ingestion and Loading (21%)",
            "body": """**Why it matters for the exam:** A streaming aggregation has to remember every window it might still need. If you never say "too late", it remembers forever — and the job slowly drowns in its own memory. The watermark is exactly that sentence: "accept events up to 2 hours late, then stop waiting." After 2 hours a window is closed, its memory is freed, and the job stays fast forever. One setting, both exam requirements.

**Worked example (scenario)**
```python
(df.withWatermark("event_time", "2 hours")
   .groupBy(F.window("event_time", "10 minutes"), "sensor_id")
   .agg(F.avg("temp").alias("avg_temp")))
```

**How the exam asks it**
> A streaming aggregation must accept events up to 2 hours late while the state store keeps growing and processing slows.

✅ **Set a 2-hour watermark on event time — after 2 hours a window is closed and its memory is freed.** Traps: "checkpoint every 2 hours instead" (does nothing to state), "add more executors to hold the state", "deduplicate late events on the driver with pandas".""",
        },
        {
            "title": "Concept 11 — Complex scenario: full refresh vs incremental (and when COPY INTO still wins)",
            "section": "Data Ingestion and Loading (21%)",
            "body": """**Why it matters for the exam:** A streaming table is a conveyor belt — it only moves forward. But if the transformation was wrong for months, moving forward isn't enough: you need the whole history run through the fixed logic. That is a full refresh, and the exam's words for it are "reprocess the entire source". Also know the other side of the coin: a small nightly file drop that must survive re-runs is still COPY INTO territory.

**Worked example (scenario)**
```sql
REFRESH TABLE bronze.events;      -- incremental: new files only
-- a full refresh (pipeline UI/API) reprocesses ALL source files

COPY INTO silver.ref_data
FROM '/Volumes/main/landing/ref'
FILEFORMAT = CSV
COPY_OPTIONS ('mergeSchema' = 'true');
```

**How the exam asks it**
> Months ago, two days of files were processed with a wrong transformation; what reprocesses them?

✅ **A full refresh — run the entire history through the fixed transformation.** Same family: a small nightly partner file drop that must survive re-runs → ✅ **COPY INTO**. Traps: "DELETE the bad rows and OPTIMIZE" (the table never recomputes them), "VACUUM the table", "drop and recreate the checkpoint".""",
        },
    ],
    (
        "associate",
        "Working with Lakeflow Jobs (16%)",
    ): [
        {
            "title": "Concept 1 — Jobs, tasks, and dependencies",
            "section": "Working with Lakeflow Jobs (16%)",
            "body": """**What it is:** a **job** is an orchestration unit made of **tasks** — each task runs one thing (a notebook, SQL query, pipeline, or script). **Dependencies** ("runs after") wire tasks into a DAG: B waits for A; tasks with no dependency run in **parallel**. Jobs give you retries, timeouts, notifications, and a run history — none of which you get from running a notebook manually.

**Simple example:** Job `daily_sales`: task 1 `load_orders` → task 2 `build_silver` (depends on 1) → task 3 `refresh_dashboard` (depends on 2). Tasks 1 and a parallel `load_returns` run at the same time.

**How the exam asks it**
> A engineer must guarantee task B runs only after task A completes successfully. What should they configure? ✅ **A task dependency in the Lakeflow Job.** Trap: "sleep timer in the notebook" — brittle and not orchestration.""",
        },
        {
            "title": "Concept 2 — Job clusters vs all-purpose clusters (the cost question)",
            "section": "Working with Lakeflow Jobs (16%)",
            "body": """**What it is:** jobs run on **job clusters** by default — created fresh for each run, used by that job only, and **auto-terminated** when it ends. That gives you isolation (no interference with interactive work), reproducibility (same clean environment), and cost efficiency (zero idle time). **All-purpose clusters** keep running between uses and can be shared interactively — great for people, wrong for schedules. The exam repeatedly asks "minimize cost / no lingering compute for a scheduled job" → job cluster.

**Simple example:** nightly job with 20-minute runtime on job compute = 20 minutes billed, then it disappears. The same job on an all-purpose cluster left running overnight = hundreds of wasted hours.

**How the exam asks it**
> Which compute should a scheduled production job use to maximize isolation and avoid idle costs? ✅ **Job clusters.**""",
        },
        {
            "title": "Concept 3 — Run-if conditions: branching on upstream results",
            "section": "Working with Lakeflow Jobs (16%)",
            "body": """**What it is:** every dependency has a **run-if** condition: `all_success` (default), `all_failed`, `none_failed`, `any_success`… The classic pattern: an alerting/cleanup task that must run **when something fails** — give it a dependency with run-if `all_failed` (or `any_failed`), and it executes exactly on failure paths. This is how you build error handling without external monitoring tools.

**Simple example:** task `send_failure_email` depends on `load_orders` with **run-if: all_failed**. If the load succeeds, the email task is skipped; if it fails, the email fires.

**How the exam asks it**
> A job must send a notification only when an upstream task fails. Which configuration achieves this? ✅ **A downstream task with a run-if condition of `all_failed`/`any_failed`.**""",
        },
        {
            "title": "Concept 4 — Triggers: schedules, file arrival, continuous",
            "section": "Working with Lakeflow Jobs (16%)",
            "body": """**What it is:** what *starts* a job. **Schedule (cron)** — e.g. every day 06:00, with timezone. **File arrival** — the job runs when new files land in a volume or cloud path (event-driven, no polling in your code). **Continuous** — the job restarts automatically whenever it ends (near-real-time processing pipelines). Also know table-update triggers for dependent jobs. Pick by *what* should cause the run — clock, data, or availability.

**Simple example:** a partner drops a CSV into `/Volumes/.../landing` at unpredictable times → file-arrival trigger; a daily 6am report → cron `0 6 * * *`.

**How the exam asks it**
> A job should run whenever new data files arrive in cloud storage, with no fixed schedule. Which trigger type fits? ✅ **File arrival trigger.**""",
        },
        {
            "title": "Concept 5 — Monitoring runs: retries, timeouts, notifications",
            "section": "Working with Lakeflow Jobs (16%)",
            "body": """**What it is:** production hygiene. Each task supports **retries** (max retries + wait interval — for transient failures like a flaky API), a **timeout** (kill the task if it exceeds a limit — protects the schedule from hung code), and job-level **email/webhook notifications** on start, success, or failure. The **run history** page shows every run, per-task state and durations — first stop when debugging "why did last night's run fail?".

**Simple example:** `load_orders` occasionally dies on a transient network error → set max retries = 2, interval 5 minutes; add timeout 30 min so a hung load can't block the rest of the DAG.

**How the exam asks it**
> A task fails intermittently due to a temporary external API outage. What should be configured? ✅ **Automatic retries on the task.** Trap: "run the job again manually" — not an answer to an exam question about resilience.""",
        },
        {
            "title": "Concept 6 — Complex scenario: repair runs — finish the job, keep the winners",
            "section": "Working with Lakeflow Jobs (16%)",
            "body": """**Why it matters for the exam:** A 6-step job failed at step 4 because of a hiccup. Steps 1-3 already finished and were expensive. Re-running the whole job throws that work away; a repair run keeps it and redoes only what is broken — the failed task plus everything after it. Any question containing "transient" plus "expensive upstream tasks" is a repair-run question.

**Worked example (scenario)** in run history, the failed run offers **Repair run** — fix the issue (or nothing, for a cloud hiccup) and only tasks 4 → 5 → 6 re-execute, with no shared filesystem state needed because the outputs already sit in storage.

**How the exam asks it**
> Task 4 of a 6-task nightly job failed on a transient cloud error; tasks 1-3 are expensive and completed successfully.

✅ **Click Repair run — only task 4 and the tasks after it re-run; the expensive results of tasks 1-3 are kept.** Traps: "re-run the entire job", "build a new job with tasks 4-6", "start a second run and cancel the slower one".""",
        },
        {
            "title": "Concept 7 — Complex scenario: run-if conditions on multi-branch graphs",
            "section": "Working with Lakeflow Jobs (16%)",
            "body": """**Why it matters for the exam:** When two branches feed one task, you must say when the task is allowed to run. The default ("all successful") waits for both. "At least one successful" runs on either. "All done" runs no matter what happened — right for cleanup, wrong for publishing. The exam's give-away phrase is "never when both fail" — that points straight at "at least one successful".

**Worked example (scenario)** branch A (data checks) ∥ branch B (dim load) → task C (publish): set C to **At least one successful** so C runs when either branch lands and is skipped only when both fail.

**How the exam asks it**
> Task C must run if either parallel parent succeeds but never when both fail.

✅ **At least one successful — it runs when either parent succeeds and skips only when both fail.** Traps: "All successful" (waits for both), "All done" (runs even when both fail), "All failed" (fires only when both fail).""",
        },
        {
            "title": "Concept 8 — Complex scenario: task values, for-each, and per-environment parameters",
            "section": "Working with Lakeflow Jobs (16%)",
            "body": """**Why it matters for the exam:** Tasks are separate notebooks — they cannot see each other's variables. Task values are the official mailbox: one task drops a value in (set), any later task picks it up (get). Two relatives complete the family: a for-each task runs the same work once per item in a list, and job parameters let one job serve dev and prod with different catalogs.

**Worked example (scenario)**
```python
# task 1 (compute)                     # task 2 (for-each inner)
dbutils.jobs.taskValues.set(           d = dbutils.jobs.taskValues.get(
  key="run_date",                       taskKey="task_1",
  value="2026-11-22")                   key="run_date")
```

**How the exam asks it**
> Task 1 computes yesterday's partition date and must feed a for-each task that runs per region.

✅ **Task 1 saves the date with taskValues.set(); the for-each task reads it with taskValues.get().** Traps: "put the date in the cron expression", "hardcode the date and edit it every morning", "pass it through job tags".""",
        },
        {
            "title": "Concept 9 — Complex scenario: concurrency, queueing, and alerting in production",
            "section": "Working with Lakeflow Jobs (16%)",
            "body": """**Why it matters for the exam:** Two production facts. One: if a nightly job can run longer than a day, overlap is normal life — allow it with max concurrent runs, and extra runs wait politely in a queue instead of vanishing. Two: failure alerts belong in the job's settings (email, webhook, or a Slack notification destination) — never pasted into the job's code.

**How the exam asks it**
> A nightly job sometimes runs longer than 24 hours and overlapping runs are being dropped.

✅ **Raise max concurrent runs — overlap is allowed, and extra runs wait in a queue.** Same family: Slack alert on every failed run → ✅ **set it up in the job's notification settings (webhook or Slack destination)**. Traps: "drop the schedule and run it by hand", "split into two nightly jobs", "shorten the timeout to kill long runs".""",
        },
    ],
    (
        "associate",
        "Implementing CI/CD (10%)",
    ): [
        {
            "title": "Concept 1 — Git folders (Repos): Git inside the workspace",
            "section": "Implementing CI/CD (10%)",
            "body": """**What it is:** a **Git folder** clones a remote repo straight into the workspace — notebooks and files, editable in the UI, with **commit, push, pull, branches, and pull requests** built in. Because notebooks are `.ipynb`, they diff cleanly. The flow the exam expects: work on a **branch**, commit and push, open a **pull request** for review, merge to main — never edit main directly. Each branch/repo checkout is its own file view, so devs don't collide.

**Simple example:** clone `github.com/team/analytics` → create branch `feature/add-silver` → edit notebook → commit + push → open PR → teammate reviews → merge.

**How the exam asks it**
> Two engineers must change the same notebook without overwriting each other's work. What supports this? ✅ **Git branches via a Git folder** — isolate changes, merge through a pull request.""",
        },
        {
            "title": "Concept 2 — Environments and the promotion flow (dev → prod)",
            "section": "Implementing CI/CD (10%)",
            "body": """**What it is:** CI/CD on Databricks means *code* travels through environments — dev → staging → prod — while each environment points at its own compute, data, and schedule. You develop in a dev branch/workspace, promote through PRs, and deploy to prod with a **Declarative Automation Bundle** — never by hand-editing the production job. The exam's core message: production changes must be **reviewed, versioned, reproducible**.

**Simple example:** same MERGE logic, dev runs on `catalog_dev.silver.customers`, prod on `catalog.silver.customers` — only the *target config* differs, driven by bundle targets.

**How the exam asks it**
> What is the recommended way to move a notebook change into a production job? ✅ **Commit to Git, review via PR, deploy with a bundle.** Trap: "edit the production job's notebook directly" — no review, no version, exactly what CI/CD exists to prevent.""",
        },
        {
            "title": "Concept 3 — Declarative Automation Bundles: infrastructure as code",
            "section": "Implementing CI/CD (10%)",
            "body": """**What it is:** a **Declarative Automation Bundle (DAB)** defines Databricks resources — jobs, pipelines, dashboards — as **YAML files in Git**. One bundle, multiple **targets**: `dev` and `prod` differ only in variables (cluster size, catalog, schedule). Deploy with `databricks bundle deploy --target prod`; validate with `bundle validate`. Because everything is a file, your whole pipeline is reviewable in a PR and deployable identically every time.

**Simple example**
```bash
databricks bundle init          # scaffold project
databricks bundle deploy --target dev
# after PR review:
databricks bundle deploy --target prod
```

**How the exam asks it**
> A job must run a slightly different configuration in dev and prod, deployed consistently from source control. What should be used? ✅ **A Declarative Automation Bundle with environment targets.**""",
        },
        {
            "title": "Concept 4 — Parameters and widgets: one notebook, many environments",
            "section": "Implementing CI/CD (10%)",
            "body": """**What it is:** CI/CD needs the *same* code to behave differently per environment. **Job parameters** pass values (table names, dates, environment names) into a notebook task; the notebook reads them with `dbutils.widgets.get(...)`. Default widget values appear in the job UI; parameters override them per run. This keeps notebooks identical across dev/prod — all differences live in the job/bundle configuration, not in code edits.

**Simple example**
```python
catalog = dbutils.widgets.get("catalog")   # job passes: dev_catalog / prod_catalog
orders = spark.table(f"{catalog}.silver.orders")
```

**How the exam asks it**
> A notebook must write to a dev catalog in one job run and a prod catalog in another, without code changes. What achieves this? ✅ **Job parameters read via notebook widgets** (or bundle target variables). Trap: "two copies of the notebook" — drifts, unreviewable.""",
        },
        {
            "title": "Concept 5 — Complex scenario: the bundle is the source of truth (UI edits get reverted)",
            "section": "Implementing CI/CD (10%)",
            "body": """**Why it matters for the exam:** A bundle is a complete recipe for your job. Deploying it means "make the job look exactly like the recipe" — so any hand-edits someone made in the UI are quietly undone. The fix is not a smarter deploy flag; it is a rule: if the bundle owns it, change it in the bundle (through Git), never in the UI.

**Worked example (scenario)** incident tuning in the UI lasted a week; the next deploy silently reverted it → move the tuning into the bundle YAML, open a PR, merge, deploy.

**How the exam asks it**
> A teammate tuned a bundle-deployed job in the UI; the next bundle deployment reverted the changes.

✅ **If the bundle owns it, change it in the bundle (through Git) — then deploys never surprise anyone.** Traps: "deploy with --force to merge UI edits" (there is no such merge), "remove the job from the bundle", "pause deployments during incidents".""",
        },
        {
            "title": "Concept 6 — Complex scenario: targets, variables, and run-as for dev and prod",
            "section": "Implementing CI/CD (10%)",
            "body": """**Why it matters for the exam:** One pipeline, two worlds: dev and prod. Instead of editing code per world, you write down the differences once — a variable like "catalog", with a different value per target — and the bundle fills in the right value at deploy time. Per-target settings also cover who the job runs as and who can see it. The exam's tell-tale phrase: "without code changes".

**Worked example (scenario)**
```yaml
variables:
  catalog: { default: catalog_dev }
targets:
  dev:  { variables: { catalog: { default: catalog_dev } } }
  prod: { variables: { catalog: { default: catalog_prod } } }
```

**How the exam asks it**
> One pipeline definition must run against catalog_dev or catalog_prod depending on environment, and the prod job runs as a service principal.

✅ **One variable (the catalog) with a different value per target, plus per-target run-as and permissions — same code, both worlds.** Traps: "two pipeline definitions edited by hand", "hardcode prod and rewrite for dev", "two bundles with changes copied by hand".""",
        },
        {
            "title": "Concept 7 — Complex scenario: CI authentication — service principals over PATs",
            "section": "Implementing CI/CD (10%)",
            "body": """**Why it matters for the exam:** A build machine needs to log in, but it must not be a person. Give it its own identity — a service principal — with only the rights it needs to deploy, and keep its credentials in the CI system's secret store (e.g., GitHub Actions secrets). A personal token borrowed from an engineer is the trap answer: it is over-privileged, it expires, and the day that engineer leaves, your deploys die with them.

**How the exam asks it**
> GitHub Actions deploys the bundle on every merge to main; the workflow needs secure, least-privilege authentication to the workspace.

✅ **A service principal — its own login for the build machine, stored in CI secrets, with only the rights to deploy.** Traps: "the senior engineer's personal token in a GitHub secret", "admin credentials for the runner", "a shared team password in the workflow file".""",
        },
    ],
    (
        "associate",
        "Troubleshooting, Monitoring, and Optimization (10%)",
    ): [
        {
            "title": "Concept 1 — The small-files problem and OPTIMIZE",
            "section": "Troubleshooting, Monitoring, and Optimization (10%)",
            "body": """**What it is:** streaming and frequent small writes create **thousands of tiny Parquet files** — each open, metadata entry, and listing slows reads massively. **`OPTIMIZE`** compacts them into larger files (bin-packing). For write-heavy Delta tables, schedule OPTIMIZE regularly. This is the default answer whenever the scenario says "table with many small files, queries slow".

**Simple example**
```sql
OPTIMIZE bronze_events;                 -- compact now
-- or regularly: a scheduled job running OPTIMIZE weekly
```

**How the exam asks it**
> A streaming pipeline writes every 30 seconds; queries on the table are slower every day, and `DESCRIBE DETAIL` shows tens of thousands of files. What should the engineer do? ✅ **Run (or schedule) `OPTIMIZE`** to compact small files. Traps: "increase cluster size" (treats symptom), "rewrite the stream" (unnecessary).""",
        },
        {
            "title": "Concept 2 — ZORDER and data skipping",
            "section": "Troubleshooting, Monitoring, and Optimization (10%)",
            "body": """**What it is:** Delta keeps **min/max statistics** per file for each column, so queries with filters can **skip whole files** that can't match ("data skipping"). **`ZORDER BY`** rewrites files so values of chosen columns are co-located — putting all of `user_id=42`'s rows into the same few files, making skipping far more effective. ZORDER the 1–3 columns you filter most; over-ZORDERing many columns dilutes the layout.

**Simple example**
```sql
OPTIMIZE sales ZORDER BY (user_id, order_date);
```

**How the exam asks it**
> Queries filter heavily on `user_id`; most scan the whole table anyway. Which maintenance step most improves this? ✅ **`OPTIMIZE … ZORDER BY (user_id)`** — improves data-skipping effectiveness for that column.""",
        },
        {
            "title": "Concept 3 — VACUUM: reclaiming storage (and its time-travel cost)",
            "section": "Troubleshooting, Monitoring, and Optimization (10%)",
            "body": """**What it is:** every UPDATE/DELETE/MERGE leaves **old file versions** in storage — that's what time travel reads. **`VACUUM`** deletes files no longer needed by versions within the **retention period** (default **7 days**). It reclaims storage but **destroys time travel** older than the retention — the exam's trade-off question. Set retention deliberately: `VACUUM orders RETAIN 168 HOURS;` (7 days).

**Simple example**
```sql
VACUUM orders;                 -- default 7-day retention
VACUUM orders RETAIN 72 HOURS; -- keep only 3 days of history
```

**How the exam asks it**
> Storage costs grow because of old table versions no one needs after 7 days. What reduces storage without losing recent time travel? ✅ **`VACUUM` with the 7-day retention.** Flip: "must keep 30 days of time travel" → *don't* vacuum below 30-day retention (raise it first).""",
        },
        {
            "title": "Concept 4 — Reading the query profile (where slow actually is)",
            "section": "Troubleshooting, Monitoring, and Optimization (10%)",
            "body": """**What it is:** your debugging toolkit. The **query profile** (SQL) and **Spark UI** (notebook jobs) break execution into **stages**: per-stage duration, shuffle read/write, and **spill to disk** (data too big for memory — a red flag). Workflow: find the longest stage → is it shuffle-heavy (wide transformation/join/skew) or spilling (memory pressure) → fix accordingly (filter earlier, broadcast join, bigger cluster). `df.explain()` shows the logical/physical plan — which filters got pushed down, what joins exist.

**Simple example:** profile shows stage 12 = 80% of runtime with huge *shuffle read* → the `groupBy`/`join` there is the problem, not the read.

**How the exam asks it**
> A query is slow. Which tool identifies which stage consumes the most time? ✅ **The query profile / Spark UI** — measure first, then optimize. Trap answers that jump straight to "add nodes" skip the diagnosis step the exam wants.""",
        },
        {
            "title": "Concept 5 — Complex scenario: diagnosing the 199-and-1 straggler",
            "section": "Troubleshooting, Monitoring, and Optimization (10%)",
            "body": """**Why it matters for the exam:** The Spark UI's stage view lists every task and how long it took. Healthy stages look even. One task 20x slower with a huge "shuffle read" means that task was handed far more data — skew. Read the numbers before guessing anything: task durations first, shuffle read per task, then the driver logs for errors. The exam asks you to read exactly this screen.

**Worked example (scenario)** stage 47: 199 tasks ≈ 2 min, one 40 min with 10x the shuffle read → inspect its keys → 70% of rows share one customer_id → filter/re-key that value and let AQE split the partition.

**How the exam asks it**
> Diagnose a job hanging at the same stage with almost no task progress.

✅ **Open the slow stage's task list and compare task durations and shuffle read — one task grabbing far more data than the rest is skew.** Traps: "the driver is busy printing results", "VACUUM is running", "the UI is sleeping".""",
        },
        {
            "title": "Concept 6 — Complex scenario: driver OOM — collect() and toPandas() on big data",
            "section": "Troubleshooting, Monitoring, and Optimization (10%)",
            "body": """**Why it matters for the exam:** Spark keeps data spread across many workers — until you call collect() or toPandas(), which drags everything onto the single driver machine. 40 million rows will never fit there, no matter how big it is. The habit the exam wants: shrink first, move second — aggregate or sample on the cluster, then pull only the small result.

**Worked example (scenario)**
```python
# DON'T: df.toPandas() on 40M rows -> driver OOM
# DO: summarize on the cluster, convert the small result
summary = df.groupBy("region").agg(F.sum("amount").alias("total"))
summary.toPandas()        # thousands of rows, not millions
```

**How the exam asks it**
> A notebook calls .toPandas() on 40M rows for a quick chart and the driver crashes with OutOfMemoryError.

✅ **Shrink it on the cluster first — aggregate or sample, then call toPandas() only on the small result.** Traps: "give the driver 1 TB of memory", "cache it first" (it still collects everything), "rewrite the code in Scala".""",
        },
        {
            "title": "Concept 7 — Complex scenario: the 400,000-file table — compaction and clustering",
            "section": "Troubleshooting, Monitoring, and Optimization (10%)",
            "body": """**Why it matters for the exam:** A Delta table is just a pile of files. Hundreds of thousands of tiny ones is death by paperwork: every query must list and open them all. OPTIMIZE glues them into a few big healthy files, and clustering on your filter columns makes sure related rows live together, so most files can be skipped entirely. VACUUM only deletes old junk — it fixes nothing, it just cleans up afterwards.

**Worked example (scenario)**
```sql
OPTIMIZE sales.orders
  ZORDER BY (customer_id, order_date);  -- compact + co-locate keys
VACUUM sales.orders RETAIN 168 HOURS;   -- reclaim obsolete files
```

**How the exam asks it**
> A Delta table has about 400,000 small files; queries take minutes just to list files; analysts filter on customer_id and order_date.

✅ **Run OPTIMIZE with clustering on customer_id and order_date — files get big and healthy, and queries can skip most of them.** Traps: "VACUUM immediately" (only deletes old files, fixes nothing), "restart the warehouse hourly", "move the table to CSV".""",
        },
        {
            "title": "Concept 8 — Complex scenario: unblocking a poisoned micro-batch",
            "section": "Troubleshooting, Monitoring, and Optimization (10%)",
            "body": """**Why it matters for the exam:** A stream failed and keeps failing the same batch — one file is poisoned. Do not nuke the checkpoint (that means "start all over again" and re-read everything). Do surgery: find the bad file from the error message, quarantine it (move it aside, or catch it with the rescue column), then restart the stream — the checkpoint picks up right where it should.

**How the exam asks it**
> A streaming job is stuck on the same micro-batch after one malformed file; valid data behind it is delayed.

✅ **Move the bad file aside (quarantine) and restart the stream — the checkpoint skips past it and keeps going.** Traps: "delete the checkpoint" (re-reads everything, usually duplicating), "drop the table and rebuild", "turn off schema checking so bad files load as NULLs".""",
        },
        {
            "title": "Concept 9 — Complex scenario: warehouse concurrency — scale out for dashboards",
            "section": "Troubleshooting, Monitoring, and Optimization (10%)",
            "body": """**Why it matters for the exam:** 200 dashboard users at 9 AM is a traffic problem, not a speed problem. One warehouse cluster works through one queue of queries at a time. More clusters = more queues = more users served at once. Bigger machines make each query faster but do not add lanes — and that distinction is the whole question.

**How the exam asks it**
> At 9 AM, 200 concurrent dashboard users overwhelm a single-cluster warehouse; queries queue for minutes.

✅ **Add more clusters to the warehouse — more lanes, so more users get served at once.** Traps: "buy bigger nodes only" (each query faster, still one queue), "disable result caching", "run the dashboard hourly on a job cluster".""",
        },
    ],
    (
        "associate",
        "Governance and Security (15%)",
    ): [
        {
            "title": "Concept 1 — The Unity Catalog model: one governance layer",
            "section": "Governance and Security (15%)",
            "body": """**What it is:** Unity Catalog (UC) governs all data across **every workspace** in the account. Structure: **catalog → schema → table / view / volume / function**. UC replaces the old per-workspace Hive metastore: one central place for permissions, lineage, and auditing — data in any workspace, same rules everywhere. Schemas usually group by team/domain or bronze/silver/gold.

**Simple example:** `workspace.cert_prep.questions` reads as catalog `workspace`, schema `cert_prep`, table `questions` — you've used the full hierarchy in this app already.

**How the exam asks it**
> An organization has multiple workspaces and needs one consistent permission model over all data. What provides it? ✅ **Unity Catalog**. Trap: "the Hive metastore" — per-workspace, no central governance; that's exactly what UC fixed.""",
        },
        {
            "title": "Concept 2 — The grant chain: USE CATALOG → USE SCHEMA → SELECT",
            "section": "Governance and Security (15%)",
            "body": """**What it is:** the #1 practical governance question. To read a table a principal needs **three** grants: `USE CATALOG` on the catalog (to traverse it), `USE SCHEMA` on the schema (to see inside it), and `SELECT` on the table itself. Miss any one → "not found / permission denied", even if SELECT exists. Grant via SQL:

**Simple example**
```sql
GRANT USE CATALOG ON CATALOG workspace TO `group Analysts`;
GRANT USE SCHEMA ON SCHEMA workspace.cert_prep TO `group Analysts`;
GRANT SELECT ON TABLE workspace.cert_prep.questions TO `group Analysts`;
```

**How the exam asks it**
> A user has `SELECT` on a table but cannot query it. What is missing? ✅ **`USE CATALOG` and/or `USE SCHEMA` grants on the parent objects.** The exam loves this "has SELECT but still blocked" scenario.""",
        },
        {
            "title": "Concept 3 — Principals, ownership, and sharing with groups",
            "section": "Governance and Security (15%)",
            "body": """**What it is:** three principal types: **users**, **groups**, and **service principals** (non-human identities for jobs and apps). Every UC object has an **owner** with full control of it (including the right to grant further). Best practice the exam tests: assign permissions to **groups**, never individual users — one grant covers onboarding/offboarding for whole teams. Service principals own production pipelines so they don't depend on any employee's account.

**Simple example:** new analyst joins → admin adds them to `group Analysts` → they instantly inherit every grant the group has. Done.

**How the exam asks it**
> Permissions must apply to a whole team and survive members joining and leaving. What should grants be assigned to? ✅ **A group.** Trap: "each user individually" — an admin nightmare the exam marks wrong.""",
        },
        {
            "title": "Concept 4 — Lineage and audit: provenance for free",
            "section": "Governance and Security (15%)",
            "body": """**What it is:** UC **captures lineage automatically** — which tables/queries/jobs produced or consumed which data — no setup. Impact analysis ("if I change this column, what breaks?") becomes a look at the lineage graph. **Audit logs / system tables** record who accessed what, when. Together they answer the two governance questions auditors always ask: *where did this data come from?* and *who touched it?*

**Simple example:** open a table in Catalog Explorer → Lineage tab → upstream tables and downstream dashboards/jobs appear as a graph.

**How the exam asks it**
> Before changing a table's schema, an engineer wants to know every downstream consumer. What provides this with no extra tooling? ✅ **Unity Catalog lineage.**""",
        },
        {
            "title": "Concept 5 — Hive metastore vs Unity Catalog (why UC won)",
            "section": "Governance and Security (15%)",
            "body": """**What it is:** the legacy world: each workspace had its **own** Hive metastore — data and permissions trapped per workspace, no shared view, no central audit. **Unity Catalog** fixes all of it: account-level, all workspaces see the same catalogs, one permission model, built-in lineage/audit, and finer objects (volumes, functions). This contrast *is* the exam question.

**Simple example:** with a Hive metastore, `GRANT SELECT` had to be repeated in *every* workspace needing the table; with UC, one grant on `catalog.schema.table` applies everywhere.

**How the exam asks it**
> What limitation of legacy Hive metastores does Unity Catalog address? ✅ **Per-workspace isolation of data and permissions — UC centralizes governance across all workspaces.**""",
        },
        {
            "title": "Concept 6 — Complex scenario: row filters and column masks in production",
            "section": "Governance and Security (15%)",
            "body": """**Why it matters for the exam:** One table, different eyes. A row filter is a small rule attached to the table: "this row only shows if it belongs to the viewer's region". A column mask does the same for one column: "salary is visible only to the HR group; everyone else sees NULL". Because both attach to the table itself, every path — SQL, notebooks, dashboards — is covered automatically. No copies, no trusting users to behave.

**Worked example (scenario)**
```sql
CREATE OR REPLACE FUNCTION main.sec.region_filter(region STRING)
RETURNS BOOLEAN
RETURN is_account_group_member(CONCAT('region_', region));

ALTER TABLE main.gold.customers SET ROW FILTER
  main.sec.region_filter ON (region);
```

**How the exam asks it**
> Analysts in five regions query one governed table but may only see their own region.

✅ **A row filter on the table — each viewer only sees the rows belonging to their own region.** Same family: compensation visible only to the hr_comp group → ✅ **a column mask — members see the real value, everyone else sees NULL**.""",
        },
        {
            "title": "Concept 7 — Complex scenario: least privilege and evidence for auditors",
            "section": "Governance and Security (15%)",
            "body": """**Why it matters for the exam:** Sharing a catalog safely means granting the smallest thing that works: each team gets access to its own schemas, never the whole catalog. And when an auditor asks "prove who touched the payroll data in the last 90 days", the answer is not screenshots or a spreadsheet — Unity Catalog already writes down every single action in system audit tables. Query them, export, done.

**Worked example (scenario)**
```sql
GRANT USE CATALOG ON CATALOG company_data TO `team_a`;
GRANT USE SCHEMA, SELECT ON SCHEMA company_data.team_a_assets TO `team_a`;

-- evidence for the auditor
SELECT event_time, user_identity, action_name
FROM system.access.audit
WHERE service_name = 'UnityCatalog';
```

**How the exam asks it**
> Two teams share a catalog; team B must never browse team A's finance schemas.

✅ **Grant each team access only to its own schema — never the whole catalog.** Same family: an auditor wants 90 days of proof → ✅ **Unity Catalog audit tables**. Traps: screenshots of the permissions UI, a shared spreadsheet of grants, job run history.""",
        },
        {
            "title": "Concept 8 — Complex scenario: sharing data without shipping it (Delta Sharing)",
            "section": "Governance and Security (15%)",
            "body": """**Why it matters for the exam:** The safest copy is the one you never make. A share is a list of tables you publish; a recipient is the partner's key to it. They query with their own compute — your data never leaves, and revoking the recipient cuts them off instantly. When "no copies" and "instantly revocable" show up in the same question, it is always Delta Sharing.

**Worked example (scenario)**
```sql
CREATE SHARE partner_analytics;
ALTER SHARE partner_analytics ADD TABLE gold.orders;
CREATE RECIPIENT partner_acme;           -- gets a token to query with
GRANT SELECT ON SHARE partner_analytics TO RECIPIENT partner_acme;
-- revoke = instant cutoff, no data to recall
```

**How the exam asks it**
> A partner must analyze 12 of your Delta tables; copying data is forbidden and access must be revocable immediately.

✅ **A Delta Sharing share — publish the 12 tables and give the partner a recipient; revoking the recipient cuts them off instantly.** Traps: "nightly exports to the partner's bucket", "partner users with workspace accounts", "read-only mounts over VPN".""",
        },
        {
            "title": "Concept 9 — Complex scenario: lineage for impact analysis and identity-proof pipelines",
            "section": "Governance and Security (15%)",
            "body": """**Why it matters for the exam:** Two "who" questions. Before renaming a column you need everyone downstream who uses it — that list is lineage, and Unity Catalog tracks it automatically, down to the column. And a job that depends on one human's login dies the day that person leaves the company — so run jobs as a service principal, a job-shaped identity that never resigns, and hand ownership to it.

**How the exam asks it**
> Before renaming cust_id to customer_id, the team needs every dependent object.

✅ **Column-level lineage — Unity Catalog already knows every object that depends on that column.** Same family: the job's owner is leaving → ✅ **recreate the job under a service principal and hand it the ownership and grants**. Traps: "ask the departing engineer for a long-lived token", "store their PAT in a secret scope", "rebuild every table nightly".""",
        },
    ],
    (
        "professional",
        "Developing Code for Data Processing using Python and SQL (22%)",
    ): [
        {
            "title": "Concept 1 — Watermarks: stop waiting so memory doesn't grow forever",
            "section": "Developing Code for Data Processing using Python and SQL (22%)",
            "body": """**Why it matters for the exam:** A streaming aggregation never gets a "final" answer — Spark keeps every group's running total in memory in case a late event arrives for it. Without a stop rule, that memory grows forever. The **watermark** is the stop rule: "once an event's timestamp is more than 2 hours behind the newest data, that window is closed — stop waiting for it." Closed windows are dropped from memory (that's *state eviction*), and events arriving later than the watermark are dropped too. Three parts to remember: the timestamp column, the delay, and the freed memory.

**Worked example (scenario)**
```python
(spark.readStream.table("events")
    .withWatermark("event_time", "2 hours")
    .groupBy(window("event_time", "10 minutes"), "region")
    .agg(F.sum("amount").alias("total"))
)
# once event_time + 2h has passed, that 10-minute window is closed for good
```

**How the exam asks it**
> An hourly aggregation over an event stream has unbounded state growth; events arrive up to 2 hours late.

✅ **Set a 2-hour watermark on event time — old windows close, their memory is freed, and later-arriving rows are dropped.** Traps: "checkpoint every 2 hours" (checkpoint ≠ state), "add more workers" (state still grows), "use complete output mode" (keeps every window — the worst choice).""",
        },
        {
            "title": "Concept 2 — Trigger.AvailableNow: process what's there, then clock out",
            "section": "Developing Code for Data Processing using Python and SQL (22%)",
            "body": """**Why it matters for the exam:** A trigger decides *when* a stream processes data. The default runs forever in small micro-batches. `trigger(availableNow=True)` processes everything currently available and then **stops** — "work through the backlog, then clock out." That is exactly how scheduled backfills work: run nightly, consume only the new data, exit, release compute, resume from the checkpoint next run. It's also what a *Triggered* pipeline does. Contrast with `Trigger.Once` (one single giant batch, no size control) and `processingTime` (never stops on its own).

**Worked example (scenario)**
```python
(spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .load(landing_path)
    .writeStream
    .option("checkpointLocation", chk)     # remembers progress across runs
    .trigger(availableNow=True)             # drain available data, then stop
    .toTable("bronze_events")
)
```

**How the exam asks it**
> A nightly job must process only newly arrived files and exit when done, picking up where the last run stopped.

✅ **trigger(availableNow=True) with a durable checkpoint — drains available data in bounded micro-batches, then exits; the next run resumes from the checkpoint.** Traps: "trigger(once=True)" (one huge unbounded batch), "processingTime="1 day"" (keeps running), "batch INSERT OVERWRITE" (re-reads everything).""",
        },
        {
            "title": "Concept 3 — foreachBatch + idempotent MERGE: streaming into anything",
            "section": "Developing Code for Data Processing using Python and SQL (22%)",
            "body": """**Why it matters for the exam:** Structured Streaming has first-class sinks for Delta tables — but what if the target is a JDBC database, Redis, or a legacy system? `foreachBatch` hands each micro-batch to **your** function as an ordinary DataFrame, and you write it however the target needs. The catch: on failure a batch is retried, so the same rows can hit your function twice. The fix is making the write **idempotent** — running it twice leaves the same result as once. The classic idempotent pattern is a MERGE on keys (or overwrite exactly the batch's range).

**Worked example (scenario)**
```python
def upsert(batch_df, batch_id):
    batch_df.createOrReplaceTempView("batch")
    spark.sql('''
        MERGE INTO jdbc_metrics t USING batch s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    ''')

(stream.writeStream
    .foreachBatch(upsert)                     # your logic, every micro-batch
    .option("checkpointLocation", chk)
    .start())
```

**How the exam asks it**
> A stream must upsert into a non-Delta external database reliably, even when a micro-batch is retried.

✅ **foreachBatch with an idempotent MERGE (or range-overwrite) plus a checkpoint — retries can't create duplicates.** Traps: "plain append writes" (retries duplicate rows), "writeStream.format('jdbc')" (JDBC isn't a streaming sink), "turn retries off" (loses the fault tolerance you want).""",
        },
        {
            "title": "Concept 4 — Checkpoints: the bookmark that makes a stream resumable",
            "section": "Developing Code for Data Processing using Python and SQL (22%)",
            "body": """**Why it matters for the exam:** A stream's **checkpoint** is its memory: which files and rows were already processed, plus any aggregation state. Restart with the same checkpoint → the stream picks up exactly where it stopped, no duplicates. Delete it or point to a new location → the stream thinks it's brand new and re-reads everything, usually duplicating data. Rules the exam tests: same checkpoint for the same query; changed query logic (different joins/aggregations) needs a fresh checkpoint location; never delete a checkpoint to "fix" a schema error — that reprocesses everything.

**Worked example (scenario)**
```python
stream = (spark.readStream.table("bronze")
    .writeStream
    .option("checkpointLocation", "/chk/events_v1")
    .toTable("silver"))
# restart the job with the SAME location -> resumes where it left off
# change the aggregation logic -> new location (e.g. events_v2)
```

**How the exam asks it**
> After a restart, the streaming job re-processed the whole source directory and the target now has duplicates. Why?

✅ **The checkpoint was deleted or moved — the stream lost its place and re-read everything.** Traps: "Spark doesn't support exactly-once" (checkpoint + idempotent sink gives it), "the driver restarted" (restarts are exactly what checkpoints handle), "VACUUM deleted progress" (VACUUM only cleans old table files).""",
        },
        {
            "title": "Concept 5 — One engine for batch and streaming: what changes and what doesn't",
            "section": "Developing Code for Data Processing using Python and SQL (22%)",
            "body": """**Why it matters for the exam:** Structured Streaming is not a separate product — the same DataFrame engine, same optimizations, just in "keep going" mode. Reads start with `spark.readStream.table(...)`, and instead of an action you call `.writeStream...start()`. Actions like `collect()` or `show()` are **impossible** on a streaming DataFrame — it never ends, so nothing can return "all rows". Output modes matter: `append` (emit rows once they're final), `update` (emit what changed each batch), `complete` (re-emit the entire aggregation every batch — state-heavy). Dedup is built in: `dropDuplicates` works on streams *when* a watermark bounds the memory it needs.

**Worked example (scenario)**
```python
df = spark.readStream.table("events").withWatermark("ts", "1 hour")
clean = df.dropDuplicates(["event_id"])   # memory-safe because the watermark bounds it
```

**How the exam asks it**
> Which is NOT valid on a streaming DataFrame — or, when must a watermark exist for dropDuplicates?

✅ **collect()/show() never work on an endless stream; dropDuplicates on a stream needs a watermark so Spark knows when it can forget an event_id.** Traps: "cache the streaming DataFrame" (an endless stream can't be fully cached), "complete mode saves state" (it's the most stateful), "streaming joins need a different API" (same API; stream-stream joins just need watermarks).""",
        },
        {
            "title": "Concept 6 — Built-ins over UDFs; when/otherwise and higher-order functions",
            "section": "Developing Code for Data Processing using Python and SQL (22%)",
            "body": """**Why it matters for the exam:** A Python UDF forces Spark to ship every row out of the optimized JVM into a Python process and back — the optimizer can't see inside it, so it can't optimize around it. Built-in expressions run inside the engine. So the habit the exam rewards: always look for the built-in first. Know the branching tool — `when(...).when(...).otherwise(...)` is Spark's CASE WHEN; and for arrays, the higher-order functions `transform`, `filter`, `exists` beat explode-and-regroup because the whole array is processed in one expression.

**Worked example (scenario)**
```python
df2 = df.select(
    F.when(F.col("amount") > 100, "high").otherwise("normal").alias("band"),
    F.expr("transform(tags, t -> upper(t))").alias("tags_up"),
)
# prefer this over a UDF like: def band(x): return 'high' if x>100 else 'normal'
```

**How the exam asks it**
> A Python UDF is making a large transformation slow. What's the fix?

✅ **Replace the UDF with built-in expressions (when/otherwise, higher-order functions, F.expr) so Catalyst can optimize the plan.** Traps: "add more executors" (the serialization bottleneck stays), "use a pandas UDF" (faster, but still second to built-ins), "disable AQE" (unrelated).""",
        },
    ],
    (
        "professional",
        "Data Ingestion & Acquisition (7%)",
    ): [
        {
            "title": "Concept 1 — Change data feed: row-level history you can stream",
            "section": "Data Ingestion & Acquisition (7%)",
            "body": """**Why it matters for the exam:** Sometimes you don't need the table — you need what *changed* in it. Diffing two snapshots is expensive and ambiguous. The **change data feed** (CDF) makes Delta record every row-level change — inserts, updates, deletes — and lets you read them back: as a batch between two versions, or as a stream that processes changes as they happen. Every returned row carries `_change_type` (insert / update_postimage / delete) and `_commit_version`, so consumers can react precisely and idempotently.

**Worked example (scenario)**
```sql
ALTER TABLE orders SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

SELECT * FROM table_changes('orders', 5, 12);  -- changes between versions 5..12
-- streaming: read option readChangeFeed=true, process each change as it lands
```

**How the exam asks it**
> A downstream job must process every insert/update/delete on a table exactly once, incrementally, without reading the whole table.

✅ **Enable the change data feed and consume it (batch range or stream) — each row-level change arrives with its type and version.** Traps: "diff two snapshots" (expensive, timing-dependent), "read DESCRIBE HISTORY" (commit metadata, not row data), "nightly full copy".""",
        },
        {
            "title": "Concept 2 — Streaming tables vs materialized views: append vs recompute",
            "section": "Data Ingestion & Acquisition (7%)",
            "body": """**Why it matters for the exam:** Both live in Lakeflow pipelines, but they grow differently. A **streaming table** ingests incrementally: each refresh processes only NEW data and appends it (with an optional full refresh to rebuild from source). A **materialized view** recalculates the whole result from scratch on every refresh — right for aggregations and joins over batch sources where "the latest answer" matters. One-liner for the exam: streaming table = "process each new record once"; materialized view = "re-derive the whole answer every refresh."

**Worked example (scenario)**
```sql
CREATE OR REFRESH STREAMING TABLE st_events AS
SELECT * FROM STREAM read_files('/landing/events');

CREATE OR REFRESH MATERIALIZED VIEW mv_daily AS
SELECT event_date, count(*) AS n FROM st_events GROUP BY event_date;
```

**How the exam asks it**
> Which pipeline object backs a continuous append-only ingest, and which backs a fully-recomputed nightly aggregate?

✅ **Streaming table for the append-only feed; materialized view for the full-refresh aggregation.** Traps: "materialized views update incrementally" (they fully recompute), "streaming tables support MERGE-style updates" (append-only by design), "a regular CREATE TABLE AS SELECT refreshes itself" (plain tables never refresh).""",
        },
        {
            "title": "Concept 3 — Append flows and Auto Loader inside pipelines",
            "section": "Data Ingestion & Acquisition (7%)",
            "body": """**Why it matters for the exam:** One streaming table can write from more than its main definition. **Append flows** are additional write paths into the same streaming table — a second source, or a one-time historical backfill that runs as a batch and appends without rebuilding the table. And **Auto Loader** (`cloudFiles`) remains the file-ingestion pattern inside pipelines: it discovers new files incrementally, handles schema drift, and scales to millions of files — that's why it beats a directory re-read for cloud storage.

**Worked example (scenario)**
```python
@dlt.table(name="st_events")
def events():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json").load(landing_path))

@dlt.append_flow(target="st_events", name="backfill_2020")
def backfill():
    return spark.read.json("/archive/2020/")   # one-time historical append
```

**How the exam asks it**
> Add a one-time historical load into an existing streaming table without rebuilding it from source.

✅ **An append flow writing the historical batch into the existing streaming table.** Traps: "full refresh after reloading the source" (slow, needs the source retained), "INSERT INTO the streaming table manually" (managed by its pipeline), "delete the checkpoint and restart" (reprocesses everything).""",
        },
    ],
    (
        "professional",
        "Data Transformation, Cleansing, and Quality (10%)",
    ): [
        {
            "title": "Concept 1 — Expectations: keep, drop, or fail — and always report",
            "section": "Data Transformation, Cleansing, and Quality (10%)",
            "body": """**Why it matters for the exam:** Expectations are data-quality rules you attach to a dataset in a Lakeflow pipeline. Each rule is a boolean (true/false per row), and the pipeline tracks what percentage of rows passed — that's the metric the exam keeps mentioning. Three flavors, three severities: `expect` — bad rows **stay**, but the metric reports them; `expect_or_drop` — bad rows are **removed**; `expect_or_fail` — the first bad row **stops the whole pipeline**. Quarantine trick: `expect` (keep everything) + a view that filters the bad rows into a separate quarantine table.

**Worked example (scenario)**
```python
@dlt.expect_or_drop("valid_amount", "amount > 0")
@dlt.expect("known_country", "country IN ('US', 'DE', 'IN')")
def orders_clean():
    return spark.readStream.table("orders_raw")
# UI/metrics: % passing per rule; bad rows dropped, unknown countries kept+flagged
```

**How the exam asks it**
> Invalid rows must be removed, and the passing percentage must be visible in pipeline metrics.

✅ **expect_or_drop — failing rows are removed and the rule's pass-rate is tracked in metrics.** Traps: "expect" (keeps bad rows), "expect_or_fail" (halts the pipeline), "a CHECK constraint" (write-time table rule, no metrics).""",
        },
        {
            "title": "Concept 2 — Constraints: NOT NULL and CHECK, enforced at write time",
            "section": "Data Transformation, Cleansing, and Quality (10%)",
            "body": """**Why it matters for the exam:** Constraints are guarantees built into the **table**, enforced on **every write** — a MERGE, an INSERT, anything. `NOT NULL` rejects rows with a null in that column; `CHECK (condition)` validates each row against a condition. A violating write **fails loudly, entirely**. Contrast with expectations: constraints protect the table for everyone forever; expectations are pipeline rules that produce metrics. When a question says "must never be possible to write" — that's a constraint.

**Worked example (scenario)**
```sql
CREATE TABLE sales (
  id BIGINT NOT NULL,
  amount DECIMAL(10,2) CHECK (amount >= 0)
) USING DELTA;
-- any INSERT/MERGE with a null id or negative amount fails the statement
```

**How the exam asks it**
> Null ids must never land in the table, even from a MERGE written months from now.

✅ **A NOT NULL constraint (plus CHECK where useful) — enforced at write time for every writer.** Traps: "expect_or_fail" (only runs during pipeline refreshes), "filter nulls in the job" (only as good as every future writer), "a column mask" (hides values, doesn't validate).""",
        },
        {
            "title": "Concept 3 — Generated columns: values the table computes for you",
            "section": "Data Transformation, Cleansing, and Quality (10%)",
            "body": """**Why it matters for the exam:** A generated column is computed by the table from other columns at write time (`GENERATED ALWAYS AS (...)`) — nobody supplies it, so nobody can write a wrong value. The exam's favorite use: you can't cluster a table by an expression, but you *can* cluster by a generated column. So: extract the date from a timestamp into a generated column, then `CLUSTER BY` that date — clean layout without a redundant column that writers must maintain.

**Worked example (scenario)**
```sql
CREATE TABLE events (
  ts TIMESTAMP,
  event_date DATE GENERATED ALWAYS AS (CAST(ts AS DATE))
) USING DELTA CLUSTER BY (event_date);

INSERT INTO events (ts) VALUES (current_timestamp());  -- event_date is filled in
```

**How the exam asks it**
> A table must be clustered on the date portion of a timestamp column.

✅ **Generate a date column with GENERATED ALWAYS AS, then CLUSTER BY it.** Traps: "CLUSTER BY ts" (too fine-grained — poor file skipping), "partition by the timestamp" (one folder per timestamp value — disaster), "a view with a derived column" (organizes nothing on storage).""",
        },
    ],
    (
        "professional",
        "Data Sharing and Federation (5%)",
    ): [
        {
            "title": "Concept 1 — Delta Sharing: shares, recipients, providers",
            "section": "Data Sharing and Federation (5%)",
            "body": """**Why it matters for the exam:** Delta Sharing publishes your governed tables to outsiders without copying data anywhere. Three roles: a **share** is your bundle of tables/views; a **recipient** is who may read it (they get a token — no workspace account needed); a **provider** is an external party sharing data *to* you. Recipients query with anything that speaks the open protocol — Power BI, Excel, pandas, Tableau — or, Databricks-to-Databricks, as a live catalog in their workspace. Revoke the recipient and access ends instantly, centrally.

**Worked example (scenario)**
```sql
CREATE SHARE partner;
ALTER SHARE partner ADD TABLE gold.orders;
CREATE RECIPIENT acme;                         -- no workspace user required
GRANT SELECT ON SHARE partner TO RECIPIENT acme;
-- REVOKE ... and acme is cut off immediately
```

**How the exam asks it**
> An external partner (no workspace account) needs live queries on 5 tables, revocable at once.

✅ **A share + recipient over Delta Sharing — open-protocol live reads, instant central revocation.** Traps: "nightly CSV exports" (stale copies, no revoke), "workspace accounts for partner users" (license + over-access), "a foreign catalog" (that's consuming others' databases, not publishing).""",
        },
        {
            "title": "Concept 2 — Foreign catalogs: query external databases live",
            "section": "Data Sharing and Federation (5%)",
            "body": """**Why it matters for the exam:** Lakehouse Federation lets you query data sitting in Postgres, MySQL, Redshift, Snowflake, and friends **without copying it**. You create a **connection** (stored credentials + endpoint), then a **foreign catalog** on top of it. The remote tables appear like normal Unity Catalog tables, access is governed by UC permissions, and Spark pushes filters and joins down to the source where it can. Direction matters for the exam: a foreign catalog is YOU consuming someone else's database; Delta Sharing is YOU publishing to others.

**Worked example (scenario)**
```sql
CREATE CONNECTION pg TYPE POSTGRESQL
OPTIONS (host 'pg.internal', port '5432', user 'ro', password '...');

CREATE FOREIGN CATALOG pg_sales USING CONNECTION pg
OPTIONS (database 'sales');

SELECT * FROM pg_sales.public.orders LIMIT 5;   -- live, no copy
```

**How the exam asks it**
> Analysts must join live Postgres tables with Delta tables, governed in Unity Catalog, with no data copies.

✅ **A connection + foreign catalog — live federated queries with pushdown, governed by UC.** Traps: "nightly ingestion job" (that's copying, not federation), "a Delta Sharing share of Postgres" (Delta Sharing serves Delta tables), "a JDBC reader configured in every notebook" (ungoverned, repeated setup).""",
        },
    ],
    (
        "professional",
        "Monitoring and Alerting (10%)",
    ): [
        {
            "title": "Concept 1 — System tables: the observability backbone",
            "section": "Monitoring and Alerting (10%)",
            "body": """**Why it matters for the exam:** Your workspace writes its own history into queryable Delta tables. `system.query.history` — every query with durations, user, and result size; `system.access.audit` — every API/UI action, who did it and when; `system.access.lineage` (plus table/column lineage) — what feeds what; `system.billing.usage` — cost per SKU per day. The exam's pattern: "monitor / report / alert on X" — if X is performance, audit evidence, lineage, or cost, the answer starts with the matching system table (often + a SQL alert on top).

**Worked example (scenario)**
```sql
-- heaviest users this week
SELECT user_name, SUM(execution_time_ms)/1000 AS secs
FROM system.query.history
WHERE start_time > now() - INTERVAL 7 DAYS
GROUP BY user_name ORDER BY secs DESC;
```

**How the exam asks it**
> Security needs 90 days of who-did-what evidence; finance needs cost per team.

✅ **system.access.audit for the evidence trail; system.billing.usage for cost — both queryable, retained, Delta.** Traps: "job run history" (jobs only), "cluster event logs" (infrastructure events, not user actions), "export screenshots".""",
        },
        {
            "title": "Concept 2 — SQL alerts and job notifications: who tells you when",
            "section": "Monitoring and Alerting (10%)",
            "body": """**Why it matters for the exam:** Two notification systems, two triggers. A **SQL alert** re-runs a saved query on a schedule and fires when the result crosses a threshold — freshness SLAs, failed-login counts, low stock, cost spikes. A **job notification** attaches to a Lakeflow job and fires on *events* — failure, timeout, start. Both deliver to the same destinations: email, webhook, Microsoft Teams, Slack. Signal words: "result crosses a threshold" → alert; "when the nightly job fails" → job notification.

**Worked example (scenario)**
```sql
-- the alert's query, evaluated every 30 minutes
SELECT 1 WHERE (SELECT max(updated_at) FROM gold.orders)
          < now() - INTERVAL 2 HOURS;
-- configure: trigger "when rows returned > 0" → destination: Slack webhook
```

**How the exam asks it**
> Page the team when data freshness exceeds 2 hours; also notify on any nightly-job failure.

✅ **A SQL alert on the freshness query (threshold) plus a failure notification on the job — both to a webhook/Slack destination.** Traps: "a notebook that loops and emails" (hand-built, fragile), "watch the dashboard" (not automated), "auto-termination" (compute feature, unrelated).""",
        },
        {
            "title": "Concept 3 — Predictive optimization: maintenance that runs itself",
            "section": "Monitoring and Alerting (10%)",
            "body": """**Why it matters for the exam:** Tables degrade: files fragment, old versions pile up. **Predictive optimization** lets Unity Catalog run OPTIMIZE (compaction and clustering maintenance) and VACUUM **automatically** on your **managed** tables, in the background, when it predicts real benefit. You enable it once per catalog (or schema) — then there's no maintenance job to write, schedule, or forget. Scope matters: managed tables only — external tables aren't maintained by it.

**Worked example (scenario)**
```sql
ALTER CATALOG main ENABLE PREDICTIVE OPTIMIZATION;
-- UC now compacts/clusters/VACUUMs managed tables as needed, on its own clock
```

**How the exam asks it**
> Replace the team's manual OPTIMIZE/VACUUM jobs with self-maintaining tables.

✅ **Enable predictive optimization — Unity Catalog runs OPTIMIZE and VACUUM automatically on managed tables.** Traps: "schedule OPTIMIZE in a job" (that's the manual approach you're replacing), "it also works on external tables" (managed only), "liquid clustering replaces it" (different feature — layout, not maintenance).""",
        },
    ],
    (
        "professional",
        "Cost & Performance Optimisation (13%)",
    ): [
        {
            "title": "Concept 1 — AQE: re-planning the query at runtime",
            "section": "Cost & Performance Optimisation (13%)",
            "body": """**Why it matters for the exam:** The optimizer plans a query *before* it knows real data sizes. **Adaptive Query Execution (AQE)** fixes that: at each shuffle boundary it measures the actual data and re-plans the rest. Three things AQE does — memorize them: (1) converts a sort-merge join to a **broadcast** join when one side turns out small; (2) **coalesces shuffle partitions** when data is smaller than planned (fewer, bigger tasks instead of hundreds of tiny ones); (3) **splits skewed join partitions** automatically. It's ON by default on modern runtimes and warehouses — so "enable AQE" is usually a wrong answer; "AQE handles it" is often the right one.

**Worked example (scenario)**
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")          # default true
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")  # default true
```

**How the exam asks it**
> A join ran with hundreds of nearly-empty tasks and stale statistics; what made execution efficient?

✅ **AQE — it measured real sizes at runtime, coalesced shuffle partitions, and converted the join to a broadcast using runtime stats.** Traps: "manually raise shuffle partitions" (more tiny tasks), "recompute statistics" (good practice, but the runtime fix is AQE), "set a broadcast hint" (a tool, but AQE does it automatically).""",
        },
        {
            "title": "Concept 2 — Broadcast joins and skew: the two classic fixes",
            "section": "Cost & Performance Optimisation (13%)",
            "body": """**Why it matters for the exam:** Two opposite join problems, two fixes. Joining a big fact table to a small dimension table? Don't shuffle both — **broadcast** the small one to every executor so each joins locally, zero shuffle. Spark broadcasts automatically below a size threshold, and you can force it with a hint. The opposite problem is **skew** — one key owns most of the rows, so one task runs while the rest finish. Fixes: let AQE split skewed partitions, or **salt** the hot key (add a random 0–N column to both sides, join on key+salt, then re-aggregate).

**Worked example (scenario)**
```python
orders.join(F.broadcast(dims), "dim_id")   # small dims: no shuffle for orders
# skew: add salt = (F.rand() * 8).cast("int") to both sides, join (key, salt), then group by key
```

**How the exam asks it**
> 199 tasks finish in 2 minutes; one task runs 40 minutes on a single customer_id.

✅ **That's skew — fix it with AQE skew handling or by salting the hot key.** Traps: "use a bigger cluster" (the same single hot task remains), "broadcast the big table" (broadcast the SMALL side), "repartition to 1 file" (one task does everything — worse).""",
        },
        {
            "title": "Concept 3 — Deletion vectors + liquid clustering: cheap updates, fast filters",
            "section": "Cost & Performance Optimisation (13%)",
            "body": """**Why it matters for the exam:** Two storage features that pair beautifully. **Deletion vectors**: UPDATE/DELETE/MERGE mark changed rows in a small side file instead of rewriting whole files right now — DML gets dramatically faster (files are rewritten lazily during OPTIMIZE). Enabled by default for Unity Catalog managed tables. **Liquid clustering** (`CLUSTER BY`): incremental, self-tuning data layout for file skipping. Unlike partitioning or ZORDER you never rewrite the whole table to change keys, and it handles high-cardinality columns like customer_id. A modern table is simply `CLUSTER BY (filter columns)`.

**Worked example (scenario)**
```sql
CREATE TABLE orders (
  customer_id STRING,
  ts TIMESTAMP
) USING DELTA CLUSTER BY (customer_id, days(ts));
-- MERGE is cheap (deletion vectors), filters skip files (clustering)
```

**How the exam asks it**
> Heavy MERGE/UPDATE workload plus filters by customer and date — best table layout?

✅ **Liquid clustering on the filter columns with deletion vectors (default) — fast DML and file skipping without over-partitioning.** Traps: "Hive-partition by customer_id" (one folder per customer — millions of tiny folders), "ZORDER with a full rewrite" (legacy, expensive), "turn deletion vectors off" (slows DML back down).""",
        },
        {
            "title": "Concept 4 — Caching, OPTIMIZE, VACUUM: which knob does what",
            "section": "Cost & Performance Optimisation (13%)",
            "body": """**Why it matters for the exam:** Three maintenance tools the exam loves to mix up. **cache/persist** keeps a result in memory/disk for *reuse* — only worth it for results used more than once; caching everything is a memory sink (and meaningless on streams). **OPTIMIZE** compacts small files into big ones and applies clustering — the fix for "400,000 tiny files make everything slow." **VACUUM** deletes files no longer needed by any retained version — that's *storage cleanup* with a retention window (default 7 days), not a performance fix, and it removes time travel beyond the window.

**Worked example (scenario)**
```sql
OPTIMIZE events;                       -- compaction + clustering
VACUUM events RETAIN 168 HOURS;        -- remove expired files (7 days)
```

**How the exam asks it**
> A table has hundreds of thousands of tiny files; queries are slow and storage costs creep up.

✅ **Run OPTIMIZE to compact the files (VACUUM separately cleans expired old versions).** Traps: "VACUUM first" (it never fixes file counts), "cache the table" (memory trick, not a layout fix), "REPARTITION once" (one-off — new writes fragment again).""",
        },
    ],
    (
        "professional",
        "Ensuring Data Security and Compliance (10%)",
    ): [
        {
            "title": "Concept 1 — Row filters and column masks with caller context",
            "section": "Ensuring Data Security and Compliance (10%)",
            "body": """**Why it matters for the exam:** Instead of trusting every query to filter itself, attach the rule to the **table**. A **row filter** is a boolean function bound to a table — each user's queries only see rows where it returns true. A **column mask** is a function bound to a column — it decides what each viewer sees there. Both use *caller context*: `is_account_group_member('hr_payroll')` is true only for the person running the query. Everyone queries the same table; what they get back differs. Bypass-proof because it's enforced in the catalog, not in anyone's SQL.

**Worked example (scenario)**
```sql
CREATE FUNCTION mask_salary(s INT)
RETURN CASE WHEN is_account_group_member('hr_payroll') THEN s ELSE 0 END;

ALTER TABLE employees ALTER COLUMN salary SET MASK mask_salary;
-- every query by non-hr_payroll users sees 0 instead of the real salary
```

**How the exam asks it**
> Salaries visible only to hr_payroll; regional rows only to each regional team.

✅ **A column mask with is_account_group_member for salaries; a row filter function for regions — enforced on the table for every query.** Traps: "one filtered view per team" (N views, easily bypassed), "grant SELECT on a subset of rows" (not possible), "WHERE clauses in the dashboard" (not enforced).""",
        },
        {
            "title": "Concept 2 — Service principals and OAuth: machines get their own identities",
            "section": "Ensuring Data Security and Compliance (10%)",
            "body": """**Why it matters for the exam:** A scheduled job, a pipeline, or a CI/CD flow must log in with no human present. Give the machine its own identity — a **service principal** — and let it authenticate with **OAuth machine-to-machine credentials** (client id + secret), granted exactly the rights it needs and nothing more. The trap answers revolve around personal access tokens: tied to one human, usually over-privileged, expiring silently, and dying the day that person leaves.

**Worked example (scenario)**
```bash
databricks service-principals create --display-name "prod-etl"
# grant it exactly: USE catalog + USE schema + MODIFY on target tables
databricks bundle deploy --target prod   # CI authenticates as the SP
```

**How the exam asks it**
> CI deploys nightly; credentials must not depend on any employee's account.

✅ **A service principal with OAuth M2M credentials, least-privilege grants, secret stored in CI.** Traps: "the lead engineer's PAT in a GitHub secret", "a shared admin account", "a personal login used headlessly".""",
        },
        {
            "title": "Concept 3 — Compliance evidence: least privilege plus the audit trail",
            "section": "Ensuring Data Security and Compliance (10%)",
            "body": """**Why it matters for the exam:** Compliance questions have two halves. **Prevent** — least privilege: each group gets exactly the grants it needs, through the one central permission model (Unity Catalog). **Prove** — the audit trail: `system.access.audit` records who accessed or changed what, and when, in queryable Delta. The exam pairs them: when a scenario wants both "limited access" and "evidence for the auditor," the answer combines scoped grants with audit tables — not screenshots, not policy documents.

**Worked example (scenario)**
```sql
GRANT USE CATALOG ON CATALOG sales TO GROUP sales_analysts;
GRANT USE SCHEMA ON SCHEMA sales.gold TO GROUP sales_analysts;
GRANT SELECT ON TABLE sales.gold.orders TO GROUP sales_analysts;

SELECT event_time, user_name, action_name FROM system.access.audit
WHERE service_name = 'UnityCatalog' AND event_date > current_date() - 90;
```

**How the exam asks it**
> An auditor asks how access is limited and what each analyst actually queried over 90 days.

✅ **Least-privilege grants in Unity Catalog + system.access.audit as the queryable evidence.** Traps: "workspace-local ACLs" (the pre-UC model), "a written access policy document" (not evidence), "job run history" (runs, not data access).""",
        },
    ],
    (
        "professional",
        "Data Governance (7%)",
    ): [
        {
            "title": "Concept 1 — Lineage, ownership, and grants: the governance triangle",
            "section": "Data Governance (7%)",
            "body": """**Why it matters for the exam:** Unity Catalog gives you three automatic superpowers. **Lineage** — every table-to-table (even column-to-column) dependency is captured as queries run; use it before renaming a column to find every downstream consumer in one place. **Ownership** — every object has exactly one owner who can manage and grant on it; production objects should be owned by a **service principal**, not a person, so nothing breaks when someone leaves. **Grants** — readers always need the chain: USE CATALOG → USE SCHEMA → SELECT.

**Worked example (scenario)**
```sql
-- who consumes gold.orders.cust_id? lineage shows it (UI or system.access.column_lineage)
-- owner leaving: transfer to a service principal
ALTER TABLE gold.orders OWNER TO `app-prod-etl`;
```

**How the exam asks it**
> Rename a column safely; also, the owner of every production table is leaving the company.

✅ **Check column-level lineage first, then rename; transfer ownership to a service principal.** Traps: "email the team and ask" (incomplete and stale), "keep a departing owner's token alive" (exactly the anti-pattern), "rebuild the tables" (ownership can be transferred, not rebuilt).""",
        },
        {
            "title": "Concept 2 — Tags: classification that travels with the data",
            "section": "Data Governance (7%)",
            "body": """**Why it matters for the exam:** Tags are key-value labels you attach to catalogs, schemas, tables, and even columns — and they're **inherited downward** (a schema tag covers its tables). They power classification at scale: mark PII (`pii=true`), sensitivity levels, or environment, consistently and centrally — and you can then query, report, and drive policy off tags. That's the exam's contrast: a naming convention or a column comment is free-text hope; a tag is structured, inherited, and queryable.

**Worked example (scenario)**
```sql
ALTER TABLE hr.payroll MODIFY COLUMN amount SET TAGS ('pii' = 'true', 'sensitivity' = 'high');

-- find every PII column across the workspace:
SELECT * FROM system.information_schema.tags WHERE tag_key = 'pii';
```

**How the exam asks it**
> Consistently mark every object containing PII across hundreds of tables and columns.

✅ **Unity Catalog tags — set at column/table level, inherited down, queryable via information_schema.** Traps: "a pii_ column-name prefix" (convention, not governance), "column comments" (free text, not queryable), "a spreadsheet listing PII tables" (goes stale immediately).""",
        },
    ],
    (
        "professional",
        "Debugging and Deploying (10%)",
    ): [
        {
            "title": "Concept 1 — Query profile and EXPLAIN: read the plan, find the cost",
            "section": "Debugging and Deploying (10%)",
            "body": """**Why it matters for the exam:** When a query is slow, don't guess — read its plan. The **query profile** (SQL editor / Lakeview / Spark UI) and `EXPLAIN` show the physical plan: where data is **shuffled** (every exchange moves bytes across the network), whether a join **broadcast** or sorted-merged, how many rows each operator emits, and per-stage timings. Reading order that finds most problems: find the exchanges (shuffles) — that's data movement; find the longest bar in the profile — that's where time goes; compare rows in vs out per operator — a row explosion (join fan-out or explode) shows up right there.

**Worked example (scenario)**
```sql
EXPLAIN SELECT o.*, d.name FROM orders o JOIN dims d ON o.dim_id = d.id;
-- plan shows: BroadcastHashJoin (good) vs SortMergeJoin + shuffle (investigate)
-- profile shows: stage 3 = 82% of total time
```

**How the exam asks it**
> Determine why a query takes minutes and where the time goes.

✅ **Open the query profile — per-stage timings, shuffle sizes, and join strategy pinpoint the cost.** Traps: "restart the warehouse" (no diagnosis), "scale up compute" (untreated symptom), "read the notebook logs" (app logs, not query plans).""",
        },
        {
            "title": "Concept 2 — Declarative Automation Bundles: one source, many environments",
            "section": "Debugging and Deploying (10%)",
            "body": """**Why it matters for the exam:** A bundle (databricks.yml) declares your jobs, pipelines, and files as code. **Targets** (dev/test/prod) swap per-environment values with one command: `bundle deploy --target prod` deploys prod names, catalog, schedule, and permissions; `--target dev` deploys yours. **Variables** parameterize the differences; **run-as** and permissions are per-target — e.g., prod runs as a service principal while dev runs as you. And the golden rule: if the bundle owns a resource, its settings live in the bundle (through Git) — never edited in the UI, where a deploy would silently revert them.

**Worked example (scenario)**
```yaml
variables: { catalog: main }
targets:
  dev:  { variables: { catalog: dev_cat } }
  prod: { variables: { catalog: prod_cat }, run_as: { service_principal_name: etl-sp } }
```

**How the exam asks it**
> One pipeline definition, two catalogs, prod scheduled and owned by a service principal.

✅ **One bundle with per-target variables, run-as, and permissions — deploy with --target.** Traps: "two copies of the files edited by hand", "hardcode prod and rewrite for dev", "deploy --force after editing in the UI" (no such merge — the bundle wins).""",
        },
        {
            "title": "Concept 3 — Debugging streams and pipelines: read the metrics, not vibes",
            "section": "Debugging and Deploying (10%)",
            "body": """**Why it matters for the exam:** When a stream "runs but nothing updates," the truth is in its metrics — not in restarting things. Streaming **progress** (per micro-batch) reports input rows, processed rows, batch duration, and state size — input rising while processed is flat means falling behind; state growing means a missing watermark. For Lakeflow pipelines, the **event log** plus the dataset view show expectation pass rates, backlogs, and refresh history. And remember the boundary: streaming tables are managed by their pipeline — direct DML against them is an error.

**Worked example (scenario)**
```python
q = (spark.readStream.table("events")
     .writeStream.option("checkpointLocation", chk).toTable("silver"))
q.lastProgress   # batch duration, inputRowsPerSecond, stateOperators — the truth
```

**How the exam asks it**
> A streaming job shows RUNNING but the target table hasn't updated in hours. First step?

✅ **Read the streaming progress metrics (input vs processed rows, batch duration, state size) — or the pipeline event log — to see what it's actually doing.** Traps: "restart and watch" (no evidence), "delete the checkpoint" (reprocesses and duplicates), "increase the trigger rate" (masks the symptom).""",
        },
    ],
    (
        "genai",
        "Generative AI Concepts (25%)",
    ): [
        {
            "title": "Concept 1 — How an LLM actually works: predicting the next token",
            "section": "Generative AI Concepts (25%)",
            "body": """**Why it matters for the exam:** An LLM is not a database you query — it's a next-token predictor. It reads your prompt as a sequence of **tokens** (word fragments), and generates a continuation, one token at a time, each time picking from a probability distribution. Two settings control that picking: **temperature** — how adventurous the choices are (0 = nearly deterministic, same prompt ≈ same answer; high = creative/varied), and the **context window** — how many tokens of prompt + history the model can "see" at once; exceed it and the earliest content falls out of scope. Exam questions describe a behavior ("answers vary wildly at temperature 1.0", "the model forgot the first instruction") and expect you to name the cause (temperature, context window).

**Worked example (scenario)**
```text
Prompt: "Summarize this contract in 3 bullets."
Temperature 0 → same 3 bullets every run (reliable pipelines)
Temperature 0.8+ → different phrasing/runs (creative writing)
Long chat → "the model ignored my first instruction" = context window overflow
```

**How the exam asks it**
> A production agent must give stable, repeatable answers. Which setting guarantees that?

✅ **Low temperature (≈0) — near-deterministic token choice; and keep prompts within the context window so nothing "falls out".** Traps: "increase the context window so answers stabilize" (unrelated), "rerun and majority-vote" (wasteful workaround), "temperature 1.0 with a stronger model" (still varies).""",
        },
        {
            "title": "Concept 2 — Hallucination and grounding: why RAG exists",
            "section": "Generative AI Concepts (25%)",
            "body": """**Why it matters for the exam:** An LLM always produces *fluent* text — even when it has no idea what the truth is. A confident, invented answer is a **hallucination**, and it happens because the model only knows what it saw during training, not your private data. The cure is **grounding**: give the model the relevant facts in the prompt and instruct it to answer only from them. That's the whole idea of **RAG (retrieval-augmented generation)** — retrieve relevant documents, put them in the context, generate the answer from them. Know the distinction: fine-tuning teaches style/domain language; RAG supplies facts. Fresh data → RAG, not fine-tuning.

**Worked example (scenario)**
```text
RAG flow: user asks → search docs for relevant chunks →
prompt = "Answer ONLY from the provided context: [chunks]" → grounded answer.
No retrieval: model invents a plausible policy number = hallucination.
```

**How the exam asks it**
> A support bot keeps inventing refund policies. What fixes it?

✅ **Ground the answers with RAG — retrieve the real policy text and instruct the model to answer only from it.** Traps: "fine-tune on policy documents" (teaches language, not facts — goes stale), "lower the temperature" (fluent lies stay fluent), "add a disclaimer" (hides the problem).""",
        },
        {
            "title": "Concept 3 — Embeddings: meaning as coordinates",
            "section": "Generative AI Concepts (25%)",
            "body": """**Why it matters for the exam:** An **embedding model** converts any piece of text into a long list of numbers (a vector) with a magical property: texts with similar *meaning* end up close together in that number-space — even when they share no words. "How do I reset my password?" sits near "password recovery steps". This is what makes semantic search possible: instead of matching keywords, you match *meaning* by comparing vectors. This concept powers Vector Search, RAG retrieval, and similarity recommendations — the exam asks you to recognize when embeddings are the tool.

**Worked example (scenario)**
```text
"The battery lasts ten hours"  → [0.12, -0.53, 0.88, ...]
"Battery life is excellent"   → nearby vector  (similar meaning)
"The screen is bright"        → far vector     (different topic)
Search = find stored vectors nearest the question's vector.
```

**How the exam asks it**
> Users search "expense reimbursement" but the policy is titled "employee cost claims". Keyword search finds nothing. What enables the match?

✅ **Embeddings — semantic similarity search matches meaning, not words.** Traps: "a keyword index with synonyms" (manual, brittle), "ask the LLM to guess" (hallucination), "fine-tuning" (different purpose).""",
        },
        {
            "title": "Concept 4 — Chunking: how you cut documents decides what you find",
            "section": "Generative AI Concepts (25%)",
            "body": """**Why it matters for the exam:** RAG can only retrieve what a retriever can *see*, and it sees **chunks** — the pieces you split documents into. Split too small: each chunk loses context, the retriever returns fragments that don't answer anything. Split too big: each chunk buries the answer in noise and the context window fills with filler. Good defaults: chunk by structure (headings, sections, paragraphs) with some overlap, so no idea gets cut in half. The exam frames it as a symptom — "retrieved chunks lack context" (too small) or "chunks exceed the context budget" (too big).

**Worked example (scenario)**
```text
A 40-page policy → split at each section heading,
chunks of ~400-600 tokens with 50-100 tokens overlap.
A table split mid-row = unretrievable; keep tables whole.
```

**How the exam asks it**
> Retrieval returns text that contains the answer but the LLM says it can't find it. Why?

✅ **Chunking problem — likely chunks are too small or split mid-idea, so the retrieved chunk lacks the needed context; re-chunk with structure + overlap.** Traps: "raise the temperature" (unrelated), "a bigger model" (doesn't fix retrieval), "index the raw file whole" (one giant chunk = noise).""",
        },
    ],
    (
        "genai",
        "The Databricks AI Platform (10%)",
    ): [
        {
            "title": "Concept 1 — Model Serving endpoints: the three kinds of models",
            "section": "The Databricks AI Platform (10%)",
            "body": """**Why it matters for the exam:** Everything on Databricks AI is served through a **Model Serving endpoint** — but exams test what's *behind* it. Three flavors: **foundation models** (Databricks-hosted — pay per token, no infrastructure), **external models** (OpenAI, Anthropic, etc. — via gateway, keys stored for you), **custom models** (your own, from MLflow — provisioned compute with autoscaling, all-inclusive hourly cost). Cost model differs: token-based vs compute-based — a classic exam pairing. And in SQL, **`ai_query`** sends table data through any of these endpoints from a plain SELECT — AI functions without leaving the warehouse.

**Worked example (scenario)**
```sql
-- sentiment over a table column via a serving endpoint
SELECT ai_query('databricks-meta-llama-3-1-8b', review_text,
                 returnType => 'BOOLEAN') AS positive
FROM product_reviews;
```

**How the exam asks it**
> Score 10M review rows with an LLM from SQL — what's the mechanism?

✅ **ai_query against a Model Serving endpoint — batch LLM calls straight from SQL.** Traps: "export to pandas and loop" (no scale), "copy reviews into the chat UI" (manual), "build a Spark UDF that calls a laptop" (no).""",
        },
        {
            "title": "Concept 2 — AI Playground and Agent Builder: the no-code ladder",
            "section": "The Databricks AI Platform (10%)",
            "body": """**Why it matters for the exam:** Databricks gives you a ladder from no-code to full-code. The **AI Playground** — chat with foundation models, try prompts and parameters, compare models side by side. **Agent Builder / Mosaic AI Agent Studio** — build a full RAG chatbot or agent visually: pick a foundation model, attach knowledge sources (volumes, tables, sites), add tools and index them, then chat, debug, and eventually **deploy** it as a serving endpoint. The exam wants you to know where each tool sits: prototype a prompt → playground; build a grounded assistant without writing code → Agent Builder; full custom logic in Python → the Agent Framework.

**Worked example (scenario)**
```text
Playground → "which model handles summarization best?" (compare + tune)
Agent Builder → grounded HR-policy Q&A bot over a UC volume of PDFs,
                no code, deploy → endpoint 'hr-assistant'
```

**How the exam asks it**
> An analyst needs a Q&A bot over 50 policy PDFs with zero coding.

✅ **Agent Builder (Mosaic AI Agent Studio) — attach the volume as knowledge, deploy the assistant.** Traps: "write a LangChain app" (code — unnecessary), "use the playground" (chat only, not deployable, no attached knowledge at scale), "fine-tune a model on the PDFs" (wrong tool).""",
        },
    ],
    (
        "genai",
        "Mosaic AI Vector Search (15%)",
    ): [
        {
            "title": "Concept 1 — Vector Search endpoints and the two index types",
            "section": "Mosaic AI Vector Search (15%)",
            "body": """**Why it matters for the exam:** Semantic search needs two pieces: a **Vector Search endpoint** (the compute that runs embeddings + similarity search) and, on top of it, an **index**. Two index types, one exam-favorite distinction: a **Delta table sync index** — pointed at a source Delta table, it *automatically stays in sync* as the table changes (continuous or triggered sync), embeddings computed for you; best when your corpus is a table that updates. A **direct vector index** — you supply vectors and metadata yourself (e.g., from your own embedding model); best for full control. Symptom language: "index is stale" → sync mode/triggered, or manual index without sync.

**Worked example (scenario)**
```sql
CREATE VECTOR INDEX catalog.schema.docs_idx
ENDPOINT catalog.schema.vs_endpoint
AS SELECT id, text FROM catalog.schema.docs;  -- Delta-synced index
-- table changes → index refreshes automatically (continuous sync)
```

**How the exam asks it**
> Search results must always reflect the latest rows in the documents table — which index setup?

✅ **A Delta table sync index — automatic sync (continuous or triggered) keeps it current.** Traps: "a direct vector index reloaded nightly by a job" (manual, goes stale), "query the Delta table directly with LIKE" (not semantic), "recreate the index on a schedule" (wasteful).""",
        },
        {
            "title": "Concept 2 — Hybrid search and filters: precision on top of meaning",
            "section": "Mosaic AI Vector Search (15%)",
            "body": """**Why it matters for the exam:** Pure vector search finds *meaning* but can miss exact terms (product codes, error strings). **Hybrid search** combines keyword (BM25) and vector similarity — you get both the exact match and the meaning match. And because search returns *candidates*, you can constrain them with **metadata filters** (only 2025 documents, only this product line) — filtering happens inside the index, so results are both relevant AND compliant. Know the knobs: `num_results` (how many chunks back), similarity threshold, filters, hybrid on/off.

**Worked example (scenario)**
```python
results = vs_index.similarity_search(
    query_text="error 429 rate limit billing",
    columns=["id", "text"],
    num_results=5,
    filters={"year": {">": 2024}, "product": "serving"},  # pre-filter
)
```

**How the exam asks it**
> Users complain exact error codes are never found, though related docs are. Fix?

✅ **Enable hybrid search — keyword matching catches exact codes while vectors carry the semantic matches.** Traps: "more results (num_results=100)" (noise, not precision), "retrain the embedding model" (wrong lever), "remove filters" (loosens governance).""",
        },
        {
            "title": "Concept 3 — What the embedding pipeline does for you",
            "section": "Mosaic AI Vector Search (15%)",
            "body": """**Why it matters for the exam:** A Delta-sync index doesn't just copy rows — a managed **embedding pipeline** computes the embeddings for you (with a chosen embedding model) and keeps them updated on sync. The practical consequences the exam tests: you do NOT run your own embedding job for a synced index (the pipeline does it); you DO choose the embedding model (the same query and corpus must use the same model — mixed models produce meaningless distances); and sync modes are continuous (near-real-time, higher cost) vs triggered (refresh on demand). Symptom: "query embeddings were made with model A, corpus with model B, results are garbage" — that's why.

**Worked example (scenario)**
```text
Corpus embedded with bge-large   → query embedded with bge-large  ✓
Corpus bge-large, query MiniLM   → vectors live in different spaces, similarity meaningless ✗
Switch embedding model           → re-embed the whole corpus (rebuild index)
```

**How the exam asks it**
> A synced index returns nonsense results after an app switched its query-side embedding model. Why?

✅ **Query and corpus embeddings must come from the same embedding model — mismatched models break similarity; re-embed consistently.** Traps: "increase num_results" (garbage × 5), "enable hybrid search" (band-aid), "lower the temperature" (unrelated).""",
        },
    ],
    (
        "genai",
        "Mosaic AI Agent Framework (20%)",
    ): [
        {
            "title": "Concept 1 — Agents and tools: planner hands out the work",
            "section": "Mosaic AI Agent Framework (20%)",
            "body": """**Why it matters for the exam:** A plain LLM can only answer from what's in the prompt. An **agent** is an LLM put in a loop: it reasons, calls **tools** (functions you gave it), reads results, and repeats until it can answer. The LLM acts as the *driver* — the planner deciding what to do next — and the tools are the hands: a UC function, a serving endpoint, a vector search, a parameterized SQL query, a Genie space. On Databricks the exam's core pattern: give the agent **governed** tools (Unity Catalog functions) so it can act on data with real permissions instead of guessing. Symptom language: "the agent invents a refund status" → it lacked a tool, so it hallucinated one.

**Worked example (scenario)**
```text
User: "What's my order #4471 status?"
Driver LLM: calls tool get_order_status(order_id=4471)   # UC function
Tool returns: shipped 2026-11-20
Driver LLM: answers from the tool's real output — grounded, no guess.
```

**How the exam asks it**
> A chatbot must check live order data instead of guessing. What completes the agent?

✅ **Attach a tool — e.g., a Unity Catalog function that queries the order table — so the driver LLM calls it and answers from real results.** Traps: "fine-tune on order data" (facts go stale, no live lookup), "paste the whole table in the prompt" (context window + freshness), "increase temperature" (worse).""",
        },
        {
            "title": "Concept 2 — Agent Builder vs code-first: choosing your lane",
            "section": "Mosaic AI Agent Framework (20%)",
            "body": """**Why it matters for the exam:** Databricks offers two lanes for the same destination. **Agent Builder (no-code/low-code)** — assemble a RAG assistant or agent in the UI: pick a model, add knowledge (volumes, URLs, tables), attach tools, test in the built-in chat, deploy when ready. **Code-first (Mosaic AI Agent Framework, LangChain/LangGraph)** — write the agent in Python notebooks with full control over prompts, logic, and loops. The exam tests the boundary: quick grounded Q&A, no engineers → Agent Builder; custom multi-step logic, custom tool orchestration, custom evaluation pipelines → code-first. Both end the same way: **deploy as a Model Serving endpoint**.

**Worked example (scenario)**
```text
HR-policy Q&A over PDFs, team of analysts      → Agent Builder, deploy UI
Complex multi-tool insurance-claims agent,
custom guardrails + eval harness in CI         → code-first agent framework
```

**How the exam asks it**
> Which approach for a no-code team, and which for custom agent logic?

✅ **Agent Builder for the no-code grounded assistant; code-first Agent Framework (LangChain/LangGraph in notebooks) for custom logic.** Traps: "LangChain for everyone" (unnecessary code for simple RAG), "the playground deploys agents" (it doesn't — it's for prompts), "AI Gateway builds agents" (different product — gateway governs traffic).""",
        },
        {
            "title": "Concept 3 — Structured outputs and Genie spaces as tools",
            "section": "Mosaic AI Agent Framework (20%)",
            "body": """**Why it matters for the exam:** Two details that show up as scenario twists. **Structured outputs**: you can force the model to answer in a schema (valid JSON / typed fields) instead of prose — essential when a program consumes the output; validate against the schema, reject-and-retry on violations. **Genie spaces as tools**: an agent can call an entire *Genie space* (a curated natural-language interface over trusted data) as one tool — the space does governed, curated SQL on its certified tables, the agent gets clean answers. That's the "reuse the governed semantic layer instead of raw table access" pattern.

**Worked example (scenario)**
```text
Agent step: extract_claim(fields) → expects
{ "claim_id": "int", "amount": "float", "deny": "bool" }
Model returns prose → schema validation fails → retry with the schema restated.
Sales agent → tool: genie_space('revenue_qa') → governed answer from certified metrics.
```

**How the exam asks it**
> A downstream system consumes the agent's reply as JSON; and the agent must answer sales numbers the way the finance Genie space defines them.

✅ **Structured (schema-validated) outputs for the JSON consumer; attach the Genie space as a tool for governed metrics.** Traps: "ask politely for JSON" (unvalidated — breaks), "grant the agent raw table access" (ungoverned, reinventing Genie), "fine-tune for format" (overkill vs schema enforcement).""",
        },
        {
            "title": "Concept 4 — The review app and debugging agent steps",
            "section": "Mosaic AI Agent Framework (20%)",
            "body": """**Why it matters for the exam:** When an agent answers badly, the fix starts with *seeing* what it did. The **review app** (and playground / MLflow traces) shows every step of a run: the exact prompts sent, the tools called with which arguments, what each tool returned, and the final answer. You debug agents like you debug code — step by step — not by guessing at the prompt. Common findings: the retrieval step returned bad chunks (fix chunking/index), the tool got wrong arguments (fix the tool description), the driver skipped a tool (prompt/design).

**Worked example (scenario)**
```text
Bad answer → open the trace:
 step 1 retrieval: returned 3 irrelevant chunks   ← fix: chunking / filters
 step 2 driver: never called the pricing tool      ← fix: tool description
 step 3 answer: invented pricing                   ← hallucination from 1+2
```

**How the exam asks it**
> An agent gives wrong answers. Where do you look first?

✅ **The run's trace / review app — inspect each step (retrieval, tool calls, prompts) to find the failing stage before changing anything.** Traps: "rewrite the system prompt blindly" (guessing), "swap the foundation model" (masks the real step), "add more documents" (may worsen retrieval noise).""",
        },
    ],
    (
        "genai",
        "Mosaic AI Agent Evaluation (10%)",
    ): [
        {
            "title": "Concept 1 — Evaluation sets: the exam your agent must pass",
            "section": "Mosaic AI Agent Evaluation (10%)",
            "body": """**Why it matters for the exam:** You can't improve an agent you don't measure. An **evaluation set** is a table of test cases — each with a request, the *expected response* (or *expected facts* that any good answer must contain), and optional **guidelines** the answer should follow ("cite the policy", "no medical advice"). Run the agent over the set, compare its answers to the expectations, get scores. This is how you prove a prompt/model change *helped* rather than guessing. Change one thing at a time, re-run the eval set, compare — that's the scientific loop the exam describes.

**Worked example (scenario)**
```text
Evaluation set rows:
 request: "What's the maternity leave policy?"
 expected_facts: ["26 weeks", "applies after 1 year"]
 guideline: "cite the policy section"
→ agent's answer scored against facts + guideline; before/after model swap compared on the SAME set.
```

**How the exam asks it**
> Two foundation models are candidates for the agent; choose objectively.

✅ **Run both against the same evaluation set and compare scores — identical requests, expected facts, and guidelines.** Traps: "ask the team which feels better" (vibes), "chat with each for an hour" (anecdotal), "pick the cheaper one" (no quality signal).""",
        },
        {
            "title": "Concept 2 — AI judges and deterministic metrics",
            "section": "Mosaic AI Agent Evaluation (10%)",
            "body": """**Why it matters for the exam:** Two kinds of metrics, and the exam expects you to know which is which. **Deterministic metrics** are computed exactly, no LLM opinion: did retrieval return the expected documents (retrieval precision/recall), did the answer contain the required fact, latency, cost. **LLM-as-judge** metrics use a model to grade qualities a string-match can't: *correctness* (is the answer right, compared to ground truth), *groundedness* (is every claim supported by the retrieved context). Judges are validated models with rubrics; they're good but graders, not oracles — pair them with deterministic checks. Symptom: "the answer is factually right but cites nothing from the context" → groundedness failed.

**Worked example (scenario)**
```text
Deterministic: expected doc in top-5 retrieved? YES → retrieval precision 1.0
LLM judge:    "Is the answer correct vs the expected response?" → 4/5
LLM judge:    "Is every claim supported by the provided chunks?" → 1/5 (invented citation)
```

**How the exam asks it**
> Verify both that answers are correct AND fully based on retrieved context — which metrics?

✅ **Correctness (LLM judge vs expected response) plus groundedness (judge checks claims against retrieved context) — with deterministic retrieval metrics alongside.** Traps: "exact string match on the answer" (LLMs never match verbatim), "latency only" (not quality), "human review of every answer" (doesn't scale — judges exist for this).""",
        },
    ],
    (
        "genai",
        "Fine-tuning and Custom Models (10%)",
    ): [
        {
            "title": "Concept 1 — The fine-tuning approaches and when to use them",
            "section": "Fine-tuning and Custom Models (10%)",
            "body": """**Why it matters for the exam:** Fine-tuning takes a pretrained model and continues training on *your* data. Know the three flavors: **continued pretraining** — feed domain text (medical journals, legal contracts) so the model speaks your language; **supervised fine-tuning** — input/output pairs so the model learns your *task and format*; **preference optimization (DPO)** — chosen vs rejected pairs so the model learns your *taste* (tone, safety). The exam's golden rule: fine-tuning teaches **behavior and style** — facts still belong to RAG. "Model must always answer in our XML format" → fine-tune. "Model must know this year's policy" → RAG.

**Worked example (scenario)**
```text
Domain vocabulary (clinical notes)      → continued pretraining
"Always reply as strict JSON schema X"  → supervised fine-tuning
"Prefer concise, cautious answers"      → preference optimization (DPO)
"Know the 2026 refund policy"          → RAG (fine-tuning can't stay fresh)
```

**How the exam asks it**
> The model must always emit the company's strict output format and use internal jargon. Which technique?

✅ **Supervised fine-tuning on input/output examples (plus continued pretraining for the jargon) — format and style are learned behavior.** Traps: "RAG with format examples in the prompt" (helps, brittle — not guaranteed), "higher temperature" (opposite), "a bigger foundation model" (still won't be consistent).""",
        },
        {
            "title": "Concept 2 — Fine-tuning workflow: data → run → register → serve",
            "section": "Fine-tuning and Custom Models (10%)",
            "body": """**Why it matters for the exam:** The lifecycle has four steps, each a potential exam detail. **Data prep** — training data lives in UC tables/volumes (the exam likes that data stays governed). **Training run** — launched from the UI or API, tracked in MLflow with its params and metrics; monitor the loss curve, not vibes. **Register** — the finished model goes into Unity Catalog (versioned, permissioned like any asset). **Serve** — deploy as a custom-model endpoint with autoscaling. And the guardrail: always evaluate the fine-tuned model against the base model on the same eval set — a fine-tune that scores worse is a failed experiment, not progress.

**Worked example (scenario)**
```text
training_data in catalog.schema.volume → run finetune_claims_v2 (MLflow)
loss plateaus → register model 'claims-model' v2 in UC
→ deploy endpoint claims-model-v2 → evaluate vs base on eval set BEFORE rollout
```

**How the exam asks it**
> After a fine-tune completes, what must happen before users get the new model?

✅ **Register it in UC, deploy to an endpoint, and evaluate it against the current model on the same evaluation set.** Traps: "deploy immediately — newer is better" (unproven), "keep the weights on the engineer's laptop" (ungoverned), "compare on new test questions each time" (unfair comparison — same eval set).""",
        },
    ],
    (
        "genai",
        "AI Governance and Deployment (10%)",
    ): [
        {
            "title": "Concept 1 — Mosaic AI Gateway: traffic control for AI",
            "section": "AI Governance and Deployment (10%)",
            "body": """**Why it matters for the exam:** The **AI Gateway** sits between your apps and any model (Databricks-hosted, external, custom) and controls the traffic: **rate limits** (cap requests/tokens per user or per minute — protect budgets from runaway loops), **routing** (send requests to model destinations, split or fall back when one fails), **guardrails** (safety/PII filtering on inputs and outputs), and **usage tracking** (every call logged for cost and audit). The exam's story shape: an app burns tokens because a retry loop loops forever → rate limit fixes it; two teams fight over one external model's quota → route/limit per team.

**Worked example (scenario)**
```text
Config: rate limit 20 requests/min per principal
       + fallback to a smaller model on 429s
       + PII guardrail on inputs
       → one runaway app can't melt the budget; failures degrade, not die.
```

**How the exam asks it**
> A buggy agent entered an infinite loop and spent the day calling the LLM. What prevents a repeat?

✅ **AI Gateway rate limits — request/token caps per principal stop runaway consumption.** Traps: "fix the bug" (necessary but not a control), "rotate the API key" (doesn't limit), "ask the provider for a refund" (not engineering).""",
        },
        {
            "title": "Concept 2 — Governing AI assets: everything is a UC object",
            "section": "AI Governance and Deployment (10%)",
            "body": """**Why it matters for the exam:** The governance move to remember: on Databricks, AI assets are **Unity Catalog objects** — models are registered in UC, agents can be UC functions, knowledge lives in UC tables and volumes, endpoints have owners and permissions. Consequences the exam tests: access follows UC grants (a team that can't SELECT the source table can't have an agent leak it — tools run with the *user's* permissions, not god-mode); secrets (API keys for external models) go in **secret scopes**, never in code or notebooks; and lineage/audit extend to AI assets like everything else.

**Worked example (scenario)**
```text
Agent tool = UC function read_claims:
  analyst has SELECT on claims     → tool returns rows
  auditor lacks SELECT              → same tool returns an access error
The agent inherits the caller's permissions — data governance = AI governance.
```

**How the exam asks it**
> A user must get agent answers only over data they're allowed to see. What makes that true?

✅ **Tools and agents run under the user's own Unity Catalog permissions — grants govern what the agent can touch.** Traps: "give the agent its own admin credentials" (god-mode — exactly wrong), "mask the answers in the UI" (unbypassable? no), "a prompt asking the agent to behave" (not enforcement).""",
        },
        {
            "title": "Concept 3 — Deploying AI: agents as endpoints, external models, BYO keys",
            "section": "AI Governance and Deployment (10%)",
            "body": """**Why it matters for the exam:** Deployment patterns to keep straight. **Agents and assistants deploy to Model Serving endpoints** — the same mechanism as models: versioned, autoscaled, permissioned, called via REST by any app. **External models** (OpenAI, Anthropic, …) are served through Databricks-endpoint-fronting so the workspace keeps audit, rate limits, and guardrails — instead of every notebook calling out with its own key. **BYO keys/providers**: you can point Databricks at your own provider accounts; either way, keys live in **secret scopes** and are referenced by name, never embedded in code. Exam signal: "stop scattering API keys across notebooks" → external model endpoint + secret scope.

**Worked example (scenario)**
```text
dbutils.secrets.get('openai', 'api_key') → configured once on a gateway/endpoint
notebooks call endpoint 'llm-openai' — no keys anywhere in code,
every call audited, rate-limited, and logged.
```

**How the exam asks it**
> External-model API keys are currently pasted into many notebooks. What's the governed fix?

✅ **Serve the external model behind a Databricks endpoint with the key in a secret scope — one governed, audited front door.** Traps: "a shared keys spreadsheet" (no), "hardcode keys as notebook variables" (the problem, restated), "each user registers their own model" (ungoverned sprawl).""",
        },
    ],
}


# Official exam sections ("The exam covers") parsed from the guide concepts.
SECTIONS = {
    lvl: [ln[5:].strip() for ln in CONCEPTS[lvl].splitlines() if ln.startswith("#### ")]
    for lvl in CONCEPTS
}

# Official exam weightings parsed from the domain labels, used to weight mock
# exam results like the real exam does.
OFFICIAL_WEIGHTS = {
    lbl: int(lbl.split("(")[-1].split("%")[0])
    for doms in SECTIONS.values()
    for lbl in doms
}


def _short_title(text):
    """Derive a short concept title from a guide bullet."""
    cut = text.split(":")[0].split(" — ")[0].split(" (`")[0]
    return cut if 3 <= len(cut) <= 60 else (text[:57] + "…")


def get_concepts(level, domain):
    """Concept list for a cert level + domain. Detailed lessons override the
    guide bullets; everything else falls back to the guide concepts."""
    level = level or "associate"
    if domain in (None, "All"):
        if level in LESSONS_BY_LEVEL:
            doms = SECTIONS.get(level, [])
            out = []
            for d in doms:
                out.extend(get_concepts(level, d))
            return out
        domain = "All"
    if (level, domain) in LESSONS:
        return LESSONS[(level, domain)]
    out = []
    current = None
    for ln in CONCEPTS[level].splitlines():
        if ln.startswith("#### "):
            current = ln[5:].strip()
        elif ln.startswith("* ") and current is not None and domain in ("All", current):
            text = ln[2:].strip()
            out.append({"title": _short_title(text), "section": current, "body": text})
    if not out:
        out = [{"title": domain, "section": domain,
               "body": "Detailed lessons for this domain are coming soon."}]
    return out


LESSONS_BY_LEVEL = {lvl for (lvl, _d) in LESSONS}


def build_prep_guide(level):
    """Assemble the full AI Prep Guide page for a certification level."""
    label = next(l["label"] for l in LEVELS if l["value"] == level)
    return "\n".join(
        [
            "# AI Prep Guide — Databricks Certification",
            "",
            "Any AI tool you already use. Any Databricks exam. The steps below "
            "are the official guide, with **Steps 2–5 executed for "
            + label
            + "**.",
            "",
            "## Step 1 — Set up",
            "",
            STEP1,
            "",
            "## Step 2 — Prime your AI (paste this every session)",
            "",
            "> " + PRIME_PROMPT,
            "",
            "## Step 3 — Find your exam's \"renamed product\" traps (5 minutes, once)",
            "",
            STEP3,
            "",
            "## Step 4 — Run this 5-step loop, one section at a time",
            "",
            STEP4_LOOP,
            CONCEPTS[level],
            "",
            "## Step 5 — Hands-on minimum (CRITICAL)",
            "",
            HANDSON[level],
            "",
            "## Step 6 — Suggested pace",
            "",
            PACE,
        ]
    )


# ------------------------------------------------------------------ layout
app = Dash(__name__, title="Databricks Cert Prep", update_title=None)
server = app.server

# Inject the stylesheet through the page template (dash.html has no Style
# component in the installed Dash version).
app.index_string = (
    "<!DOCTYPE html>\n<html>\n<head>\n{%metas%}\n<title>{%title%}</title>\n"
    "{%favicon%}\n<style>\n" + CSS + "\n</style>\n{%css%}\n</head>\n"
    "<body>\n{%app_entry%}\n{%config%}\n{%scripts%}\n{%renderer%}\n</body>\n</html>"
)

bank = get_bank()
n_assoc = sum(1 for q in bank["questions"] if q["cert_level"] == "associate")
n_prof = sum(1 for q in bank["questions"] if q["cert_level"] == "professional")
n_genai = sum(1 for q in bank["questions"] if q["cert_level"] == "genai")
BANK_INFO = (
    f"Question bank: {n_assoc} Associate · {n_prof} Professional · {n_genai} GenAI Engineer questions "
    f"({len(bank['flashcards'])} flashcards) — loaded from Unity Catalog"
    if not bank["error"]
    else f"⚠ Could not load the question bank from Unity Catalog: {bank['error']}"
)

app.layout = html.Div(
    [
        html.Div(
            [
                html.H1("Databricks Cert Prep"),
                html.P(
                    "Train for the Databricks Data Engineer Associate & "
                    "Professional certifications, plus Generative AI Engineer — "
                    "learn, practice under real "
                    "exam conditions, and review every answer."
                ),
            ],
            className="hdr",
        ),
        html.Div(
            [
                dcc.Tabs(
                    id="tabs",
                    value="learn",
                    className="tabbar",
                    children=[
                        dcc.Tab(
                            label="📚 Learn",
                            value="learn",
                            className="tab-item",
                            selected_className="tab-item--selected",
                        ),
                        dcc.Tab(
                            label="⏱️ Practice Exams",
                            value="exam",
                            className="tab-item",
                            selected_className="tab-item--selected",
                        ),
                        dcc.Tab(
                            label="💡 Tips & Tricks",
                            value="tips",
                            className="tab-item",
                            selected_className="tab-item--selected",
                        ),
                    ],
                ),
                # ------------------------------------------------ LEARN
                html.Div(
                    [
                        dcc.Store(id="learn-state", data={"idx": 0}),
                        html.Div(
                            [
                                html.Div(
                                    [
                                        html.Label("Certification"),
                                        dcc.Dropdown(
                                            id="learn-level",
                                            options=LEVELS,
                                            value="associate",
                                            clearable=False,
                                        ),
                                    ],
                                    className="field",
                                ),
                                html.Div(
                                    [
                                        html.Label("Domain"),
                                        dcc.Dropdown(
                                            id="learn-domain",
                                            options=["All"],
                                            value="All",
                                            clearable=False,
                                        ),
                                    ],
                                    className="field",
                                ),
                                html.Button(
                                    "↻ Reload question bank", id="learn-refresh"
                                ),
                            ],
                            className="controls",
                        ),
                        html.P(
                            [
                                html.B("Getting ready: "),
                                html.A(
                                    "Data Engineer Associate page",
                                    href="https://www.databricks.com/learn/certification/data-engineer-associate",
                                    target="_blank",
                                ),
                                " · ",
                                html.A(
                                    "Data Engineer Professional page",
                                    href="https://www.databricks.com/learn/certification/data-engineer-professional",
                                    target="_blank",
                                ),
                                " · ",
                                html.A(
                                    "Generative AI Engineer page",
                                    href="https://www.databricks.com/learn/certification/generative-ai-engineer-associate",
                                    target="_blank",
                                ),
                                " · ",
                                html.A(
                                    "AI Prep Guide",
                                    href="https://www.databricks.com/sites/default/files/2026-06/ai-prep-guide-any-databricks-certification.pdf",
                                    target="_blank",
                                ),
                                " — domain lists mirror the official “The exam covers” "
                                "sections, with real weightings.",
                            ],
                            className="muted",
                        ),
                        html.Div(id="learn-concept"),
                        html.Div(
                            [
                                html.Button("‹ Prev", id="learn-prev"),
                                html.Button(
                                    "Next concept ›",
                                    id="learn-next",
                                    className="btn-primary",
                                ),
                                html.Span(
                                    "",
                                    id="learn-progress",
                                    className="muted",
                                    style={"marginLeft": "16px"},
                                ),
                            ],
                            style={
                                "display": "flex",
                                "gap": "10px",
                                "alignItems": "center",
                                "marginTop": "18px",
                            },
                        ),
                        html.Details(
                            [
                                html.Summary(
                                    "📘 Full AI Prep Guide — all steps and sections"
                                ),
                                html.Div(id="learn-guide", style={"marginTop": "12px"}),
                            ],
                            style={"marginTop": "24px"},
                        ),
                    ],
                    id="tab-learn",
                    className="panel",
                ),
                # ------------------------------------------- PRACTICE EXAMS
                html.Div(
                    [
                        dcc.Store(id="exam-state"),
                        dcc.Store(id="exam-result"),
                        dcc.Store(id="exam-mode", data="setup"),
                        dcc.Interval(
                            id="exam-tick", interval=1000, disabled=True
                        ),
                        # setup view
                        html.Div(
                            id="view-setup",
                            children=[
                                html.H3("Set up your mock exam"),
                                html.P(
                                    "The real exams give you roughly two minutes "
                                    "per question (e.g. 45 questions / 90 minutes "
                                    "at Associate level) and require 70% to pass — "
                                    "check the official exam page for current details. "
                                    "Defaults here mirror that pacing."
                                ),
                                html.Div(
                                    [
                                        html.Div(
                                            [
                                                html.Label("Certification"),
                                                dcc.Dropdown(
                                                    id="exam-level",
                                                    options=LEVELS,
                                                    value="associate",
                                                    clearable=False,
                                                ),
                                            ],
                                            className="field",
                                        ),
                                        html.Div(
                                            [
                                                html.Label("Domain"),
                                                dcc.Dropdown(
                                                    id="exam-domain",
                                                    options=["All domains"],
                                                    value="All domains",
                                                    clearable=False,
                                                ),
                                            ],
                                            className="field",
                                        ),
                                        html.Div(
                                            [
                                                html.Label("Difficulty"),
                                                dcc.Dropdown(
                                                    id="exam-difficulty",
                                                    options=[
                                                        "All",
                                                        "easy",
                                                        "medium",
                                                        "complex",
                                                    ],
                                                    value="All",
                                                    clearable=False,
                                                ),
                                            ],
                                            className="field",
                                        ),
                                        html.Div(
                                            [
                                                html.Label("Set"),
                                                dcc.Dropdown(
                                                    id="exam-set",
                                                    options=["All sets"],
                                                    value="All sets",
                                                    clearable=False,
                                                ),
                                            ],
                                            className="field",
                                        ),
                                        html.Div(
                                            [
                                                html.Label("Questions"),
                                                dcc.Input(
                                                    id="exam-num",
                                                    type="number",
                                                    min=5,
                                                    step=1,
                                                    value=45,
                                                    style={"padding": "8px", "borderRadius": "8px", "border": "1px solid #d5dbe4"},
                                                ),
                                            ],
                                            className="field",
                                        ),
                                        html.Div(
                                            [
                                                html.Label("Minutes (auto: 2 per question)"),
                                                dcc.Input(
                                                    id="exam-mins",
                                                    type="number",
                                                    min=5,
                                                    step=5,
                                                    value=90,
                                                    style={"padding": "8px", "borderRadius": "8px", "border": "1px solid #d5dbe4"},
                                                ),
                                            ],
                                            className="field",
                                        ),
                                        html.Button(
                                            "▶ Start exam",
                                            id="exam-start",
                                            className="btn-accent",
                                        ),
                                    ],
                                    className="controls",
                                ),
                                html.P("", id="exam-hint", className="muted"),
                                html.P(BANK_INFO, className="muted"),
                                html.Hr(),
                                html.Details(
                                    [
                                        html.Summary(
                                            "📥 Upload questions from a PDF or Word document"
                                        ),
                                        html.P(
                                            "Parse a practice-exam document with AI and add its "
                                            "questions to the bank. Pick the level, domain, and "
                                            "difficulty to label the imported questions, then upload "
                                            "the file. Parsed questions are tagged with the file name "
                                            "as their set, so you can filter exams to just that set.",
                                            className="muted",
                                        ),
                                        html.Div(
                                            [
                                                html.Div(
                                                    [
                                                        html.Label("Certification"),
                                                        dcc.Dropdown(
                                                            id="upload-level",
                                                            options=LEVELS,
                                                            value="associate",
                                                            clearable=False,
                                                        ),
                                                    ],
                                                    className="field",
                                                ),
                                                html.Div(
                                                    [
                                                        html.Label("Domain"),
                                                        dcc.Dropdown(
                                                            id="upload-domain",
                                                            options=SECTIONS.get("associate", []),
                                                            value=(
                                                                SECTIONS.get("associate", [""])[0]
                                                            ),
                                                            clearable=False,
                                                        ),
                                                    ],
                                                    className="field",
                                                ),
                                                html.Div(
                                                    [
                                                        html.Label("Difficulty"),
                                                        dcc.Dropdown(
                                                            id="upload-difficulty",
                                                            options=["easy", "medium", "complex"],
                                                            value="medium",
                                                            clearable=False,
                                                        ),
                                                    ],
                                                    className="field",
                                                ),
                                            ],
                                            className="controls",
                                        ),
                                        dcc.Upload(
                                            id="upload-questions-file",
                                            children=html.Div(
                                                [
                                                    "Drag & drop or ",
                                                    html.A("select a PDF or Word file"),
                                                    " — parsed questions are added to the bank "
                                                    "(parsing can take a minute or two)",
                                                ]
                                            ),
                                            accept=".pdf,.docx,.doc",
                                            multiple=False,
                                            className="upload-zone",
                                            style={"marginTop": "12px"},
                                        ),
                                        html.Div(id="upload-status", className="upload-status"),
                                    ],
                                    style={"marginTop": "18px"},
                                ),
                            ],
                        ),
                        # running view
                        html.Div(
                            id="view-running",
                            style={"display": "none"},
                            children=[
                                html.Div(
                                    [
                                        html.Div(
                                            [html.Span("⏱ ", style={"fontSize": "20px"}), html.Span("--:--", id="exam-timer", className="timer")]
                                        ),
                                        html.Span(id="exam-progress", className="muted"),
                                    ],
                                    className="exam-top",
                                ),
                                html.Div(id="exam-qnum"),
                                html.Div(id="exam-question", className="q-text"),
                                dcc.RadioItems(
                                    id="exam-options",
                                    options=[],
                                    labelStyle={
                                        "display": "block",
                                        "padding": "8px 10px",
                                        "borderRadius": "8px",
                                        "cursor": "pointer",
                                    },
                                ),
                                html.Div(
                                    [
                                        html.Button("‹ Prev", id="exam-prev"),
                                        html.Button("Next ›", id="exam-next"),
                                        html.Button(
                                            "⏸ Pause",
                                            id="exam-pause",
                                            title="Pause the exam and the timer (click again to resume)",
                                        ),
                                        html.Button(
                                            "Submit exam",
                                            id="exam-submit",
                                            className="btn-accent",
                                        ),
                                    ],
                                    style={"display": "flex", "gap": "10px", "marginTop": "18px"},
                                ),
                                html.Div(id="exam-nav", className="q-grid"),
                            ],
                        ),
                        # results view
                        html.Div(
                            id="view-results",
                            style={"display": "none"},
                            children=[
                                html.Div(id="exam-scorecard"),
                                html.Div(id="exam-domains"),
                                html.H3("Answer review"),
                                html.Div(id="exam-review"),
                                html.Div(
                                    [
                                        html.Button(
                                            "↺ Retake / new exam",
                                            id="exam-retake",
                                            className="btn-primary",
                                        )
                                    ],
                                    style={"marginTop": "18px"},
                                ),
                            ],
                        ),
                    ],
                    id="tab-exam",
                    className="panel",
                ),
                # ------------------------------------------------ TIPS
                html.Div(
                    [
                        html.H2("Exam strategy & common pitfalls", className="tips"),
                        dcc.Markdown(
                            """
#### Pacing and triage
* The pass bar is **70%** — you don't need a perfect score, you need broad coverage. Never leave a question blank; there is no negative marking.
* Do **three passes**: first answer everything you know fast (~1 min/question), flag the slow ones, then return. Reserve the final minutes to review flagged questions.
* Questions are scenario-based. Read the *last sentence* first (the actual ask), then the scenario. Many answers describe real techniques that are still *not what was asked*.

#### Answer-selection heuristics
* Prefer the **Databricks-recommended best practice**: Unity Catalog governance, managed tables, Delta Lake, built-in functions over UDFs, declarative pipelines over hand-coded orchestration, SQL/declarative over imperative loops.
* Watch for absolutes ("always", "never", "only") — they are usually wrong.
* If two options look similar, look for the keyword difference (e.g. *compaction* vs *retention*, *bronze* vs *silver*). The exam loves adjacent concepts.

#### Domain pitfalls to memorize
* **Delta Lake**: `OPTIMIZE` compacts small files; `VACUUM` removes files past retention. Time travel (`VERSION AS OF`) reads history; `RESTORE TABLE` reverts. Deletion vectors speed DML, not reads.
* **Streaming**: watermarks bound late data and evict state; `availableNow=True` = process everything then stop; checkpoint location = resumability + effectively-once semantics into Delta.
* **Pipelines (SDP)**: `expect` keeps bad records and reports metrics, `expect_or_drop` removes them; streaming tables are incremental, materialized views are fully recomputed.
* **Unity Catalog**: grants chain USE CATALOG → USE SCHEMA → SELECT; managed tables delete data on DROP, external tables keep it; lineage is automatic.
* **Performance**: AQE re-optimizes at runtime; broadcast small dimension tables; skew → AQE skew handling / salting; cache only what you reuse.
* **PySpark**: lazy evaluation; narrow vs wide transformations; built-ins beat Python UDFs.
"""
                        ),
                        html.H3("How to study", className="tips"),
                        dcc.Markdown(
                            """
1. **Get hands-on.** These exams test applied scenarios, not definitions. Reproduce every concept in a notebook — MERGE, time travel, watermarks, expectations, grants.
2. **Use the Learn tab daily** (spaced repetition beats cramming) and take a **timed mock exam every few days**. Review every wrong answer's explanation — that's where the learning happens.
3. **Chase your weakest domain.** The results screen ranks domains by accuracy; spend your next study session on the bottom of that list.
4. **Skim the official docs for each domain** — Databricks exams are closely aligned with current documented best practice.
5. **Use the official AI Prep Guide** to build your personalized prep plan (linked in the Learn tab).

**Official resources**
- [AI Prep Guide (any Databricks certification)](https://www.databricks.com/sites/default/files/2026-06/ai-prep-guide-any-databricks-certification.pdf)
- [Data Engineer Associate certification page](https://www.databricks.com/learn/certification/data-engineer-associate)
- [Data Engineer Professional certification page](https://www.databricks.com/learn/certification/data-engineer-professional)
- [Generative AI Engineer Associate certification page](https://www.databricks.com/learn/certification/generative-ai-engineer-associate)
- [Databricks documentation](https://docs.databricks.com)
"""
                        ),
                    ],
                    id="tab-tips",
                    className="panel tips",
                ),
            ],
            className="container",
        ),
    ]
)


# --------------------------------------------------------------- helpers
def fmt_secs(sec):
    sec = max(0, int(sec))
    return f"{sec // 60:02d}:{sec % 60:02d}"


def compute_results(state):
    by_id = {q["id"]: q for q in get_bank()["questions"]}
    details, domains = [], {}
    correct = 0
    for qid in state["qids"]:
        q = by_id[qid]
        ans = state["answers"].get(str(qid))
        ok = ans is not None and ans == q["correct_index"]
        correct += int(ok)
        agg = domains.setdefault(q["domain"], [0, 0])
        agg[0] += int(ok)
        agg[1] += 1
        details.append(
            {
                "domain": q["domain"],
                "question": q["question"],
                "options": q["options"],
                "your": ans,
                "correct": q["correct_index"],
                "ok": ok,
                "explanation": q["explanation"],
            }
        )
    total = len(state["qids"])
    score = round(100.0 * correct / total, 1) if total else 0.0
    return {
        "level": state["level"],
        "score": score,
        "correct": correct,
        "total": total,
        "passed": score >= PASS_PCT,
        "domains": domains,
        "details": details,
        "started": state.get("started"),
        "duration_sec": int(time.time() - state.get("started_epoch", time.time())),
    }


def save_attempt(state, result):
    """Persist the attempt to workspace.cert_prep.attempts (best effort)."""
    try:
        from pyspark.sql.types import (
            BooleanType,
            DoubleType,
            IntegerType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        started = datetime.fromisoformat(state["started"])
        submitted = datetime.now(timezone.utc)
        schema = StructType(
            [
                StructField("attempt_id", StringType()),
                StructField("user_email", StringType()),
                StructField("cert_level", StringType()),
                StructField("started_ts", TimestampType()),
                StructField("submitted_ts", TimestampType()),
                StructField("duration_sec", IntegerType()),
                StructField("total_questions", IntegerType()),
                StructField("correct_count", IntegerType()),
                StructField("score_pct", DoubleType()),
                StructField("passed", BooleanType()),
                StructField("details", StringType()),
            ]
        )
        row = (
            str(uuid.uuid4()),
            current_user_email(),
            state["level"],
            started,
            submitted,
            result["duration_sec"],
            result["total"],
            result["correct"],
            result["score"],
            result["passed"],
            json.dumps(result["details"]),
        )
        get_spark().createDataFrame([row], schema).write.mode("append").saveAsTable(
            ATTEMPTS_TABLE
        )
        log.info("Saved exam attempt to %s", ATTEMPTS_TABLE)
    except Exception:
        log.exception("Failed to save exam attempt")


# ------------------------------------------------------- tab switching
@app.callback(
    Output("tab-learn", "style"),
    Output("tab-exam", "style"),
    Output("tab-tips", "style"),
    Input("tabs", "value"),
)
def switch_tab(tab):
    show, hide = {"display": "block"}, {"display": "none"}
    return (
        show if tab == "learn" else hide,
        show if tab == "exam" else hide,
        show if tab == "tips" else hide,
    )


# ------------------------------------------------------------------ LEARN
@app.callback(
    Output("learn-domain", "options"),
    Output("learn-domain", "value"),
    Input("learn-level", "value"),
    Input("learn-refresh", "n_clicks"),
    State("learn-domain", "value"),
)
def learn_domain_options(level, _refresh, current):
    if ctx.triggered_id == "learn-refresh":
        get_bank(force=True)  # pick up newly added exam questions
    doms = SECTIONS.get(level or "associate", [])
    opts = ["All"] + doms
    return opts, current if current in opts else opts[0]


@app.callback(
    Output("learn-state", "data"),
    Input("learn-level", "value"),
    Input("learn-domain", "value"),
    Input("learn-prev", "n_clicks"),
    Input("learn-next", "n_clicks"),
    State("learn-state", "data"),
)
def learn_mutate(level, domain, _prev, _next, state):
    tid = ctx.triggered_id
    if tid in ("learn-level", "learn-domain"):
        return {"idx": 0}
    state = state or {"idx": 0}
    total = len(get_concepts(level, domain))
    idx = int(state.get("idx", 0))
    if tid == "learn-prev":
        return {"idx": max(0, idx - 1)}
    if tid == "learn-next":
        return {"idx": min(idx + 1, total - 1) if total else 0}
    return no_update


@app.callback(
    Output("learn-concept", "children"),
    Output("learn-progress", "children"),
    Input("learn-state", "data"),
    State("learn-level", "value"),
    State("learn-domain", "value"),
)
def render_concept(state, level, domain):
    concepts = get_concepts(level, domain)
    if not concepts:
        return "No concepts yet for this selection.", "0 concepts"
    idx = min(int((state or {}).get("idx", 0)), len(concepts) - 1)
    c = concepts[idx]
    card = html.Div(
        [
            html.Span(c["section"], className="chip"),
            html.H3(c["title"], style={"marginTop": "10px"}),
            dcc.Markdown(c["body"], className="markdown"),
        ],
        className="lesson-card",
    )
    pct = round(100 * (idx + 1) / len(concepts))
    progress = html.Div(
        [
            html.Div(
                [html.Div("", style={"width": f"{pct}%"}, className="bar-fill")],
                className="bar",
            ),
            html.Span(
                f"Concept {idx + 1} of {len(concepts)} · {c['section']}",
                className="muted",
            ),
        ],
        className="progress-wrap",
    )
    return card, progress


@app.callback(
    Output("learn-guide", "children"),
    Input("learn-level", "value"),
)
def render_guide(level):
    return html.Div(
        dcc.Markdown(build_prep_guide(level or "associate")),
        style={"lineHeight": "1.65"},
    )


# ------------------------------------------------------------------ EXAM
@app.callback(
    Output("exam-num", "value"),
    Input("exam-level", "value"),
)
def exam_defaults(level):
    return EXAM_DEFAULTS.get(level, 45)


# Real-exam pacing: 2 minutes per question (45 questions -> 90 minutes).
# The minutes field follows the question count automatically.
@app.callback(
    Output("exam-mins", "value"),
    Input("exam-num", "value"),
)
def exam_minutes(num):
    try:
        num = int(num)
    except (TypeError, ValueError):
        num = 45
    return max(1, num) * 2


# Sets shown in the Practice-exam "Set" dropdown. Every other set stays in
# the bank (still reachable via "All sets" and kept in the UC table), but is
# hidden from the dropdown to keep it clean.
FEATURED_SETS = {
    "Expected Exam Bank — Complex Scenarios (45)",
    "Expected Exam Bank — Complex Scenarios 2 (45)",
    "Expected Exam Bank — Complex Scenarios 3 (45)",
    "Official Practice Exam (Databricks Academy PDF)",
    "DataBricks-Practice-exam(1).pdf",
    "Generative AI Engineer — Expected Exam Bank (60)",
}


@app.callback(
    Output("exam-domain", "options"),
    Output("exam-domain", "value"),
    Output("exam-set", "options"),
    Output("exam-set", "value"),
    Output("exam-hint", "children"),
    Input("exam-level", "value"),
    Input("exam-domain", "value"),
    Input("exam-difficulty", "value"),
    Input("exam-set", "value"),
)
def exam_filter_options(level, domain, difficulty, qset):
    if ctx.triggered_id == "exam-level":
        domain = "All domains"
    doms = ["All domains"] + SECTIONS.get(level or "associate", [])
    domain = domain if domain in doms else "All domains"
    sets = ["All sets"] + sorted(
        s
        for s in {q["source"] for q in get_bank()["questions"] if q["source"]}
        if s in FEATURED_SETS
    )
    qset = qset if qset in sets else "All sets"
    pool = [q for q in get_bank()["questions"] if q["cert_level"] == level]
    if qset != "All sets":
        pool = [q for q in pool if q["source"] == qset]
    if domain != "All domains":
        pool = [q for q in pool if q["domain"] == domain]
    if difficulty not in (None, "", "All"):
        pool = [q for q in pool if q["difficulty"] == difficulty]
    hint = f"{len(pool)} questions match this selection"
    if len(pool) < 5:
        hint += " — widen the filters for a realistic mock"
    return doms, domain, sets, qset, hint


# --------------------------------------------- question upload (AI parse)
_UPLOAD_EXTRACT_SCHEMA = (
    '{"questions": {"type": "array", "description": "Every multiple-choice question found", '
    '"items": {"type": "object", "properties": {'
    '"question": {"type": "string"}, '
    '"option_a": {"type": "string"}, "option_b": {"type": "string"}, '
    '"option_c": {"type": "string"}, "option_d": {"type": "string"}, '
    '"option_e": {"type": "string"}, '
    '"answer": {"type": "string", "description": "Letter of the correct option (A-E)"}, '
    '"explanation": {"type": "string"}}}}}'
)
_UPLOAD_EXTRACT_INSTR = (
    "This is a certification practice exam document, usually with an answer key at "
    "the end. Extract every multiple-choice question. If an answer key is present, "
    "use it to fill the answer letter for each question. Put option text in "
    "option_a..option_e with letter prefixes stripped."
)


def _parse_document_questions(content_bytes):
    """Parse an uploaded PDF/Word document and extract MCQs with AI functions."""
    spark = get_spark()
    df = spark.createDataFrame([(content_bytes,)], "content binary")
    row = df.selectExpr(
        "ai_extract("
        "concat_ws('\n\n', transform("
        "try_cast(ai_parse_document(content, MAP('version','2.0')):document:elements AS ARRAY<VARIANT>),"
        "element -> try_cast(element:content AS STRING))),"
        f"'{_UPLOAD_EXTRACT_SCHEMA}',"
        f"MAP('version','2.1','instructions','{_UPLOAD_EXTRACT_INSTR}')) AS data"
    ).collect()[0]["data"]
    raw = json.loads(row.toJson()) if hasattr(row, "toJson") else json.loads(str(row))
    err = raw.get("error_message")
    if err:
        raise RuntimeError(f"AI extraction error: {err}")
    return raw.get("response", {}).get("questions", []) or []


@app.callback(
    Output("upload-domain", "options"),
    Output("upload-domain", "value"),
    Input("upload-level", "value"),
)
def upload_domain_options(level):
    doms = SECTIONS.get(level or "associate", [])
    return doms, (doms[0] if doms else "")


@app.callback(
    Output("upload-status", "children"),
    Input("upload-questions-file", "contents"),
    State("upload-questions-file", "filename"),
    State("upload-level", "value"),
    State("upload-domain", "value"),
    State("upload-difficulty", "value"),
    prevent_initial_call=True,
)
def process_question_upload(contents, filename, level, domain, difficulty):
    if not contents:
        return no_update
    try:
        import base64

        from pyspark.sql import Row as _Row
        from pyspark.sql.types import (
            IntegerType,
            StringType,
            StructField,
            StructType,
        )

        _, b64 = contents.split(",", 1)
        raw_bytes = base64.b64decode(b64)
        parsed = _parse_document_questions(raw_bytes)

        def _v(d, k):
            v = d.get(k)
            return v.get("value") if isinstance(v, dict) else v

        spark = get_spark()
        max_id = int(
            spark.sql(
                f"SELECT coalesce(max(id), 0) m FROM {QUESTIONS_TABLE}"
            ).first()["m"]
        )
        rows, skipped = [], 0
        for q in parsed:
            opts = [
                str(_v(q, f"option_{c}")).strip()
                for c in "abcde"
                if _v(q, f"option_{c}")
            ]
            ans = (_v(q, "answer") or "").strip().upper()
            text = (_v(q, "question") or "").strip()
            if not text or len(opts) < 2 or not ans or "ABCDE".find(ans) >= len(opts):
                skipped += 1
                continue
            expl = (_v(q, "explanation") or "").strip() or (
                "No explanation provided in the source document."
            )
            rows.append(
                (
                    max_id + len(rows) + 1,
                    level,
                    domain,
                    difficulty,
                    text,
                    json.dumps(opts),
                    "ABCDE".find(ans),
                    expl,
                    filename,
                )
            )
        if rows:
            schema = StructType(
                [
                    StructField("id", IntegerType()),
                    StructField("cert_level", StringType()),
                    StructField("domain", StringType()),
                    StructField("difficulty", StringType()),
                    StructField("question", StringType()),
                    StructField("options", StringType()),
                    StructField("correct_index", IntegerType()),
                    StructField("explanation", StringType()),
                    StructField("source", StringType()),
                ]
            )
            spark.createDataFrame([_Row(*r) for r in rows], schema).write.mode(
                "append"
            ).saveAsTable(QUESTIONS_TABLE)
            get_bank(force=True)
        if not rows:
            return (
                f"⚠ No valid multiple-choice questions were found in {filename} "
                f"({skipped} candidates skipped). Check that the document contains "
                "numbered MCQs with an answer key."
            )
        msg = f"✅ Added {len(rows)} questions from {filename}"
        if skipped:
            msg += f" ({skipped} candidates skipped — missing options or answer)"
        return msg
    except Exception as e:
        log.exception("Question upload failed")
        return f"⚠ Upload failed: {e}"


@app.callback(
    Output("view-setup", "style"),
    Output("view-running", "style"),
    Output("view-results", "style"),
    Input("exam-mode", "data"),
)
def toggle_exam_views(mode):
    mode = mode or "setup"
    show, hide = {"display": "block"}, {"display": "none"}
    return (
        show if mode == "setup" else hide,
        show if mode == "running" else hide,
        show if mode == "results" else hide,
    )


@app.callback(
    Output("exam-state", "data"),
    Output("exam-result", "data"),
    Output("exam-mode", "data"),
    Output("exam-tick", "disabled"),
    Input("exam-start", "n_clicks"),
    Input("exam-options", "value"),
    Input("exam-prev", "n_clicks"),
    Input("exam-next", "n_clicks"),
    Input("exam-pause", "n_clicks"),
    Input({"type": "exam-nav", "index": ALL}, "n_clicks"),
    Input("exam-retake", "n_clicks"),
    Input("exam-submit", "n_clicks"),
    State("exam-level", "value"),
    State("exam-num", "value"),
    State("exam-mins", "value"),
    State("exam-domain", "value"),
    State("exam-difficulty", "value"),
    State("exam-set", "value"),
    State("exam-state", "data"),
    State("exam-result", "data"),
)
def exam_mutate(
    n_start, opt_value, _prev, _next, _pause, _nav, _retake, _submit,
    level, num, mins, domain, difficulty, qset, state, result,
):
    tid = ctx.triggered_id
    tid_dict = tid if isinstance(tid, dict) else {}

    # -- lifecycle transitions ------------------------------------------
    if tid == "exam-retake":
        return None, None, "setup", True

    if tid == "exam-start":
        pool = [
            q for q in get_bank()["questions"] if q["cert_level"] == level
        ]
        if domain and domain != "All domains":
            pool = [q for q in pool if q["domain"] == domain]
        if qset not in (None, "", "All sets"):
            pool = [q for q in pool if q["source"] == qset]
        if difficulty not in (None, "", "All"):
            pool = [q for q in pool if q["difficulty"] == difficulty]
        if not pool:
            return no_update
        num = max(1, min(int(num or 10), len(pool)))
        picked = random.sample(pool, num)
        return (
            {
                "level": level,
                "qids": [q["id"] for q in picked],
                "answers": {},
                "idx": 0,
                "deadline": time.time() + int(mins or 60) * 60,
                "started": datetime.now(timezone.utc).isoformat(),
                "started_epoch": time.time(),
            },
            None,
            "running",
            False,
        )

    # -- in-flight updates ----------------------------------------------
    if not state or result is not None or state.get("submitted"):
        return no_update

    # while paused only resume (exam-pause) is accepted — no answering,
    # no navigating, no submitting, no auto-submit
    if state.get("paused") and tid != "exam-pause":
        return no_update

    if tid == "exam-options":
        if opt_value is None:
            return no_update
        s = dict(state)
        s["answers"] = dict(state["answers"])
        s["answers"][str(state["qids"][state["idx"]])] = int(opt_value)
        return s, no_update, no_update, no_update

    if tid == "exam-prev":
        s = dict(state)
        s["idx"] = max(0, state["idx"] - 1)
        return s, no_update, no_update, no_update

    if tid == "exam-next":
        s = dict(state)
        s["idx"] = min(len(state["qids"]) - 1, state["idx"] + 1)
        return s, no_update, no_update, no_update

    if tid == "exam-pause":  # pause / resume the exam and its timer
        s = dict(state)
        now = time.time()
        if state.get("paused"):
            s["deadline"] = now + state["remaining"]
            s["paused"] = False
        else:
            s["paused"] = True
            s["remaining"] = state["deadline"] - now
        return s, no_update, no_update, no_update

    if tid_dict.get("type") == "exam-nav":
        s = dict(state)
        s["idx"] = int(tid_dict["index"])
        return s, no_update, no_update, no_update

    if tid == "exam-submit":
        res = compute_results(state)
        save_attempt(state, res)
        s = dict(state)
        s["submitted"] = True
        return s, res, "results", True

    return no_update


@app.callback(
    Output("exam-pause", "children"),
    Input("exam-state", "data"),
)
def pause_label(state):
    if state and not state.get("submitted"):
        return "▶ Resume" if state.get("paused") else "⏸ Pause"
    return "⏸ Pause"


@app.callback(
    Output("exam-timer", "children"),
    Output("exam-timer", "className"),
    Output("exam-state", "data", allow_duplicate=True),
    Output("exam-result", "data", allow_duplicate=True),
    Output("exam-mode", "data", allow_duplicate=True),
    Output("exam-tick", "disabled", allow_duplicate=True),
    Input("exam-tick", "n_intervals"),
    State("exam-state", "data"),
    State("exam-result", "data"),
    prevent_initial_call=True,
)
def tick_timer(_n, state, result):
    """Timer display + auto-submit at zero. Kept OUT of exam_mutate on
    purpose: a 1s interval as an extra input there made Dash consolidate
    user clicks with ticks, silently dropping the clicks."""
    if not state or result is not None or state.get("submitted"):
        return no_update, no_update, no_update, no_update, no_update, no_update
    if state.get("paused"):
        return (
            f"⏸ {fmt_secs(state['remaining'])}",
            "timer paused",
            no_update, no_update, no_update, no_update,
        )
    remaining = state["deadline"] - time.time()
    if remaining <= 0:  # time is up: auto-submit the exam
        res = compute_results(state)
        save_attempt(state, res)
        s = dict(state)
        s["submitted"] = True
        return "00:00", "timer low", s, res, "results", True
    cls = "timer" + (" low" if remaining < 60 else "")
    return fmt_secs(remaining), cls, no_update, no_update, no_update, no_update


@app.callback(
    Output("exam-qnum", "children"),
    Output("exam-question", "children"),
    Output("exam-options", "options"),
    Output("exam-options", "value"),
    Output("exam-progress", "children"),
    Output("exam-nav", "children"),
    Output("exam-options", "disabled"),
    Output("exam-prev", "disabled"),
    Output("exam-next", "disabled"),
    Output("exam-submit", "disabled"),
    Input("exam-state", "data"),
)
def render_question(state):
    if not state:
        return no_update
    paused = bool(state.get("paused"))
    if paused:  # hide the question while the exam is paused
        qnum = html.Div([html.Span("⏸ Exam paused", className="q-count")])
        qtext = (
            "The question and its answers are hidden while the exam is "
            "paused. Click ▶ Resume to continue with your remaining time."
        )
        nav = [
            html.Button(
                str(i + 1),
                id={"type": "exam-nav", "index": i},
                className="nav-btn"
                + (" answered" if str(qid) in state["answers"] else "")
                + (" current" if i == state["idx"] else ""),
                disabled=True,
            )
            for i, qid in enumerate(state["qids"])
        ]
        return (
            qnum, qtext, [], None,
            "Exam paused — timer stopped", nav,
            True, True, True, True,
        )
    by_id = {q["id"]: q for q in get_bank()["questions"]}
    qids, idx = state["qids"], state["idx"]
    q = by_id[qids[idx]]
    qnum = html.Div(
        [
            html.Span(f"Question {idx + 1} of {len(qids)}", className="q-count"),
            html.Span(q["domain"], className="chip"),
            html.Span(
                f" · {q['difficulty']}",
                style={"color": "#667085", "fontSize": "13px", "marginLeft": "6px"},
            ),
        ]
    )
    opts = [{"label": o, "value": i} for i, o in enumerate(q["options"])]
    value = state["answers"].get(str(q["id"]))
    lvl_label = next((l['label'] for l in LEVELS if l['value'] == state['level']), '')
    progress = f"{len(state['answers'])}/{len(qids)} answered · {lvl_label}"
    nav = [
        html.Button(
            str(i + 1),
            id={"type": "exam-nav", "index": i},
            className="nav-btn"
            + (" answered" if str(qid) in state["answers"] else "")
            + (" current" if i == idx else ""),
            disabled=paused,
        )
        for i, qid in enumerate(qids)
    ]
    return qnum, q["question"], opts, value, progress, nav, False, False, False, False


@app.callback(
    Output("exam-scorecard", "children"),
    Output("exam-domains", "children"),
    Output("exam-review", "children"),
    Input("exam-result", "data"),
)
def render_results(result):
    if not result:
        return no_update
    level_label = next(
        l["label"] for l in LEVELS if l["value"] == result["level"]
    )
    scorecard = html.Div(
        [
            html.Div(
                [
                    html.Div(f"{result['score']:.0f}%", className="score"),
                    html.Span(
                        "PASSED ✅" if result["passed"] else "NOT PASSED ❌",
                        className=("badge pass" if result["passed"] else "badge fail"),
                    ),
                ],
                style={"display": "flex", "alignItems": "center", "gap": "18px"},
            ),
            html.P(
                f"{result['correct']} of {result['total']} correct · "
                f"{fmt_secs(result['duration_sec'])} used · passing bar is 70% · "
                f"{level_label}"
            ),
        ]
    )

    rows = sorted(result["domains"].items(), key=lambda kv: kv[1][0] / kv[1][1])
    table = html.Table(
        className="domains",
        children=[
            html.Tr([html.Th("Domain (weakest first)"), html.Th("Correct"), html.Th("Accuracy")]),
            *[
                html.Tr(
                    [
                        html.Td(d),
                        html.Td(f"{ok} / {tot}"),
                        html.Td(f"{100 * ok / tot:.0f}%"),
                    ]
                )
                for d, (ok, tot) in rows
            ],
        ],
    )
    weakest = rows[0][0] if rows else ""

    # Trainer's evaluation: weight each domain like the real exam does, and
    # give a readiness verdict like an instructor would after a mock.
    weighted = None
    num = den = 0.0
    for d, (ok, tot) in result["domains"].items():
        w = OFFICIAL_WEIGHTS.get(d, 0)
        if w and tot:
            num += w * (ok / tot)
            den += w
    if den:
        weighted = 100 * num / den

    if result["score"] >= 85:
        verdict = (
            "Exam-ready — you'd clear the bar with margin. Keep sharp with a "
            "short mock every few days."
        )
    elif result["score"] >= 70:
        verdict = (
            "Above the 70% passing bar, but not comfortable yet — polish your "
            "weakest domains below in the Learn tab, then retake."
        )
    else:
        verdict = (
            "Below the 70% passing bar. Work through your weakest domains "
            "below in the Learn tab (one at a time, concept by concept), "
            "then retake this exam."
        )

    eval_block = html.Div(
        [
            html.H3("Trainer's evaluation"),
            html.P(verdict),
            html.P(
                (
                    f"Exam-weighted score: {weighted:.0f}% (each domain weighted "
                    "by its real exam weight, so strengths in high-weight domains "
                    "count more) · raw score: {result['score']:.0f}% · passing "
                    "bar: 70%"
                )
                if weighted is not None
                else f"Raw score: {result['score']:.0f}% · passing bar: 70%",
                className="muted",
            ),
        ]
    )

    domains_block = html.Div(
        [
            eval_block,
            html.H3("Per-domain breakdown"),
            html.P(f"Focus your next study session on: {weakest}", className="muted"),
            table,
        ]
    )

    review = []
    for i, d in enumerate(result["details"]):
        your = (
            d["options"][d["your"]]
            if d["your"] is not None
            else "— no answer —"
        )
        review.append(
            html.Div(
                [
                    html.Div(
                        [
                            html.Strong(f"{i + 1}. "),
                            html.Span(d["question"]),
                            html.Span(d["domain"], className="chip"),
                        ]
                    ),
                    html.Div(
                        [
                            "Your answer: ",
                            html.Span(
                                your,
                                className=("ok" if d["ok"] else "ko"),
                            ),
                            " · Correct answer: ",
                            html.Strong(d["options"][d["correct"]]),
                        ],
                        style={"marginTop": "8px"},
                    ),
                    html.Div(d["explanation"], className="explain"),
                ],
                className="review-item",
            )
        )
    return scorecard, domains_block, review


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8080")), debug=False)

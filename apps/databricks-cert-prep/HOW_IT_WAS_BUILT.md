# Databricks Cert Prep — How It Was Built

**App**: `databricks-cert-prep` (Databricks Apps)
**URL**: https://databricks-cert-prep-7474649843398849.aws.databricksapps.com
**Built**: November 21, 2026 · Owner: hello.prabus@gmail.com

---

## 1. Purpose

A web app to prepare for the **Databricks Data Engineer Associate** and **Data Engineer Professional** certifications with three areas:

1. **Learn** — flashcards by certification and exam domain
2. **Practice Exams** — timed, exam-realistic mock exams with scoring and answer review
3. **Tips & Tricks** — exam strategy and common pitfalls

The question bank and every attempt result are stored in **Unity Catalog** (Delta), so the bank keeps growing and results can be analyzed over time.

---

## 2. Architecture at a glance

```
┌──────────────────────────── Databricks Apps (managed runtime) ──────────────────────────┐
│                                                                                      │
│  app.py  (Dash web UI: Learn / Practice Exams / Tips)                                 │
│    │                                                                                 │
│    │  Databricks Connect (serverless Spark session, app's service principal)          │
│    ▼                                                                                 │
└──│────────────────────────────────────────────────────────────────────────────────── ┘
   │
   ▼  Unity Catalog (Delta tables)
   ├── workspace.cert_prep.questions   ← 46 seeded questions (22 Associate / 24 Professional)
   ├── workspace.cert_prep.flashcards  ← 20 seeded flashcards (10 Associate / 10 Professional)
   └── workspace.cert_prep.attempts    ← one row per mock exam taken
```

* **Frontend**: Dash (Python), no JavaScript build step. Custom CSS injected via the page template.
* **Backend**: the app process talks to your workspace over **Databricks Connect (serverless)** using a Spark session — the same Spark APIs as a notebook, no warehouse needed.
* **Data**: plain Delta tables in a dedicated UC schema, so you can query, insert, and govern them like any other table.
* **Source files** (in `/Workspace/Users/hello.prabus@gmail.com/databricks-cert-prep/`):
  * `app.py` — the entire application (~920 lines: layout, callbacks, data layer)
  * `app.yaml` — app config: `command: [python, app.py]`
  * `requirements.txt` — `dash`, `databricks-sdk`, `databricks-connect`

---

## 3. The data layer (Unity Catalog)

### `workspace.cert_prep.questions`
| Column | Type | Notes |
|---|---|---|
| `id` | INT | unique question id |
| `cert_level` | STRING | `associate` / `professional` |
| `domain` | STRING | e.g. `Delta Lake`, `Streaming`, `Lakeflow SDP` |
| `difficulty` | STRING | `easy` / `medium` / `hard` |
| `question` | STRING | question text |
| `options` | STRING | **JSON array** of 4 answer options |
| `correct_index` | INT | 0-based index of the correct option |
| `explanation` | STRING | why the answer is correct |

### `workspace.cert_prep.flashcards`
`id INT, cert_level STRING, domain STRING, front STRING, back STRING`

### `workspace.cert_prep.attempts` (written by the app after every exam)
`attempt_id STRING, user_email STRING, cert_level STRING, started_ts TIMESTAMP, submitted_ts TIMESTAMP, duration_sec INT, total_questions INT, correct_count INT, score_pct DOUBLE, passed BOOLEAN, details STRING (JSON per-question results)`

**Seeded content**: 46 questions across 11 domains — Associate (Platform & Workspace, ETL with PySpark & SQL, Delta Lake, Medallion Architecture, Unity Catalog) and Professional (Delta Lake Advanced, Lakeflow SDP, Streaming, Unity Catalog Governance, Performance Optimization, CI/CD & Deployment) — plus 20 flashcards. Every question includes an explanation shown in the answer review.

---

## 4. How the app works internally

### Question bank loading
On startup (and on demand via the **↻ Reload bank** button) the app reads both content tables through Spark and caches them **in process memory**. That is why adding new questions needs no redeploy — a click on Reload re-reads the tables.

### Exam engine (Practice Exams tab)
* **Setup → Running → Results** are three views toggled by an `exam-mode` state store.
* On *Start exam*, questions are randomly sampled (`random.sample`) from the selected certification's pool — the number of questions and minutes are configurable, with defaults mirroring real pacing (~2 min/question).
* Exam state (question ids, answers, current index, deadline timestamp) lives in the browser (`dcc.Store`), so navigation never loses your place.
* A **1-second interval** drives the countdown; when the deadline passes, the exam **auto-submits**.
* On submit, results are computed in the app (score vs the 70% pass bar, per-domain accuracy), the full attempt is **appended to `attempts`** (best effort — an insert failure is logged but never blocks your results), and the review screen lists every question with your answer, the correct answer, and the explanation. The per-domain table is **sorted weakest-first** to tell you what to study next.

### User identity
The Databricks Apps frontend forwards the logged-in user in request headers (`X-Forwarded-Email`); the app records it on each attempt so results can be attributed per person if you share the app with your team.

---

## 5. Security model

The app runs under its **own service principal** (`app-56bu38 databricks-cert-prep`, application id `42641129-20a2-4052-89ae-7210c81d6439`) — never under your personal credentials. It holds exactly these Unity Catalog grants:

* `USE CATALOG` on `workspace`
* `USE SCHEMA` on `workspace.cert_prep`
* `SELECT` on `questions` and `flashcards` (read-only content)
* `SELECT, INSERT, MODIFY` on `attempts` (append results)

Anyone you share the app with sees the app through this principal — grant the app's principal more access only if you intend that for all app users.

---

## 6. Deployment & lifecycle (commands used)

```bash
# 1. inspect state before any lifecycle action
databricks apps get databricks-cert-prep --output JSON

# 2. deploy source (needs compute_status.state = ACTIVE)
databricks apps deploy databricks-cert-prep \
  --source-code-path /Workspace/Users/hello.prabus@gmail.com/databricks-cert-prep

# 3. logs
databricks apps logs databricks-cert-prep
```

Compute was `MEDIUM`; deployments are `SNAPSHOT` mode (a frozen copy of the source at deploy time — later source edits need a redeploy, but question-bank edits don't).

---

## 7. Lessons learned / gotchas (for future edits)

* **`html.Style` does not exist** in the installed Dash (2.18) — CSS must be injected via `app.index_string` (page template), which the app does.
* **Databricks Connect** here requires `DatabricksSession.builder.serverless(True).getOrCreate()`; plain `getOrCreate()` fails with “Cluster id or serverless are required”. `sdkConfig()` takes a **`Config` object**, not a `WorkspaceClient`.
* The Spark session and the question-bank cache are **per process** — after changing grants or tables, redeploy (or click Reload bank) to see effects.
* Exam state is browser-side; a page refresh mid-exam **drops the attempt** (it is not saved unless submitted).

---

## 8. How to extend it

* **Add questions**: `INSERT INTO workspace.cert_prep.questions VALUES (...)` then click **↻ Reload bank**. Follow the `options` JSON-array format and set `correct_index` (0-based).
* **Add a new certification** (e.g. ML Associate): add rows with a new `cert_level`, then add the level to the two `LEVELS` dropdowns and `EXAM_DEFAULTS` in `app.py` and redeploy.
* **Track progress**: query `workspace.cert_prep.attempts`, e.g.
  ```sql
  SELECT user_email, cert_level, submitted_ts, score_pct, passed, duration_sec
  FROM workspace.cert_prep.attempts ORDER BY submitted_ts DESC;
  ```
  Per-question history lives in the `details` JSON column (`from_json` can unpack it).
* **Analytics dashboard**: the `attempts` table is a perfect source for an AI/BI dashboard — score trend, pass rate by level, time-per-question.

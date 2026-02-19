# Engineering Decisions

This document captures every significant architectural and technical decision made during the development of the DQ Engineering Pipeline. Each entry explains the context, the options we considered, and why we chose what we chose.

---

## Table of Contents

1. [Tech Stack](#1-tech-stack)
2. [UUID Primary Keys for Files and Runs](#2-uuid-primary-keys-for-files-and-runs)
3. [Redis as Job Queue Between FastAPI and Prefect](#3-redis-as-job-queue-between-fastapi-and-prefect)
4. [MinIO File Access Pattern — Streaming vs Staging](#4-minio-file-access-pattern--streaming-vs-staging)
5. [BytesIO Buffering for MinIO Responses](#5-bytesio-buffering-for-minio-responses)
6. [asyncio.run() Inside Sync Prefect Tasks](#6-asynciorun-inside-sync-prefect-tasks)
7. [Profiling Library Choice — Custom Pandas](#7-profiling-library-choice--custom-pandas)
8. [DQ Score Formula and Weights](#8-dq-score-formula-and-weights)
9. [Outlier Detection — IQR, Advisory Only](#9-outlier-detection--idr-advisory-only)
10. [Code Organisation — clients.py + dq_logic.py + thin dq_flow.py](#10-code-organisation--clientspy--dq_logicpy--thin-dq_flowpy)
11. [Client Factory Functions, Not Singletons](#11-client-factory-functions-not-singletons)
12. [PYTHONPATH for Prefect Import Resolution](#12-pythonpath-for-prefect-import-resolution)
13. [Prefect Task Testing via .fn()](#13-prefect-task-testing-via-fn)
14. [df.copy() at the Start of profile_dataframe()](#14-dfcopy-at-the-start-of-profile_dataframe)
15. [Code vs LLM Boundary — Metadata vs Decisions](#15-code-vs-llm-boundary--metadata-vs-decisions)
16. [Recommendations JSON Schema](#16-recommendations-json-schema)
17. [Docker Multi-Process — trap + wait -n](#17-docker-multi-process--trap--wait--n)

---

## 1. Tech Stack

**Decision:** FastAPI + Prefect 3 + PostgreSQL + MinIO + Redis + Anthropic API

**Context:** We needed a pipeline that lets a user upload a CSV, automatically runs data quality analysis, lets the user review and edit recommendations, then applies transformations and produces a cleaned file.

**Why each component:**

| Component | Why |
|---|---|
| **FastAPI** | Async-native, excellent typing with Pydantic, auto-generated docs. Faster to develop than Django REST, lighter than Spring. |
| **Prefect 3** | Workflow orchestration with built-in retries, observability UI, and deployment management. Chosen over Airflow because Prefect flows are plain Python functions — no DAG boilerplate, easier to test. |
| **PostgreSQL** | Relational storage for users, files, and runs. JSONB support for the recommendations column means we can store and query the recommendations without a separate document store. |
| **MinIO** | S3-compatible object storage we can run locally in Docker. Raw zone stores uploaded CSVs. Curated zone stores cleaned output files. Presigned URLs let users download directly without proxying through the API. |
| **Redis** | Lightweight job queue. FastAPI pushes a job with LPUSH, the Redis worker picks it up with BRPOP and triggers the Prefect flow. Decouples the HTTP request from the long-running analysis. |
| **Anthropic API** | LLM for custom transforms (natural language → pandas code) and semantic enrichment of recommendations. Not yet implemented — reserved for Phase 3. |

---

## 2. UUID Primary Keys for Files and Runs

**Decision:** Files and Runs use UUID (`gen_random_uuid()`), not SERIAL integers.

**Context:** The API exposes file and run IDs in URLs (`/api/files/{file_id}`). If IDs are sequential integers, a user can enumerate other users' files by incrementing the ID.

**Why UUID:**
- Non-guessable — a user cannot enumerate other users' resources by trying `file_id=1, 2, 3`
- Per-user isolation is enforced at the query level (`WHERE file_id = $1 AND user_id = $2`), but UUID is a second layer of defence
- PostgreSQL's `gen_random_uuid()` is fast and built-in

**Why Users still use SERIAL:**
Users are authenticated via JWT — their ID is never exposed in a URL. No enumeration risk.

**Gotcha discovered:** UUID values must be converted to `str()` before `json.dumps()` for Redis serialisation. Python's `json` module does not serialise UUID objects natively.

---

## 3. Redis as Job Queue Between FastAPI and Prefect

**Decision:** FastAPI pushes jobs to Redis with LPUSH. A separate Redis worker process (BRPOP) picks them up and triggers Prefect deployments via `run_deployment()`.

**Context:** When a user uploads a file, we need to trigger a long-running Prefect flow without blocking the HTTP response. The upload endpoint must return immediately with a `run_id` the client can poll.

**Options considered:**

| Option | Problem |
|---|---|
| Call Prefect API directly from FastAPI | Creates a hard coupling between the API and the Prefect server. If Prefect is slow or down, the upload endpoint hangs. |
| Celery | Heavy dependency, needs a broker (usually RabbitMQ or Redis anyway), overkill for a single queue. |
| Redis LPUSH/BRPOP | Simple, lightweight, already needed for caching. BRPOP blocks until a job is available — zero CPU waste when idle. |

**Message format:**
```json
{ "run_id": "uuid-str", "file_id": "uuid-str", "minio_path": "uuid/filename.csv" }
```

**Bug discovered during development:** The Redis worker was originally written with the sync `redis.Redis` client inside an `async` function, then called with `await`. This caused `TypeError: object bool can't be used in 'await' expression`. Fixed by switching to `redis.asyncio.Redis`.

---

## 4. MinIO File Access Pattern — Streaming vs Staging

**Decision:** Stream the MinIO response directly into `pd.read_csv()`. No staging volumes. No FUSE mounting.

**Context:** The Prefect flow needs to read a CSV file from MinIO to profile it. We considered three approaches:

**Option A — Staging volume (shared Docker volume):**
Mount the same volume to the FastAPI container and the Prefect worker. FastAPI writes the file to the volume; Prefect reads it from disk.
- **Rejected:** Double storage (once in MinIO, once on disk). Defeats the purpose of having MinIO as the source of truth. Creates a sync problem — what if the volume mount path changes or the file is deleted?

**Option B — FUSE / S3-FUSE mounting:**
Mount MinIO as a filesystem inside the Prefect worker container using `s3fs-fuse`. Files appear as local paths.
- **Rejected:** Requires `--privileged` Docker flag, which is a security risk in production. Complex to configure. Not supported on all container runtimes (EKS, GKE managed nodes).

**Option C — `get_object()` streaming (chosen):**
Use MinIO's `get_object()` which returns an HTTP response object. Pass it directly to `pd.read_csv()`.
- **Chosen:** Industry standard pattern. No intermediate file on disk. No extra storage cost. Works identically in local Docker and cloud (AWS S3 uses the same API).

**Research:** Confirmed by reviewing MinIO Python SDK documentation and AWS S3 + pandas usage examples. The pattern `pd.read_csv(s3_response)` is widely used in production data pipelines.

---

## 5. BytesIO Buffering for MinIO Responses

**Decision:** Buffer the MinIO HTTP response into `io.BytesIO` before passing to `pd.read_csv()`.

**Context:** After implementing streaming (Decision 4), the flow failed with `pandas.errors.EmptyDataError: No columns to parse from file` when using `engine="python"`.

**Root cause:** The MinIO client returns a `urllib3.response.HTTPResponse` object — a raw binary socket stream. The pandas Python CSV engine requires a text-mode, line-iterable stream. The raw socket does not satisfy this interface reliably across all pandas versions.

**Confirmation:** The backend's own `MinioService.download_file()` already used `BytesIO(response.read())` for the same reason — we just missed it the first time.

**Fix:**
```python
buf = io.BytesIO(stream.read())
df = pd.read_csv(buf, on_bad_lines=_on_bad_line, engine="python")
```

**Why this is acceptable despite buffering into memory:**
The DataFrame must be held in memory anyway for profiling. The BytesIO intermediate adds no meaningful peak memory overhead. Max file size is capped at 200MB.

---

## 6. asyncio.run() Inside Sync Prefect Tasks

**Decision:** Use `asyncio.run(_coroutine())` inside synchronous Prefect tasks that need to call asyncpg (async PostgreSQL).

**Context:** asyncpg is an async-only library. Prefect tasks by default run as synchronous functions in threads (via `ConcurrentTaskRunner`). We needed to call asyncpg from inside a sync task.

**Why it is safe:**
Prefect's `ConcurrentTaskRunner` runs each task in a separate thread. Each thread has no running event loop. `asyncio.run()` creates a new event loop for that thread, runs the coroutine to completion, then destroys the loop. There is no conflict with the main thread's event loop.

**Alternative considered — making tasks async:**
Prefect supports `async def` tasks. However, mixing async tasks with sync tasks in the same flow can cause subtle issues with Prefect's task runner. Since only the DB tasks need async, keeping them sync with `asyncio.run()` internally is cleaner and more explicit.

---

## 7. Profiling Library Choice — Custom Pandas

**Decision:** Write custom profiling logic in pandas rather than using a third-party profiling library.

**Options considered:**

| Library | Problem |
|---|---|
| **ydata-profiling** (formerly pandas-profiling) | Extremely heavy. Generates a full HTML report. Designed for human exploration, not machine-readable JSON output for an API. Slow on large files. |
| **Great Expectations** | Wrong paradigm — it validates data against pre-defined expectations. We don't have expectations upfront; we're discovering them from the data. |
| **DuckDB** | Excellent for SQL-based profiling on large files. Worth revisiting if performance becomes a bottleneck (files approaching 200MB). Not needed now. |
| **Custom pandas** (chosen) | Full control over output format. We output exactly the JSON structure the API needs. Fast enough for our 200MB cap. Fully testable. |

**Future consideration:** If files regularly approach 200MB, replace the pandas profiling with DuckDB's `SUMMARIZE` query — same output format, faster execution.

---

## 8. DQ Score Formula and Weights

**Decision:**
```
DQ Score = completeness × 0.35 + uniqueness × 0.25 + validity × 0.25 + consistency × 0.15
```

**Definitions:**

| Component | Calculation | Weight | Rationale |
|---|---|---|---|
| **Completeness** | `(1 - missing_cells / total_cells) × 100` | 35% | Missing data is the most common and impactful DQ issue. Highest weight. |
| **Uniqueness** | `(1 - duplicate_rows / total_rows) × 100` | 25% | Duplicate records cause double-counting in analysis. High impact. |
| **Validity** | `(type_conforming_cells / total_cells) × 100` | 25% | Type mismatches (e.g. "N/A" in a salary column) break downstream processing. Equal weight to uniqueness. |
| **Consistency** | `(pattern_matching_cells / total_cells) × 100` | 15% | Format inconsistency (e.g. mixed date formats) is important but less critical than missing/duplicate data. Lowest weight. |

**Validity implementation:** Pandas validates numeric/bool/datetime columns on read. Only `object` dtype columns need checking — if >50% of values parse as numeric, non-numeric values are counted as invalid. Same logic for datetime.

**Consistency implementation:** For each string column, if >50% of non-null values match a known pattern (email, phone, URL, ISO date, UK postcode, UUID), non-matching values are counted as inconsistent. Only one pattern is applied per column.

---

## 9. Outlier Detection — IQR, Advisory Only

**Decision:** Use IQR (Interquartile Range) method to detect outliers. Report as advisory information only — outliers do not affect the DQ score.

**Why IQR:**
- Non-parametric — does not assume normal distribution (most real-world data is not normal)
- Simple to understand and explain: "values more than 1.5× the IQR above Q3 or below Q1"
- Well-established in data quality literature

**Why advisory only (not part of DQ score):**
An outlier is not necessarily wrong. A salary of £500,000 in a dataset of employees might be the CEO — it is extreme but valid. Flagging it for human review is correct; automatically penalising the DQ score would be incorrect.

**Minimum row threshold:** Only run outlier detection if a column has ≥ 30 non-null values. Below this, IQR is statistically unreliable — one or two extreme values can distort Q1/Q3 significantly.

**Note:** UK postcode chosen over US zip code for pattern matching, as the project is UK-focused.

---

## 10. Code Organisation — clients.py + dq_logic.py + thin dq_flow.py

**Decision:** Split `dq_flow.py` into three files:

```
prefect/
├── clients.py          # Factory functions for MinIO and PostgreSQL
├── flows/
│   ├── dq_flow.py      # @task + @flow definitions only (thin wrappers)
│   └── dq_logic.py     # Pure business logic — no Prefect imports
└── tests/
    └── test_dq_logic.py
```

**Context:** The original single `dq_flow.py` mixed orchestration (`@task`, `@flow`, logging) with business logic (profiling math, DQ scoring, recommendation rules). This made it hard to:
- Test logic without a Prefect context
- Understand the flow DAG at a glance
- Reuse logic outside Prefect (notebooks, CLI)

**Research into Prefect's official guidance:**
Prefect's docs recommend keeping logic inside `@task` and using `.fn()` to test (e.g. `my_task.fn(arg)` bypasses the Prefect wrapper). This works for simple, short tasks.

**Why we deviated:**
Our tasks are not simple. `profile_data` with full per-column profiling is 150+ lines of logic. The `.fn()` approach adds a `disable_run_logger()` context manager to every test. For complex growing logic, a clean module separation is clearer than the framework's escape hatch.

**Rule applied:**

| Task | Logic stays in task? | Reason |
|---|---|---|
| `load_dataframe_from_minio` | Yes (mostly) | It is all I/O — MinIO client + error handling |
| `update_run_status` | Yes | It is all I/O — PostgreSQL write |
| `save_results_to_db` | Yes | It is all I/O — PostgreSQL write |
| `profile_data` | No → `dq_logic.py` | Complex growing business logic |
| `calculate_dq_score` | No → `dq_logic.py` | Pure math, highly testable |
| `generate_recommendations` | No → `dq_logic.py` | Complex growing rule logic |

---

## 11. Client Factory Functions, Not Singletons

**Decision:** `get_minio_client()` and `get_pg_conn()` are factory functions that create a fresh client on every call.

**Context:** Client initialisation (MinIO endpoint, PostgreSQL credentials) was duplicated inline in every task that needed it.

**Why not module-level singletons:**
- Prefect can run tasks in separate processes (ProcessTaskRunner, Ray, Dask). Each process imports the module fresh — a singleton would be re-created per process anyway, giving false safety.
- Environment variables might not be loaded at import time in all deployment scenarios. Factory functions read them at call time, which is always safe.
- Mocking is clean: `mocker.patch("flows.dq_flow.get_minio_client")`.

**Why not a connection pool for PostgreSQL:**
Each Prefect task is short-lived and runs in a thread. A connection pool would require async lifecycle management across threads. Per-task connections (`asyncpg.connect()` + `conn.close()`) are simpler and correct for this workload.

---

## 12. PYTHONPATH for Prefect Import Resolution

**Decision:** Set `PYTHONPATH=/prefect` in the `prefect-worker` Docker service environment.

**Context:** After splitting into `clients.py` (at `/prefect/`) and `flows/dq_logic.py` (at `/prefect/flows/`), imports broke at runtime.

**Root cause:** When Prefect loads a flow from the entrypoint `flows/dq_flow.py:dq_analysis_flow`, it uses `importlib.util.spec_from_file_location` and adds the **file's directory** (`/prefect/flows`) to `sys.path` — not the working directory (`/prefect`). So:
- `from clients import ...` → looked for `/prefect/flows/clients.py` ❌
- `from flows.dq_logic import ...` → looked for `/prefect/flows/flows/dq_logic.py` ❌

**Fix:** `PYTHONPATH=/prefect` ensures `/prefect` is always in `sys.path`, making both imports resolve correctly.

**Benefit:** The future transform flow at `flows/transform_flow.py` can also `from clients import ...` without any additional configuration.

---

## 13. Prefect Task Testing via .fn()

**Research finding:** Prefect 3 officially recommends testing tasks by calling `.fn()` on the decorated function, bypassing the Prefect wrapper:

```python
from prefect.logging import disable_run_logger

def test_my_task():
    with disable_run_logger():
        result = my_task.fn(arg1, arg2)
```

**Where we apply this:** For I/O tasks (`update_run_status`, `save_results_to_db`, `load_dataframe_from_minio`) that require mocking external services, `.fn()` is the right testing approach.

**Where we don't need it:** Functions in `dq_logic.py` have no Prefect imports. They are plain Python functions tested directly without `.fn()` or `disable_run_logger()`.

---

## 14. df.copy() at the Start of profile_dataframe()

**Decision:** The first line of `profile_dataframe()` is `df = df.copy()`.

**Why:** Prefect retries a task by calling it again with the same arguments. If `profile_dataframe` modifies the DataFrame in-place (e.g. dropping rows, filling values during profiling), a retry would receive an already-mutated DataFrame, producing wrong results.

`df.copy()` ensures each attempt starts with an untouched copy of the input. This is especially important as the function grows to include per-column analysis that may need to mutate data (e.g. casting columns for type detection).

---

## 15. Code vs LLM Boundary — Metadata vs Decisions

**Decision:** Code scrapes objective facts (metadata). LLM interprets those facts and makes recommendations.

**The boundary:**

```
Code answers:  "What is in this data?"
LLM answers:   "What should we do about it?"
Human decides: "Do I agree?"
```

**What code produces (deterministic, testable):**
- Column type detection (numeric / string / date / bool)
- Per-column stats: null count, null %, min, max, mean, median, std
- Outlier count via IQR
- Pattern detection: email, phone, URL, date, postcode, UUID
- Cardinality: unique count, top values
- Dataset-level: duplicates, malformed rows, overall DQ score

**What LLM adds (semantic, context-aware):**
- Fill strategy per column based on column name + distribution (e.g. "salary → median because right-skewed")
- Row drop recommendations when a column is a key (e.g. "customer_id nulls → drop the row")
- Human-readable explanation for each recommendation
- Pandas code for custom transforms from natural language descriptions

**Key constraint:** Recommendations must be reasonable without LLM. If the LLM call fails, simple code rules apply as defaults (median for numeric nulls, mode for categorical nulls, drop for duplicates). LLM is an enhancement, not a hard dependency.

**Why this boundary matters for prompting:** The LLM never receives raw CSV rows. It receives the structured `column_profiles` dict — a compact JSON of facts. This makes prompts smaller, cheaper, more reliable, and easier to test.

---

## 16. Recommendations JSON Schema

**Decision:** The recommendations JSON has a fixed schema agreed between the DQ flow (producer) and the Transform flow (consumer).

```json
{
  "schema": {
    "column_name": { "type": "int|float|string|date|bool", "nullable": true }
  },
  "missing_values": {
    "column_name": { "strategy": "median|mean|mode|fill|drop_row", "value": null }
  },
  "duplicates": {
    "strategy": "drop", "subset": [], "keep": "first|last"
  },
  "normalization": {
    "columns": ["column_name"]
  },
  "custom_transforms": [
    {
      "description": "human-readable description",
      "type": "computed_column|rename_column|filter_rows|cast_type",
      "output_column": "new_col",
      "logic": "pandas expression or lambda"
    }
  ],
  "_metadata": {
    "generated_at": "ISO timestamp",
    "dq_score": 93.98,
    "issues_found": { "missing": 2, "duplicates": 1, "type_mismatches": 0 }
  }
}
```

**Why a fixed schema:**
- The Transform flow must be able to parse and apply recommendations deterministically
- The user edits this JSON in the review step — a predictable structure makes that feasible
- LLM output must conform to this schema (validated before saving)

**Stored in PostgreSQL as JSONB** — allows querying specific fields without unpacking the entire blob, and supports future ML training on (generated, approved) recommendation pairs.

---

## 17. Docker Multi-Process — trap + wait -n

**Decision:** The `prefect-worker` container runs two processes: the Redis worker and the Prefect worker, managed with `trap` and `wait -n`.

**Context:** We need both processes running in the same container. The Redis worker polls the job queue. The Prefect worker executes flows triggered by the Redis worker.

**Why not two separate containers:**
The Redis worker calls `run_deployment()` which talks to the Prefect API. Both processes need identical environment variables and the Prefect flow code. Keeping them in one container simplifies configuration and avoids network coordination.

**The pattern:**
```bash
bash -c "
  prefect deploy --all &&
  python redis_worker.py &
  REDIS_PID=$! &&
  trap 'kill -TERM $REDIS_PID $PREFECT_PID 2>/dev/null' TERM INT &&
  prefect worker start --pool default-agent-pool &
  PREFECT_PID=$! &&
  wait -n &&
  kill -TERM $REDIS_PID $PREFECT_PID 2>/dev/null
"
```

**Why `bash` not `sh`:** Alpine Linux uses `dash` for `sh`, which does not support `wait -n` (wait for any child to exit). `bash` is required.

**Why `wait -n`:** If either process dies, `wait -n` returns immediately and the cleanup `kill` fires, stopping the other process. This ensures Docker detects the failure and restarts the container (`restart: unless-stopped`), rather than leaving a zombie container with one dead process.

**Why `prefect deploy --all` first:** Flows must be registered with the Prefect server before the worker starts polling. Running it as the first command in the startup sequence guarantees ordering.

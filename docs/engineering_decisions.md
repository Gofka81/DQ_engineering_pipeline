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
18. [Single Jobs Queue with job_type Routing](#18-single-jobs-queue-with-job_type-routing)
19. [JSONB for DQ Scores — 5 Dimensions vs Single Float](#19-jsonb-for-dq-scores--5-dimensions-vs-single-float)
20. [Transform Flow — Self-Contained, Not Shared Tasks](#20-transform-flow--self-contained-not-shared-tasks)
21. [invalid_count — Minority Invalid Value Detection](#21-invalid_count--minority-invalid-value-detection)
22. [pandas Version Pin — 2.x, Not 3.0](#22-pandas-version-pin--2x-not-30)
23. [asyncpg Returns JSONB as Strings](#23-asyncpg-returns-jsonb-as-strings)
24. [apply_recommendations() Order of Operations](#24-apply_recommendations-order-of-operations)
25. [Redis Push Failure — Leave Run in PENDING, Return 503](#25-redis-push-failure--leave-run-in-pending-return-503)
26. [Recommendations JSON Validated with Pydantic Before Persisting](#26-recommendations-json-validated-with-pydantic-before-persisting)
27. [CORS — Wildcard in Development, Explicit Origins in Production](#27-cors--wildcard-in-development-explicit-origins-in-production)
28. [LLM Enrichment — Runner → Validator Pattern](#28-llm-enrichment--runner--validator-pattern)
29. [Column-Centric Recommendations Format](#29-column-centric-recommendations-format)
30. [LLM Prompts Co-located with Logic, Not Separated](#30-llm-prompts-co-located-with-logic-not-separated)
31. [LLM Provider — Groq + Llama, Not Anthropic](#31-llm-provider--groq--llama-not-anthropic)
32. [Compact CSV Profile Format for LLM Prompts](#32-compact-csv-profile-format-for-llm-prompts)
33. [LLM Returns Partial Diff, Not Full Recommendations](#33-llm-returns-partial-diff-not-full-recommendations)
34. [LLM Post-Processing Guards](#34-llm-post-processing-guards)
35. [Rate Limit Backoff in LLM Retry Loop](#35-rate-limit-backoff-in-llm-retry-loop)
36. [Rename Feature — LLM Suggestion, User Decides](#36-rename-feature--llm-suggestion-user-decides)
37. [DQ Framework — DAMA DMBOK Dimensions + MCAR/MAR/MNAR for Completeness](#37-dq-framework--dama-dmbok-dimensions--mcarmarmnar-for-completeness)
38. [Code vs LLM Boundary — Revised After Research](#38-code-vs-llm-boundary--revised-after-research)
39. [Standard Alignment — Honest Scope Assessment](#39-standard-alignment--honest-scope-assessment)
40. [Formal MAR Detection — scipy Point-Biserial + Chi-Square](#40-formal-mar-detection--scipy-point-biserial--chi-square)
41. [Numeric Sentinel Detection — 3×IQR Fence + Absolute Count ≥5](#41-numeric-sentinel-detection--3iqr-fence--absolute-count-5)
42. [Z-Score vs Min-Max Normalization — Selected by Outlier Presence](#42-z-score-vs-min-max-normalization--selected-by-outlier-presence)
43. [Lean Baseline for LLM — Strip Default Fields Before Sending](#43-lean-baseline-for-llm--strip-default-fields-before-sending)
44. [Format Inconsistency Detection — Partial Castability, Not Regex Patterns](#44-format-inconsistency-detection--partial-castability-not-regex-patterns)
45. [Type Cast Guard — Skip Unsafe Casts, Don't Silently Corrupt](#45-type-cast-guard--skip-unsafe-casts-dont-silently-corrupt)
46. [Frontend Stack — React + Vite + TypeScript + Tailwind CSS v4](#46-frontend-stack--react--vite--typescript--tailwind-css-v4)
47. [Sidebar Layout — Run ID as Primary Identifier](#47-sidebar-layout--run-id-as-primary-identifier)
48. [SSE Authentication — JWT via Query Parameter](#48-sse-authentication--jwt-via-query-parameter)
49. [SSE Reconnect Prevention After Terminal State](#49-sse-reconnect-prevention-after-terminal-state)
50. [Active Run State — sessionStorage Persistence](#50-active-run-state--sessionstorage-persistence)
51. [Recommendation Diff — Client-Side Pure Function](#51-recommendation-diff--client-side-pure-function)
52. [Dark Mode — Tailwind v4 Class Strategy with localStorage Persistence](#52-dark-mode--tailwind-v4-class-strategy-with-localstorage-persistence)
53. [Pydantic Silent Field Stripping — transform_hint and transform_code](#53-pydantic-silent-field-stripping--transform_hint-and-transform_code)
54. [After-Transform Issue Counts — Strategy-Based Outlier Counting](#54-after-transform-issue-counts--strategy-based-outlier-counting)
55. [Per-Run File Logging — Named Logger + Shared File Handler](#55-per-run-file-logging--named-logger--shared-file-handler)
56. [LLM Call Audit Logging — Timing and Token Counts](#56-llm-call-audit-logging--timing-and-token-counts)
57. [generate_missing_transform_codes — LLM 2 in Transform Flow](#57-generate_missing_transform_codes--llm-2-in-transform-flow)
58. [Download Button Placement — RunHeader, Not COMPLETED Card](#58-download-button-placement--runheader-not-completed-card)
59. [AWAITING_REVIEW UI — Issues Bar, Rename Checkboxes, Expandable Column Rows](#59-awaiting_review-ui--issues-bar-rename-checkboxes-expandable-column-rows)
60. [MinIO Object Lifecycle Policies — Raw and Curated Buckets](#60-minio-object-lifecycle-policies--raw-and-curated-buckets)

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
| **Groq API** | LLM for semantic enrichment of recommendations (`llama-3.3-70b-versatile`). Groq chosen over Anthropic for faster inference and lower cost. Custom transforms (natural language → pandas code) are a future Phase 3 item. |

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

## 6. Native Async Prefect Tasks (Updated — was asyncio.run())

**Decision:** All Prefect tasks that call asyncpg are declared as `async def` tasks. Prefect 3 runs async tasks natively on its own event loop.

**Context:** asyncpg is an async-only library. We originally used `asyncio.run(_inner())` inside sync tasks (each creating its own event loop per thread). This was replaced with native `async def` tasks for two reasons:

1. **LLM integration:** The Anthropic SDK (`anthropic.AsyncAnthropic`) is async. Async tasks can `await` LLM calls directly without the `asyncio.run()` nesting anti-pattern.
2. **Prefect 3 support:** Prefect 3 fully supports `async def` tasks on its `AsyncIOTaskRunner`. No mixing issues.

**Why the original approach was wrong:**
`asyncio.run()` creates a brand-new event loop, runs the coroutine to completion, then tears the loop down — on every task call. This is acceptable for isolated DB calls but breaks when an outer async context already exists (which LLM SDK calls require).

**Change:** Removed `asyncio.run()` wrapper and inner `async def _inner()` pattern. Tasks are now directly `async def`, and all awaits are at the top level of the task function.

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

**Validity implementation:** Pandas validates numeric/bool/datetime columns on read. Only `object` dtype columns need checking — if >50% of values parse as numeric, non-numeric values are counted as invalid. Same logic for datetime. **Known gap:** native `int`/`float` columns are not range-validated (e.g. `age = -5` registers as valid). This makes validity close to decorative for already-typed numeric data.

**Consistency implementation:** For each string column, if >50% of non-null values match a known pattern (email, phone, URL, ISO date, UK postcode, UUID), non-matching values are counted as inconsistent. Only one pattern is applied per column. **Important:** this is within-column format consistency, not cross-column rule consistency — "end_date < start_date" type violations are not checked.

**On the weights (35 / 25 / 25 / 15):** These are project-specific heuristics. No industry standard (ISO/IEC 25012, DAMA DMBOK) specifies numeric weights for DQ dimensions — the relative importance depends entirely on the use case. The weights chosen here reflect a common practitioner view: completeness is the most impactful issue for most CSV datasets, uniqueness and validity are equally important, consistency (format) is lowest. These weights should be declared as heuristic choices in any academic writeup, not as derived from a standard.

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

**Decision:** The recommendations JSON has a fixed schema agreed between the DQ flow (producer) and the Transform flow (consumer). See Decision #29 for the migration to the current column-centric format.

**Current schema (column-centric, since Decision #29):**

```json
{
  "columns": {
    "salary": {
      "type": "float",
      "nullable": true,
      "missing_values": { "strategy": "median", "value": null },
      "normalize": true,
      "warning": "drop_row will remove up to 3 rows...",
      "note": null
    }
  },
  "duplicates": { "strategy": "drop", "subset": [], "keep": "first" },
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

---

## 18. Single Jobs Queue with job_type Routing

**Decision:** Use one Redis queue (`jobs`) with a `job_type` field in the payload, rather than separate queues per flow type.

**Context:** When the Transform flow was added, the initial design had two queues: `dq_jobs` and `transform_jobs`. The Redis worker would BRPOP on both.

**Why a single queue is better:**

| Separate queues | Single queue + job_type |
|---|---|
| Two queue names to configure in `.env` | One setting: `REDIS_JOBS_QUEUE=jobs` |
| Worker logic must BRPOP on a list of queues | Worker always BRPOPs the same queue |
| Adding a third flow type requires a new queue and env var | Adding a third flow type adds one entry to `_DEPLOYMENTS` dict |
| Payload is identical — only the queue differs | Payload carries the routing information itself |

**Message format:**
```json
{
  "job_type":   "dq_analysis | transform",
  "run_id":     "uuid-str",
  "file_id":    "uuid-str",
  "minio_path": "uuid/filename.csv"
}
```

**Worker routing:**
```python
_DEPLOYMENTS = {
    "dq_analysis": "DQ Analysis/dq-analysis",
    "transform":   "Transform/transform",
}
```

Unknown `job_type` values are logged and skipped — no crash, no message loss.

---

## 19. JSONB for DQ Scores — 5 Dimensions vs Single Float

**Decision:** Store DQ scores as `dq_scores_before JSONB` and `dq_scores_after JSONB` (5 values), replacing the original `dq_score_before FLOAT` and `dq_score_after FLOAT` columns.

**Context:** The original schema stored a single composite float. After researching industry DQ evaluation standards and reflecting on the primary use of these scores (feeding the LLM and showing the user), it became clear a single number loses too much information.

**Options considered:**

| Option | Problem |
|---|---|
| Single float | Hides which dimension improved. A score of 97 could mean perfect completeness and bad validity, or vice versa. |
| 5 flat columns (`completeness_before`, `uniqueness_before`, ...) | Rigid schema. Adding a sixth dimension requires a migration. |
| JSONB (chosen) | Schema-flexible. All 5 scores in one read. PostgreSQL can index and query individual JSONB keys. Consistent with the existing `recommendations_generated JSONB` pattern. |

**Shape stored:**
```json
{
  "overall":      97.77,
  "completeness": 96.29,
  "uniqueness":   100.0,
  "validity":     96.29,
  "consistency":  100.0
}
```

**Why this matters for LLM:** The LLM receives all 5 scores and can say "your completeness improved from 96 → 100 but validity is still 98 — consider reviewing the salary column patterns." A single float cannot support this.

---

## 20. Transform Flow — Self-Contained, Not Shared Tasks

**Decision:** `transform_flow.py` defines its own `update_run_status` and `load_dataframe_from_minio` tasks rather than importing them from `dq_flow.py`.

**Context:** Both flows need `update_run_status` (PostgreSQL write) and `load_dataframe_from_minio` (MinIO read). Sharing these tasks would mean importing from one flow file into another.

**Why not import from dq_flow.py:**
- Importing `dq_flow.py` would also import and register `dq_analysis_flow` as a side effect. Prefect may attempt to register the flow again during the transform deployment, causing subtle issues.
- Self-contained files are easier to read — all tasks for a flow are in one place.
- The duplicated tasks are pure I/O (< 20 lines each). The duplication cost is low; the coupling risk of cross-importing flow files is higher.

**Rule applied:** Share logic from `dq_logic.py` (pure functions). Do not cross-import between flow files. Duplicate small I/O tasks where needed.

---

## 21. invalid_count — Minority Invalid Value Detection

**Decision:** Track `invalid_count` per column: the number of non-null values that will become `NaN` when the schema cast is applied (e.g. `"N/A"` in a numeric column).

**Context:** The `missing_values` recommendation section was originally only triggered by actual nulls (`null_count > 0`). This missed a real-world pattern: a column like `salary` where most values are valid floats but a minority are string sentinels (`"N/A"`, `"unknown"`, `"-"`). These look non-null in the raw data but break after a `pd.to_numeric()` cast.

**How it works:**
```python
if "object" in str(df[col].dtype) and detected_type == "numeric":
    coerced = pd.to_numeric(non_null, errors="coerce")
    invalid_count = int(coerced.isna().sum())
```

**Trigger logic updated:**
```python
needs_fill = cp["null_count"] > 0 or cp.get("invalid_count", 0) > 0
```

This means `missing_values` is recommended for a column with `"N/A"` strings even if `null_count == 0`.

**Why this distinction matters:** Without `invalid_count`, the transform would cast `"N/A"` → `NaN` and then not fill it (because no fill strategy was recommended). The user would silently lose data. With `invalid_count`, a `median` fill strategy is recommended and applied automatically.

---

## 22. pandas Version Pin — 2.x, Not 3.0

**Decision:** Pin `pandas>=2.2.0,<3.0.0` in `prefect/requirements.txt`.

**Context:** During development, the `.venv` resolved to pandas 3.0 (newly released). This caused 10 test failures with no obvious error messages.

**Root cause:** pandas 3.0 changed the default dtype for string columns from `object` to `str`. Our type detection relied on:
```python
if "object" in str(df[col].dtype):
    ...
```
On pandas 3.0, `str(df["salary"].dtype)` returns `"str"`, not `"object"`. The condition silently evaluated to `False`, bypassing all string-column logic.

**Why stay on 2.x rather than migrate to 3.0:**
- pandas 2.3.3 is the latest stable 2.x release — well-supported, no security concerns.
- Migrating to 3.0 would require auditing every `dtype` check across the codebase — significant risk for no current benefit.
- The `<3.0.0` cap makes the breaking change explicit in `requirements.txt`, so a future upgrade is a deliberate, reviewed decision.

---

## 23. asyncpg Returns JSONB as Strings

**Decision:** When reading JSONB columns via asyncpg, always decode with `json.loads()` rather than assuming the value is already a Python dict.

**Context:** The Transform flow's `load_approved_recommendations` task failed in production with:
```
ValueError: dictionary update sequence element #0 has length 1; 2 is required
```

**Root cause:** asyncpg returns PostgreSQL JSONB columns as raw JSON strings, not Python dicts. Calling `dict(json_string)` iterates over characters of the string — each character has length 1, not 2 (key-value pair), causing the error.

**Fix:**
```python
raw = row["recommendations_approved"]
return json.loads(raw) if isinstance(raw, str) else dict(raw)
```

**Why the isinstance check:** asyncpg's behaviour may change across versions or with registered codecs. The guard handles both the current string-returning behaviour and any future version that returns a dict directly.

**Lesson:** This bug only surfaces at runtime against a real PostgreSQL instance — it cannot be caught by unit tests that mock the DB layer. Added to the known gotchas list in `CLAUDE.md`.

---

## 24. apply_recommendations() Order of Operations

**Decision:** The Transform step applies recommendations in this fixed order: schema cast → fill/drop missing → int recast → deduplicate → normalize.

**Rationale for each step's position:**

| Step | Why this position |
|---|---|
| **1. Schema cast** | Must come first — converts `"N/A"` → `NaN` in numeric columns, creating the missing values that the fill step will handle. |
| **2. Fill / drop missing** | After cast — now handles both original nulls and newly coerced NaNs in one pass. `drop_row` columns are collected and dropped in a single `dropna(subset=[...])` call for efficiency. |
| **3. Int recast** | After fill — if a column is declared `int` but had NaN, it could not be cast to int before filling. After filling, if no NaN remains, safely cast to int64. |
| **4. Deduplicate** | After fill — filling may resolve rows that appeared different only because of NaN. Deduplication on clean data is more semantically correct. |
| **5. Normalize** | Last — min-max scaling on the final, clean data. Scaling before filling would produce incorrect bounds if NaN values were present. |

**Input safety:** `apply_recommendations()` starts with `df = df.copy()`. This mirrors the same decision in `profile_dataframe()` (Decision 14) — Prefect retries pass the same object, and the caller's DataFrame must not be mutated.

---

## 25. Redis Push Failure — Leave Run in PENDING, Return 503

**Decision:** If `redis.lpush()` raises an exception after the DB commit, return HTTP 503 to the client without changing the run status. The run stays in `PENDING`.

**Options considered:**

| Option | Problem |
|---|---|
| Mark run as `FAILED` immediately | Race condition: if the command was sent but the ACK was lost (network partition), Redis may have received it. The worker would then try to process a `FAILED` run, overwriting the status back to `ANALYZING` — confusing and hard to debug. |
| Delete run + file records | Complex rollback across MinIO, PostgreSQL, and Redis. Over-engineering for a rare failure mode. |
| Leave in `PENDING`, return 503 | Safe in all scenarios. If Redis had the job, it processes normally. If not, the client retries. Stale `PENDING` runs are detectable by age (`created_at` older than N minutes with no status change). |

**Future:** Add a `POST /api/files/{file_id}/requeue` endpoint to manually re-trigger analysis for runs stuck in `PENDING`. This is a better UX than asking users to re-upload.

---

## 26. Recommendations JSON Validated with Pydantic Before Persisting

**Decision:** `RecommendationsUpdate` accepts a `RecommendationsSchema` Pydantic model, not a bare `dict[str, Any]`. The validated object is serialised back to a dict via `.to_flow_dict()` before being saved to the database.

**Why:** The recommendations JSON is the contract between the user-facing API and `apply_recommendations()`. Accepting `dict[str, Any]` would pass arbitrary JSON through to the transform flow, causing opaque runtime errors deep in pandas. With a typed schema:

- Invalid strategies (`"strategy": "delete_everything"`) are rejected at the API boundary with a clear 422 response.
- Required fields (e.g. `value` when `strategy == "fill"`) are enforced by a `field_validator`.
- LLM-generated recommendations (Phase 3) can be validated using the same model before being offered to the user for review.
- The schema documents the allowed structure — it is the spec, not just validation.

**Trade-off:** The schema must be kept in sync with `apply_recommendations()`. If a new strategy is added to the logic, the Pydantic `Literal` must also be updated. Accepted as a straightforward maintenance task.

---

## 27. CORS — Wildcard in Development, Explicit Origins in Production

**Decision:** `CORSMiddleware` is configured with `allow_origins=settings.CORS_ORIGINS`. The default is `["*"]` (development). Production deployments must set `CORS_ORIGINS` explicitly in `.env`.

**Why:** Without CORS headers, any browser-based frontend on a different origin cannot call the API. Wildcard is acceptable in local development. In production, locking it to the frontend's domain prevents cross-origin requests from arbitrary sites.

**Configuration:** Add `CORS_ORIGINS=["https://yourdomain.com"]` to `.env` for production.

---

## 28. LLM Enrichment — Runner → Validator Pattern

**Decision:** Add an LLM enrichment step between `generate_recommendations` and `save_results_to_db` using a two-stage pattern: Runner (JSON generation, up to 3 retries) → Validator (structural check). `temperature=0`, `max_tokens=512`.

**Context:** The code-generated recommendations are deterministic and correct but semantically blind — they cannot use column *names* to reason. A column called `customer_id` with nulls should use `drop_row`, not `median`. A column called `sal` is clearly `salary` and should be renamed. The LLM sees column names, stats, patterns, and actual sample rows, giving it the context to make smarter decisions.

**Why Runner → Validator (not Planner → Runner → Validator):**
An earlier version had a separate Planner call that produced free-text reasoning before the Runner generated JSON. This was removed: the reasoning rules embedded directly in `_RUNNER_SYSTEM` as bullet instructions proved sufficient for a capable LLM, and a single call is cheaper and faster. The Planner's output was text that immediately fed into the next call anyway — folding it into the system prompt eliminated one full API round-trip per attempt.

**Why a separate `llm_enrichment.py` file (Decision #10 pattern):**
All LLM logic lives in a pure Python module with no Prefect imports. The `@task` in `dq_flow.py` is a thin wrapper. This keeps `llm_enrichment.py` fully unit-testable without a Prefect context and mirrors the `dq_logic.py` pattern.

**`temperature=0`:**
Set for deterministic outputs — the same profile always produces the same recommendations. This makes prompt changes testable: if a rule is added or removed, the change in output is attributable to the prompt, not random variance.

**`max_tokens=512`:**
The LLM returns a partial diff (Decision #33) — only changed columns, only changed keys. A typical 10-column dataset produces ~125–200 completion tokens. 512 is a conservative cap that prevents token runaway while leaving headroom for verbose notes.

**Sample rows — stratified sampling:**
The LLM receives actual row data alongside the profile. Without sample rows it cannot detect sentinel strings (`"N/A"`, `"unknown"`) in numeric columns or infer meaning from abbreviated names (`sal`, `dept_cd`). Sampling strategy: `min(10% of rows, 25)` — proportional for small datasets, capped to control token cost on large files. Stratified: null-containing rows first (up to n//3), then systematic every k-th row, then random fill.

**Graceful fallback — never raises:**
`enrich_recommendations()` returns `base_recommendations` unchanged if: API key is missing, `groq` package is not installed, or all 3 retries are exhausted. The pipeline always reaches `AWAITING_REVIEW`. LLM enrichment is an enhancement, not a hard dependency (Decision #15).

**`note` per column:**
The LLM adds a `note` field on each column entry it changes — one sentence explaining the reasoning (e.g. "Salary is right-skewed — using median is more robust than mean"). Columns the LLM leaves unchanged have `note: null`. Notes are co-located with the recommendation they explain, making the JSON self-documenting for users in the review step.

---

## 29. Column-Centric Recommendations Format

**Decision:** Collapse `schema`, `missing_values`, and `normalization` top-level keys into a single `columns` dict where every field for a column lives in one place.

**Context:** The original format spread per-column information across five separate top-level keys:

```
schema[col].type + nullable
missing_values[col].strategy + value
normalization.columns[]           ← col appears as list membership
_metadata.warnings[]              ← col appears inside a list of dicts
_metadata._llm_explanation[col]   ← col appears as a key in another nested dict
```

To understand what would happen to a single column during transformation, you had to look in five places. To send the recommendations to the LLM for enrichment, the model had to mentally join five separate structures.

**New format:**
```json
{
  "columns": {
    "salary": {
      "type": "float",
      "nullable": true,
      "missing_values": { "strategy": "median", "value": null },
      "normalize": true,
      "warning": "drop_row will remove up to 3 rows (15% of dataset)...",
      "note": null
    }
  },
  "duplicates": { "strategy": "drop", "subset": [], "keep": "first" },
  "custom_transforms": [],
  "_metadata": { "generated_at": "...", "dq_score": 67.4, "issues_found": {} }
}
```

**Why this is better:**

| Concern | Old format | New format |
|---|---|---|
| Reading one column's full treatment | Join 5 keys | Read one `columns[col]` dict |
| LLM prompt size | Full schema + missing_values + normalization blocks | One `columns` block — same information, less structure |
| LLM validation | 3 separate validation blocks in `validate_llm_output()` | 1 `columns` block validator |
| Warning co-location | List in `_metadata.warnings` — requires searching by column name | `columns[col].warning` — directly on the column |
| Pydantic schema | `ColumnSchema` + `MissingValueStrategy` + `NormalizationConfig` as separate models | Single `ColumnConfig` model |

**What stayed top-level:** `duplicates`, `custom_transforms`, `_metadata` — these are dataset-level, not per-column, so top-level is correct for them.

**Migration:** `apply_recommendations()` and `build_recommendations()` updated in `dq_logic.py`. Pydantic schema updated in `backend/app/schemas/file.py`. LLM validator and prompt updated in `llm_enrichment.py`. All 218 tests updated and passing.

---

## 30. LLM Prompts Co-located with Logic, Not Separated

**Decision:** Keep `_PLANNER_SYSTEM`, `_RUNNER_SYSTEM`, and `_RUNNER_RETRY_USER` prompt strings in `llm_enrichment.py` alongside the validation and enrichment logic. Do not move them to a separate `prompts/` directory or config file.

**Context:** As the LLM layer grew to multiple prompts with retry templates, the question arose of whether to separate prompts into their own files (e.g. `prompts/runner.jinja2`, `prompts.yaml`).

**Why separation would hurt here:**
The `_RUNNER_SYSTEM` prompt and `validate_llm_output()` form a **contract** — the prompt tells the LLM what schema to produce, and the validator checks that the schema was followed. They must always be updated together. If the prompt moves to a separate file:
- A developer editing the prompt might not know to update the validator
- The coupling becomes invisible — a drift between prompt and validator only surfaces as a runtime failure, not a test failure
- Code review diffs no longer show the prompt change and the validator change side by side

**When separation makes sense (not yet applicable here):**
- Non-engineers (domain experts, PMs) need to edit prompts without touching Python
- 5+ prompts spread across multiple files become hard to scan
- A/B testing prompt variants requires swapping files without code changes

**Mitigation:** Added an explicit coupling comment in `llm_enrichment.py`:
```python
# NOTE: _RUNNER_SYSTEM schema must stay in sync with validate_llm_output().
# Any structural change here (key names, strategies) needs a matching change there.
```

**Rule:** Separate prompts when you have a concrete reason to. Not as a default.

---

## 31. LLM Provider — Groq + Llama, Not Anthropic

**Decision:** Use Groq's API with `llama-3.3-70b-versatile` via the `groq` Python SDK (OpenAI-compatible interface). Do not add LiteLLM, LangChain, or any other abstraction layer.

**Context:** The initial design used Anthropic Claude. Switched to Groq for two reasons: faster inference (Groq's custom LPU hardware) and lower cost at the current stage of development. The switch had minimal code impact because Groq exposes an OpenAI-compatible `chat.completions.create` interface — the same call signature as most providers.

| Approach | Examples | What it gives you |
|---|---|---|
| **Provider SDK directly** (chosen) | `groq`, `anthropic`, `openai` | Minimal dependencies, no abstraction overhead |
| **OpenAI-compatible REST** | Groq, Together AI, Fireworks, Ollama | One HTTP client works across providers |
| **Abstraction library** | LiteLLM, LangChain | Single call signature across 100+ providers; useful when switching providers frequently |

**Why Groq + llama-3.3-70b-versatile:**
- Fast inference — typical prompt completes in ~1–2 seconds vs 5–10s on hosted APIs
- llama-3.3-70b is a capable instruction-following model that reliably produces valid JSON when given a clear schema in the system prompt
- OpenAI-compatible interface — `client.chat.completions.create()` — identical to most other providers, making a future swap cheap
- `temperature=0` is supported and produces deterministic outputs

**Why not Anthropic:**
- Claude's stronger JSON compliance was the original motivation, but llama-3.3-70b with explicit schema rules in the system prompt achieves the same result
- Groq's inference speed and cost are better for iterative prompt development

**Model version pinning:**
`_MODEL = "llama-3.3-70b-versatile"` is a module-level constant. `llama-3.1-70b-versatile` was decommissioned mid-development — updating the constant in one place was the only required change. This pattern is intentional.

**If provider flexibility becomes a requirement:**
LiteLLM wraps Groq, Anthropic, OpenAI, Gemini, Bedrock, Ollama, and others behind a single `litellm.completion()` call. The change would be ~5 lines in `llm_enrichment.py`. This is a cheap future migration, not a reason to add complexity now.

---

## 32. Compact CSV Profile Format for LLM Prompts

**Decision:** `_build_llm_profile()` returns a compact CSV string — two separate tables (numeric columns and string/other columns) plus sample rows — instead of a JSON dict.

**Context:** The original implementation serialised the full profile dict to indented JSON (`json.dumps(profile, indent=2)`). For a 5-column dataset this produced ~614 input tokens. With up to 3 runner attempts per run, token costs compound. More importantly, indented JSON with deeply nested keys (`column_profiles[col].stats.median`) is noisier than a flat table row for the same information.

**Why two tables, not one:**
A single CSV table covering all column types would have empty cells: `mean/median/std` are empty for string columns; `cardinality/top_values` are empty for numeric columns. Empty cells create a `0` vs `null` ambiguity — does `outliers=` mean "zero outliers" or "not computed"? Two homogeneous tables eliminate this: every cell in the numeric table is a real measurement; same for the string table.

**Format:**
```
rows,cols,missing,duplicates,completeness,uniqueness,validity,consistency
10,5,2,0,96.0,100.0,98.0,90.0

name,type,null_pct,mean,median,std,min,max,outliers
sal,numeric,10,93111,49000,52800,45000,210000,1

name,type,null_pct,unique,cardinality_pct,top_values,pattern,invalid
emp_id,string,10,9,100,E001(1) E002(1) E003(1),,0
dept_cd,string,0,3,30,FIN(4) MGT(3) HR(3),,0
email,string,0,10,100,a@c.com(1) b@c.com(1) c@c.com(1),email,1

SAMPLE ROWS:
emp_id,sal,dept_cd,email,hire_dt
...
```

**Fields kept vs dropped vs format-changed:**
- `null_count` (raw) dropped — `null_pct` is what prompt rules reference (">50% missing"); LLM has `total_rows` to compute the count if needed
- `unique_count` kept for string columns — "unique=3" is more immediately readable than computing from cardinality_pct
- `invalid_count` shown only when > 0 — signals malformed values (e.g. "N/A" in an email column) without noise on clean columns
- `outliers` always shown for numeric — `outliers=0` means "checked, found none"; absence would mean "unknown"
- `top_values` trimmed to 3 entries, space-separated as `val(count)` — enough for mode detection and sentinel spotting
- `pandas_dtype` dropped — internal implementation detail, not useful for the LLM

**Token result:** ~161 tokens for a 5-column fixture vs ~614 for JSON — ~74% reduction. Scales better as column count grows since JSON key repetition is eliminated.

---

## 33. LLM Returns Partial Diff, Not Full Recommendations

**Decision:** The LLM returns only the columns it is improving and only the keys it is changing within those columns — a partial diff. This diff is deep-merged onto the code-generated baseline.

**Context:** An earlier design had the LLM return the entire recommendations object (all columns, all fields). Problems:
1. High completion token cost — the LLM echoed back every unchanged column verbatim
2. The LLM anchored to baseline values and reproduced them without improvement
3. No clear audit trail of what the LLM actually changed vs just copied

**LLM response schema:**
```json
{
  "columns": {
    "<col>": {
      "type": "...",          (optional — only if changing)
      "missing_values": ...,  (optional — only if changing)
      "rename_to": "...",     (optional — only for abbreviated names)
      "note": "..."           (required for every included column)
    }
  }
}
```
Only `columns` is required. `_metadata`, `duplicates`, `custom_transforms` are always taken from the baseline — the LLM never touches them.

**Deep merge:**
```python
enriched = copy.deepcopy(base_recommendations)
for col, col_diff in llm_diff["columns"].items():
    if col in enriched["columns"]:
        enriched["columns"][col].update(col_diff)
```
Baseline fields not mentioned in the diff are preserved exactly. The LLM cannot accidentally wipe fields it did not intend to change.

**Validator — collect all errors before returning:**
The validator accumulates every structural error into a list before returning `(False, all_errors_joined)`. A retry prompt that gets all errors at once converges faster than one that sees errors one at a time — the LLM can fix everything in the next attempt rather than playing whack-a-mole across three attempts.

**`rename_to` as a per-column key:**
Column renaming is expressed as `columns[col].rename_to: "new_name"` rather than a `custom_transforms` entry. This keeps all per-column decisions co-located and lets the validator enforce it as a typed string field. `apply_recommendations()` handles it in a dedicated rename step after all other transforms.

**Token result:** ~125 completion tokens for a 5-column dataset. The partial diff format also makes it clearer in the DB (`recommendations_generated` JSONB) exactly what the LLM contributed vs what the code generated.

---

## 34. LLM Post-Processing Guards

**Decision:** After the LLM diff passes structural validation, a post-processing step applies four deterministic guards before the diff is deep-merged onto the baseline.

**Context:** Prompt rules alone are probabilistic — LLMs follow them most of the time but not always. Certain failure modes are structurally detectable in code and should be caught deterministically regardless of what the LLM produces.

**Guards (in order):**

1. **Strip echoed keys** — if a key in the LLM diff is identical to the baseline value, it is removed. The LLM sometimes echoes unchanged keys alongside genuine changes, adding noise and token waste.

2. **Strip `missing_values` when baseline has null** — if the baseline column has `missing_values: null`, any LLM-added imputation strategy is removed. The baseline sets `missing_values: null` when `null_pct = 0`; the profiler is authoritative on null counts. An LLM-added imputation strategy for a zero-null column is always wrong.
   - Root cause: the reasoning bullets primed the LLM toward imputation strategies before the `null_pct > 0` rule was read. The prompt bullet was fixed to add the guard ("only if null_pct > 0") and the code guard adds a second layer.

3. **Strip no-op renames** — if `rename_to == col` (same name), it is removed. A rename to the same name cannot be useful.

4. **Drop note-only columns** — after the above strips, if a column diff contains only a `note` and no substantive keys (`type`, `nullable`, `missing_values`, `rename_to`), the column is dropped entirely. A note without any real change is noise.

**`normalize` excluded from LLM scope:**
`normalize` is removed from the LLM output schema and rules entirely. The code-generated baseline already computes it correctly from outlier counts. LLM-generated normalize recommendations were consistently noisy (recommending normalization for non-numeric columns, or adding "no normalization needed" notes). Since `normalize` is never a decision the LLM can improve on, removing it from scope simplifies the prompt and eliminates a source of noise.

**Why code, not prompt:**
Prompt rules are probabilistic. These four guards express facts that are unconditionally true (a null column needs no imputation strategy, a rename to itself is meaningless). Deterministic code is the right enforcement layer for unconditional facts; prompts are the right layer for semantic reasoning.

---

## 35. Rate Limit Backoff in LLM Retry Loop

**Decision:** When the Groq SDK raises an exception after exhausting its own internal retries, our retry loop applies exponential backoff before attempting the next call — 10 seconds on the first failure, 20 seconds on the second.

**Context:** The Groq SDK has built-in HTTP-level retry logic for 429 (Too Many Requests) responses, with automatic wait based on the `retry-after` header. This is the primary defence. But if the SDK's retries are exhausted, it raises an exception that our `except Exception` catch picks up. Without backoff, we would immediately retry and hit the same rate limit again.

**Backoff logic:**
```python
is_rate_limit = "rate" in str(e).lower() or "429" in str(e)
backoff = 10 * (attempt + 1) if is_rate_limit else 0
if backoff:
    time.sleep(backoff)
```
Non-rate-limit errors (JSON parse errors, validation errors) retry immediately — backoff is only applied when the API itself is the bottleneck. Observed in testing: the SDK successfully handled a 429 internally on dataset 7 (2.8s vs ~0.8s for other datasets), so our backoff is rarely reached in practice.

---

## 36. Rename Feature — LLM Suggestion, User Decides

**Decision:** `rename_to` suggestions from the LLM are treated as user-facing suggestions, not enforced outputs. The only hard code guard is stripping no-op renames (`rename_to == col`). All other rename decisions flow through to the user for review.

**Context:** Early iterations tried to constrain the LLM's rename suggestions with structural rules (suffix matching, length checks, abbreviation whitelists). Each rule was too narrow and the LLM found workarounds, or too broad and caught legitimate suggestions. The problem is that "is this a good rename?" is a semantic question — exactly what the LLM is better at than code.

**What the LLM is actually doing:**
The LLM reads sample rows and infers semantic meaning. A column called `name` containing "Taras Chad", "Diana Prince" is plausibly `full_name`. A column called `sal` is plausibly `salary`. These are reasonable suggestions that a data engineer would make manually.

**LLM-as-judge considered and rejected:**
A second LLM call to validate rename suggestions was evaluated. Rejected because: (1) it doubles cost and latency, (2) the judge makes the same probabilistic errors as the runner, (3) the issues were either fixable at the prompt level or acceptable for user review. Issues fixable at prompt level (e.g. zero-null imputation) were fixed there. Issues that require semantic judgment are left to the user.

**Why the review step is sufficient:**
The user reviews the full recommendations JSON before approval. A bad rename suggestion (`email → email_address`) is visible and deletable in seconds. The cost of an occasional bad rename suggestion is trivially low; the cost of complex validation code that still misses cases is ongoing maintenance. The rename prompt rule guides the LLM toward data-context-driven suggestions; the user is the final filter.

---

## 37. DQ Framework — DAMA DMBOK Dimensions + MCAR/MAR/MNAR for Completeness

**Decision:** The recommendation framework is structured around the six practitioner dimensions from **DAMA DMBOK** (Data Management Body of Knowledge), supplemented by the MCAR/MAR/MNAR missing-data taxonomy from statistical literature (van Buuren, 2018).

**Context:** An internal review at a late stage of development revealed that the original framework (built incrementally) conflated several distinct problem types. Specifically:
- Sentinel values (`-999`, `"N/A"`, `"unknown"`) were tracked as `invalid_count` in the profiler but had no corresponding recommendation type
- Outlier treatment was advisory-only in the DQ score but absent from the recommendations JSON
- The `drop_column` vs `leave_null` decision used a single `>50% missing` threshold without considering *why* the column is missing — the MCAR/MAR/MNAR distinction
- Normalization was described as "for ML only" when it applies equally to cross-feature comparison, clustering, PCA, and multi-source standardization

**Why DAMA DMBOK, not ISO 8000 or full ISO/IEC 25012:**

| Standard | Scope | Applicable? |
|---|---|---|
| **ISO 8000** | Supply-chain master data quality (GDSN product records, asset registers) | No — designed for structured catalog data, not general CSV DQ |
| **ISO/IEC 25012** | 15 software data quality characteristics (inherent + system-dependent) | Partial — our 4 implemented dimensions overlap with 4 of its 15 "inherent" characteristics |
| **DAMA DMBOK** | 6 practitioner dimensions used by working data engineers | Yes — exactly the 6 dimensions we implement; pragmatic, industry-standard framing |

Earlier documentation cited "ISO 8000 alignment" — this was inaccurate and has been corrected. ISO/IEC 25012 is cited in `docs/thesis_references.md` as a related standard for academic grounding; DAMA DMBOK is the operative framework for this project's design.

**DAMA dimensions and honest implementation scope:**

| Dimension | Maps to | Actual scope (what the pipeline checks) |
|---|---|---|
| Completeness | Missing values section | Null ratio at value level. Does not check population or column-schema completeness. |
| Validity | Sentinel detection, type recast, outlier treatment | Type mismatches on `object`-dtype columns only. Native `int`/`float` are not range-validated. String and numeric sentinels both detected. Format inconsistency detected via partial castability. |
| Uniqueness | Duplicate handling | Exact row duplicates only. No fuzzy/near-duplicate matching. |
| Consistency | Type recast, date standardisation | Within-column format patterns only. Cross-column rule validation is a known gap. |
| Accuracy | (none) | Requires ground truth — out of scope |
| Timeliness | (none) | Out of scope |

**MCAR / MAR / MNAR framework for missing data:**

The missing data literature (van Buuren, 2018) establishes three mechanisms borrowed from clinical research and applied here to business CSV DQ:
- **MCAR** (Missing Completely At Random): no pattern — any imputation is unbiased
- **MAR** (Missing At Random): nulls correlate with an *observed* column — detectable statistically; imputing destroys the signal; `leave_null` is correct
- **MNAR** (Missing Not At Random): nulls correlate with an unobservable value — requires domain reasoning; LLM infers from column name and context

Previously the pipeline applied `drop_column` for any column ≥50% missing, regardless of mechanism. The revised framework detects MAR statistically (correlation check) and applies `leave_null` in code before the LLM ever sees the column.

**Known gaps for future work:**
- Fuzzy/near-duplicate detection (Levenshtein, phonetic matching)
- Sentinel value recommendation (currently flagged as `warning`, treatment not automated)
- Outlier treatment integration into the transform flow
- Consistency rules (cross-column validation — e.g., `end_date > start_date`)
- Validity range checking for native numeric columns

---

## 38. Code vs LLM Boundary — Revised After Research

**Decision:** The boundary between what code computes and what the LLM decides was formally defined after researching industry standards for each recommendation type. The LLM's scope was narrowed from "improve fill strategies" to "semantic decisions only".

**Context:** Through iterative live testing, the LLM consistently failed at tasks that appear semantic but are actually deterministic:
- Detecting correlated nulls (MAR): the LLM ignored the CORRELATED NULLS section of the profile even when `merchant_nm,txn_typ,atm,100%` was listed explicitly. After three prompt iterations, the LLM still returned `mode` fill. The fix was to compute MAR in Python and apply `leave_null` directly in the baseline.
- Choosing median vs mean: the LLM was re-reading numbers we had already computed. Skewness is a formula, not a judgment.
- Mode fill value: we provide `top_values` in the profile — the LLM was reading back a number we already computed.

**The research finding (MCAR/MAR/MNAR):** MAR detection is a statistical operation — create a binary indicator column for missingness and check whether other columns predict it (logit regression or simple correlation). This is deterministic and belongs in code. MNAR detection ("why is this column intentionally sparse?") requires understanding the domain from the column's name and context — this is genuinely semantic and belongs in the LLM.

**Revised boundary:**

| Decision | Owner | Reason |
|---|---|---|
| MAR `leave_null` (correlated nulls) | Code | Statistical fact — correlation is computable |
| Median vs mean selection | Code | Skewness formula — computable from profile stats |
| Mode for categoricals | Code | Cardinality threshold — computable |
| Drop row for ID/pattern columns | Code | Column name pattern + cardinality — detectable |
| Drop column threshold | Code | Arithmetic |
| Type inference | Code | Syntactic parsing |
| Normalization flag | Code | Range comparison |
| Sentinel detection | Code | Pattern matching |
| MNAR `leave_null` (semantic sparse) | LLM | Cannot detect from data — needs domain reasoning |
| `rename_to` suggestions | LLM | Requires reading sample values and understanding meaning |
| `note` explanations | LLM | Natural language generation |
| Custom transforms | LLM | User-defined natural language → pandas code (future) |

**Why this matters for reliability:** Prompt rules are probabilistic — the LLM follows them most of the time but not always. When a decision can be made deterministically, it must be made in code. The LLM's value is specifically in decisions where no deterministic rule exists — where semantic understanding of the column's meaning in context is required.

---

## 39. Standard Alignment — Honest Scope Assessment

**Decision:** The project frames its DQ evaluation against DAMA DMBOK's 6-dimension practitioner model, with partial coverage of ISO/IEC 25012's "inherent" data quality characteristics. This framing is made explicit in documentation rather than claiming full ISO alignment.

**Context:** During a critical review of the project, the following misalignments with earlier documentation were identified and corrected:

**What was claimed vs what is true:**

| Claim | Reality |
|---|---|
| "Structured around ISO 8000 / ISO/IEC 25012" | ISO 8000 is for supply-chain master data; not applicable. ISO/IEC 25012 has 15 characteristics; we implement 4. |
| "Consistency checks cross-column rule violations" | Consistency only checks within-column format patterns (e.g. mixed date formats). Cross-column rules ("end_date > start_date") are not automated. |
| "Validity catches impossible domain values (age = -5)" | Validity only catches type mismatches on `object`-dtype columns. Native `int`/`float` columns receive no range validation. |
| "DQ weights derived from standard" | Weights (35/25/25/15) are project-specific heuristics; no standard specifies numeric weights. |

**Thesis framing recommendation:**
In the thesis, frame the project as:
> "Implementing a DAMA-DMBOK-aligned DQ pipeline covering four of six practitioner dimensions (completeness, validity, uniqueness, consistency) with partial implementation depth within each dimension, and validating the approach against a dataset of n files from [domain]."

Avoid claiming ISO 8000 or full ISO/IEC 25012 alignment. Cite ISO/IEC 25012 as a related standard for academic context, not as the operative framework.

**On over-engineering:**
- **MCAR/MAR/MNAR framework:** Borrowed from clinical research (van Buuren 2018). This is academically justified for the thesis and the MAR detection in code (`_detect_mar_columns()`) is correct and generalisable. Accept it, but acknowledge in the thesis that this level of missing-data taxonomy is unusual for business CSV DQ.
- **LLM for MNAR detection:** MNAR is handled via a single prompt instruction: high-null event-related columns (`adv_evt_dt`, `incident_dt`, `resolved_at`) should use `leave_null` instead of `drop_column`. This is not a distinct code path — it is one bullet in `_RUNNER_SYSTEM`. No specific MNAR test failures have been recorded; the reliability concern is theoretical (probabilistic LLM instructions). What *was* tested and failed was MAR detection via the LLM — that was moved to code. MNAR remains as LLM reasoning, untested in isolation.

**On under-engineering:**
- **DQ score weights (invented):** Accept as project-specific heuristics; disclose in the thesis. Future work: let users configure weights for their domain.
- **Validity nearly decorative for numeric data:** This is a known gap (todo item) — validity should check native numeric types for range violations. In the current implementation, validity's 25% weight in the DQ score barely captures real validity problems.
- **Before/after comparison not actionable:** The score comparison shows a number changed, but does not tell the user *which recommendation* had the most impact. This is a UX gap that could be addressed with a per-dimension diff, but is acceptable for a research prototype.
- **No user configurability:** Thresholds (50% null for drop_column, IQR 1.5×, cardinality 10%, 80% MAR correlation) are hardcoded. For a research prototype, this is appropriate. A production tool would expose these as user-configurable parameters.

---

## 40. Formal MAR Detection — scipy Point-Biserial + Chi-Square

**Decision:** Implement MAR detection in `_detect_mar_columns()` using formal scipy statistical tests rather than a simple count-based correlation check.

**Context:** The original design described MAR detection as checking "whether ≥80% of nulls in column A appear when column B = value V". This is a simple heuristic that works on toy examples but fails on real data with multiple correlated values or continuous distributions.

**Implementation:**
- Create a binary missingness indicator: `missing_flag = col.isna().astype(int)`
- For numeric other-columns: **point-biserial correlation** between `missing_flag` and the numeric values (`scipy.stats.pointbiserialr`) — tests whether the numeric values are systematically different when the target column is null
- For categorical other-columns: **chi-square test** of independence (`scipy.stats.chi2_contingency`) on a 2×k contingency table (missing/present vs each category)
- α = 0.05 significance threshold — if p-value < 0.05, MAR is detected, `leave_null` is applied

**Why formal tests over the 80% heuristic:**
- The 80% threshold was calibrated on one example (ATM transactions). With a different dataset, 80% could be too high (misses real MAR) or too low (false positive on coincidental clustering).
- Statistical hypothesis testing with p < 0.05 is a defensible, reproducible threshold with an established meaning in the literature.
- The result is binary and deterministic: either the null pattern is statistically explained by another column or it is not.

**Trade-off:** scipy adds a dependency. Accepted: scipy is a standard scientific Python library already present in most data engineering environments.

---

## 41. Numeric Sentinel Detection — 3×IQR Fence + Absolute Count ≥5

**Decision:** Detect numeric sentinel values using a 3×IQR fence for extremity and an **absolute count ≥ 5** (not a percentage) as the frequency threshold.

**Context:** The initial implementation used `count / n ≥ 5%` as the frequency threshold. This failed silently on large datasets — 34 occurrences of `-999` in a 1,200-row temperature dataset is 2.8%, below the 5% threshold, meaning the sentinels were not detected even though they clearly represented "not recorded" values.

**Why 3×IQR instead of 1.5×IQR (the standard outlier fence):**
The standard 1.5×IQR fence is designed to catch all statistical outliers, including genuine rare events (fraud, sensor spikes). Sentinels are a different category — they are *implausibly* extreme, not just unusual. A temperature column with range 15–35°C and a sentinel of `-999` is qualitatively different from a salary column with a 99th-percentile value of `$450,000`. Using 3×IQR makes the fence stricter, catching only values that are structurally impossible given the column's distribution, not just statistically unusual.

**Why absolute count ≥ 5 instead of a percentage:**
- Percentage threshold silently fails on large datasets (example above: 2.8% of 1,200 rows)
- Sentinel values are typically data entry codes — if `-999` appears once, it might be a genuine measurement error; if it appears 5+ times, it is almost certainly a code
- The absolute threshold of 5 is consistent with the `castable_count ≥ 5` floor used in format inconsistency detection — same principle: below 5 occurrences there is not enough signal to make a deterministic claim
- The same threshold is used in tests: test datasets use 45 normal + 5 sentinel values (5/50 = 10%), keeping sentinels rare enough not to distort Q1/Q3

**Known limitation:** If sentinel values are >~20% of the column, they pull Q1 (or Q3) into the sentinel range, widening the 3×IQR fence until the sentinels fall within it and are no longer detected. This is an inherent limitation of IQR-based methods when the "contamination" rate is high. Addressed in documentation; a future improvement would use a robust estimator (e.g. median absolute deviation) for the fence.

---

## 42. Z-Score vs Min-Max Normalization — Selected by Outlier Presence

**Decision:** The `normalize` field in recommendations is a 3-value choice (`"min_max"`, `"z_score"`, `false`) rather than a boolean. The pipeline automatically selects the method based on the column's outlier profile.

**Context:** The original implementation used `normalize: true/false` and always applied min-max. The boolean was replaced by a string literal after research into the practical difference between the two methods in the presence of outliers.

**Selection logic:**
```python
normalize = "z_score" if has_outliers else "min_max"
```
where `has_outliers = outlier_info.get("count", 0) > 0`.

**Rationale:**
- **Min-max** compresses all values to [0, 1] using `(x − min) / (max − min)`. If the column has outliers, `min` and `max` are the outlier values — every non-outlier value is squashed into a tiny range near 0.5. The scaling is dominated by the extremes and loses all resolution for the bulk of the data.
- **Z-score** uses `(x − mean) / std`. Outliers inflate std, which compresses the distribution less dramatically than min-max. The result is a meaningful spread around 0 for the bulk of the data.

**Z-score implementation:** Uses `ddof=0` (population std, not sample std). This is consistent with scikit-learn's `StandardScaler` default and produces `std=1` rather than a slightly inflated value.

**User override:** The user can change `normalize` in the reviewed JSON — `false` to skip, or swap between `"min_max"` and `"z_score"` based on their use case.

---

## 43. Lean Baseline for LLM — Strip Default Fields Before Sending

**Decision:** `_lean_baseline()` strips all default-value fields from the recommendations JSON before sending it to the LLM. Only non-default information is transmitted.

**Context:** The full recommendations JSON includes many fields that are identical for most columns: `nullable: false`, `missing_values: null`, `normalize: false`, `warnings: []`, `rename_to: null`, `note: null`. Sending these to the LLM adds token cost and prompt noise without providing any information the LLM can act on.

**What is stripped (when at default value):**
- `nullable: false` — omitted; LLM should not change nullability
- `missing_values: null` — omitted; no fill needed, LLM has nothing to do here
- `normalize: false` — omitted; normalization is a code decision
- `warnings: []` — omitted; empty list carries no information
- `rename_to: null` — omitted; LLM adds this only when it has a suggestion
- `note: null` — omitted; LLM adds notes only when it has something to say

**What is always kept:**
- `type` — LLM's only signal about the column's data type
- Non-default `nullable: true` — signals nullability relevant for MNAR reasoning
- Non-default `missing_values` — shows what strategy was chosen; LLM may override (MNAR)
- Non-default `normalize` — shows normalization was suggested; LLM may note it
- Non-default `warnings` — shows existing warnings; LLM should not override these

**Format:** `json.dumps(lean_baseline, separators=(",", ":"))` — compact JSON (no spaces, no newline indentation). The diff-only format combined with compact JSON reduces the baseline block from ~600 tokens to ~200 tokens for a typical 10-column dataset.

**System prompt update:** A paragraph was added explaining the sparse format to the LLM: "Fields absent from a column dict are at their default value — `nullable: false`, `missing_values: null`, `normalize: false`. Do not echo default fields back; only include fields you are changing."

---

## 44. Format Inconsistency Detection — Partial Castability, Not Regex Patterns

**Decision:** Detect format inconsistency in string columns by checking how many values partially cast to numeric (`pd.to_numeric(errors="coerce")`), not by matching regex patterns for specific format types.

**Context:** The initial design considered regex-based format pattern detection: identify `currency`, `unit_value`, `percentage`, `integer`, `float` patterns and flag columns that mix patterns. After analysis, this approach was rejected:
- `unit_value` would match `"7km"`, `"7_000m"`, `"7 km"` and arbitrary suffixes like `"7th"` — far too broad
- `currency` would only catch USD `$` symbols — missing EUR `€`, GBP `£`, any written currencies
- A column with a mix of `integer` and `float` (e.g. `"100"` and `"99.5"`) would be flagged as inconsistent when both are valid numeric representations

**Why partial castability instead:**
The `pd.to_numeric(errors="coerce")` approach is the exact same function used in `apply_recommendations()` for the cast step. So the detection and the application use identical logic — if the profiler detects partial castability, the cast step will encounter the same problem. This is ground truth, not an approximation.

**Detection threshold:**
- `castable_count ≥ 5` — same absolute-count floor as sentinel detection; below 5 occurrences there is not enough signal
- `castable_pct < 80%` — the same threshold used in `_detect_column_type()` for deciding whether a column is numeric. Below 80% numeric the column is already typed as `string`; partial castability in the 5–79% range means the column is ambiguous

**Purpose — feeds custom transforms:** Format inconsistency is detected primarily to give the LLM (future custom transform generation) the signal it needs. The `warnings` field surfaces the sample of non-castable values (e.g. `"$1,200"`, `"N/A"`) so the custom transform prompt can propose the correct normalisation.

---

## 45. Type Cast Guard — Skip Unsafe Casts, Don't Silently Corrupt

**Decision:** Before applying any `int` or `float` cast in `apply_recommendations()`, `_would_cast_safely()` simulates the cast and skips it if >5% of non-null values would become `NaN`.

**Context:** An earlier implementation naively applied `pd.to_numeric(errors="coerce")` to any column typed as `int` or `float`. If the column had many string sentinels or mixed formats (e.g. currency symbols, percentage signs), the cast would silently replace a significant fraction of values with `NaN` — worse than the original data because the corruption was invisible.

**Mechanism:**
```python
def _would_cast_safely(series, target_type, threshold=0.05):
    non_null = series.dropna()
    converted = pd.to_numeric(non_null, errors="coerce")
    new_null_rate = converted.isna().sum() / len(non_null)
    return bool(new_null_rate <= threshold)
```

**Why 5%:**
- The type detection threshold uses 80% castable to classify a column as numeric. A column with 80% numeric values and 20% non-castable values would have `_detect_column_type()` → `numeric` but `_would_cast_safely()` → False (20% > 5%). This is correct: the column needs a custom transform before a cast is safe.
- 5% aligns with the acceptable invalid-cell rate: a column with 3 bad values in 100 rows (3%) is a minor data quality issue; a column with 20 bad values in 100 rows (20%) is a format problem that requires explicit treatment.
- The same 5% is used in `_profile_numeric_column()` as the boundary for reporting `invalid_count`.

**Effect:** The cast is skipped and a `WARNING` is logged identifying the column and the % that would be lost. The column stays as `object` dtype. The format inconsistency detection (Decision 44) will have already added a warning recommending a custom transform — this guard is the enforcement layer that prevents the problem from silently occurring regardless.

---

## 46. Frontend Stack — React + Vite + TypeScript + Tailwind CSS v4

**Decision:** React 19 + Vite 7 + TypeScript + Tailwind CSS v4 for the frontend. TanStack Query v5 for server state, React Router v6 for routing.

**Context:** The frontend needed to be a modern SPA that could interact with the FastAPI backend, render real-time status updates via SSE, and display complex recommendation JSON in an editable form.

**Options considered:**

| Option | Pros | Cons |
|--------|------|------|
| Next.js | SSR, file-based routing | Overkill for a single-user internal tool; SSR adds complexity with JWT auth |
| Plain React + CRA | Simple | CRA is deprecated; slow build |
| **React + Vite** | Fast HMR, modern, widely used | None significant |

**Why Tailwind v4:** The project was started with Tailwind CSS v4 (latest at time of writing). v4 changed the PostCSS plugin (`@tailwindcss/postcss` instead of `tailwindcss`), the CSS import syntax (`@import "tailwindcss"` instead of `@tailwind` directives), and dark mode variant definition (`@custom-variant dark`). Documented here to avoid confusion with v3 tutorials.

**Node version constraint:** Vite 7 requires Node 20+. The local environment runs Node 16. All production builds run inside Docker (`node:20-alpine`) — local TypeScript type-checking passes but `npm run build` must be run via Docker.

---

## 47. Sidebar Layout — Run ID as Primary Identifier

**Decision:** The sidebar lists runs (not files) as the primary navigation unit, identified by truncated run ID (`#abc12345`), with the filename as a subheading.

**Context:** A dataset can be uploaded and processed multiple times. Using the filename as the primary identifier creates ambiguity — the user cannot distinguish two runs of the same file. Run ID is unique and stable.

**Layout:** Claude/ChatGPT-style dark sidebar (`bg-zinc-900`) with grouped runs (Today / Yesterday / Last 7 days / Older), a status dot per run, and "● live" indicator for the active SSE run. The main content area is light (`bg-gray-50`) to create clear visual separation.

**Pagination:** Sidebar loads files in pages of 20 (`fileLimit` state). "Load more" button increments `fileLimit`, which is part of the React Query key, triggering a fresh cumulative fetch. `hasMore = files.length === fileLimit` detects whether more pages exist without a separate count query.

---

## 48. SSE Authentication — JWT via Query Parameter

**Decision:** The SSE endpoint (`GET /api/files/{file_id}/events`) accepts JWT via `?token=` query parameter, not the standard `Authorization: Bearer` header.

**Context:** The browser's native `EventSource` API does not support setting custom headers. The only way to pass authentication is via URL query parameters or cookies. Cookies would require CORS credential configuration. A query parameter is simpler and consistent with how other SSE implementations handle this constraint.

**Security note:** JWTs in query parameters can appear in server access logs. Acceptable in a development/research prototype context. In production, the preferred alternative would be a short-lived SSE token issued by a dedicated endpoint (`POST /api/sse-token`) exchanged for the real JWT before opening the EventSource connection.

**Backend implementation:** The SSE endpoint decodes the JWT manually using the same secret and algorithm as the standard OAuth2 dependency, then verifies file ownership before streaming.

---

## 49. SSE Reconnect Prevention After Terminal State

**Decision:** The `useRunEvents` hook uses a `doneRef` (React ref) to permanently close the `EventSource` after receiving a terminal status (`COMPLETED` or `FAILED`), preventing the browser's automatic reconnect behaviour.

**Context:** `EventSource` automatically reconnects when the server closes the connection. The backend closes the SSE stream after emitting a terminal status. Without a guard, the browser reconnects immediately, opening a new subscription, triggering another `invalidateQueries` call, and creating an infinite loop of re-fetches.

**Mechanism:**
```typescript
const doneRef = useRef(false)
source.onmessage = (e) => {
  const data = JSON.parse(e.data)
  onUpdate(data)
  if (TERMINAL.has(data.status)) {
    doneRef.current = true
    source.close()
  }
}
source.onerror = () => { if (doneRef.current) source.close() }
```

`doneRef` is a ref (not state) because updating it must not trigger a re-render, and its value must persist across renders without causing the `useEffect` cleanup to run again.

---

## 50. Active Run State — sessionStorage Persistence

**Decision:** The active run state (`liveRun`, `selectedRunId`, `selectedFileId`) is stored in `sessionStorage`, not `useState` alone or `localStorage`.

**Context:** If the user refreshes the page during an active run, pure `useState` would lose the context and the SSE connection — they would see a blank main panel and miss the rest of the run. `localStorage` would persist across browser sessions, which is undesirable for transient UI state.

**sessionStorage** survives page refresh but not a new tab or browser close. This matches user expectation: refreshing during a run keeps context; opening the tool fresh shows a clean state.

---

## 51. Recommendation Diff — Client-Side Pure Function

**Decision:** The diff between generated and applied recommendations is computed entirely on the client as a pure function `computeDiff(generated, applied)`, not precomputed on the backend.

**Context:** Both recommendation objects are already present in the `RunOut` response. Computing the diff server-side would require a new endpoint, additional storage, and re-computation whenever the comparison is viewed. The client already has all the data.

**What is diffed:** Per-column fields (`type`, `fill`, `normalize`, `rename_to`, `nullable`, `transform_hint`, `transform_code`), top-level `duplicates` config, and per-column outlier `strategy`. Bounds (`lower`, `upper`) are excluded — they are system-set and cannot be changed by the user.

**Display:** Three tabs in `RecsViewer` — Generated | Applied | Diff. The Diff tab shows a change count badge and field-level diffs with red strikethrough for old values and green for new values. "No changes" message when applied === generated.

---

## 52. Dark Mode — Tailwind v4 Class Strategy with localStorage Persistence

**Decision:** Dark mode uses Tailwind CSS v4's `@custom-variant dark` with a `.dark` class on `<html>`, toggled by a `useTheme` hook that persists the preference in `localStorage`.

**Context:** Tailwind v4 changed dark mode configuration. The v3 approach (`darkMode: "class"` in `tailwind.config.js`) no longer applies. In v4, class-based dark mode requires:
```css
@custom-variant dark (&:where(.dark, .dark *));
```
This makes all `dark:` utilities apply when `.dark` is on any ancestor element.

**Why class strategy over `prefers-color-scheme`:** The system media query approach (`prefers-color-scheme: dark`) cannot be overridden by the user within the app. A class-based toggle gives explicit user control, which is the expected behaviour for a tool with a persistent preference.

**Scrollbar theming:** Native browser scrollbars are not affected by Tailwind's `dark:` variants. Custom scrollbar styles are applied via `::-webkit-scrollbar` pseudo-elements and Firefox's `scrollbar-color` property using `.dark` class selectors directly in `index.css`.

---

## 53. Pydantic Silent Field Stripping — transform_hint and transform_code

**Decision:** `transform_hint: str | None = None` and `transform_code: str | None = None` were added explicitly to the `ColumnConfig` Pydantic schema in `backend/app/schemas/file.py`.

**Context:** These fields exist in `recommendations_generated` (added by the LLM enrichment step) but were missing from the Pydantic schema. When the user submitted approved recommendations via `PUT /recommendations`, Pydantic silently dropped any unknown fields before saving to the database. As a result, `recommendations_approved` never contained `transform_hint` or `transform_code`, breaking the transform flow for custom transforms.

**Root cause:** Pydantic v2 defaults to `model_config = ConfigDict(extra="ignore")` — unknown fields are silently discarded, not rejected. This is the correct behaviour for API input validation but creates a footgun when the schema is incomplete.

**Fix:** Explicitly declare all fields that flow through the recommendations JSON in `ColumnConfig`. Similarly, `count: int | None = None` was added to `OutliersConfig` which was also being silently stripped.

**Lesson:** Any field that must survive a round-trip through a Pydantic model must be declared in that model, even if it is only set by internal processes and never validated for user input.

---

## 54. After-Transform Issue Counts — Strategy-Based Outlier Counting

**Decision:** After-transform outlier counts are derived from approved strategies, not from re-profiling the cleaned data.

**Context:** The transform flow re-profiles the cleaned DataFrame to compute `dq_scores_after`. Initially, `count_issues_from_profile()` re-detected outliers from this post-transform profile using IQR. This produced incorrect results — specifically, more outliers after transform than before — for datasets like medical lab data.

**Why re-profiling outliers is invalid:** IQR bounds are computed from the data distribution. After transform, the distribution changes (nulls filled, rows dropped, types cast), so new IQR bounds are computed on a different population. Tighter bounds flag more values as outliers even though the data quality improved. The before/after counts are not comparable because the ruler changes.

**Fix:** `count_issues_from_profile()` accepts an optional `approved_outliers` dict (from `recommendations["outliers"]`). When provided, after-outlier count is computed from strategies: `keep` → retain original count, `winsorise`/`remove`/`cap` → 0. This is logically consistent with what the transform actually did and uses the original IQR bounds implicitly.

**Other issue types (missing, duplicates, type_mismatches, sentinels, format inconsistencies):** Re-profiling is valid for these because their counts are absolute (null count, duplicate row count, etc.) and not dependent on distribution-sensitive bounds.

---

## 55. Per-Run File Logging — Named Logger + Shared File Handler

**Decision:** Both `dq_flow.py` and `transform_flow.py` write per-run file logs to `/logs/<flow>_<filename>_<timestamp>.log`, using a named logger keyed on `run_id` and a shared `FileHandler` that is also attached to the `flows.dq_logic` and `flows.llm_enrichment` module loggers.

**Context:** Prefect's built-in `get_run_logger()` streams logs to the Prefect UI and stdout. These logs disappear when the container restarts and are not accessible if the Prefect server is down. Debugging transform_hint failures and LLM call behaviour required persistent, per-run log files that survive restarts.

**Why named loggers (`dq_run.<run_id>` / `transform_run.<run_id>`):**
Named loggers allow any task in the same Python process to get the same logger instance by name — `logging.getLogger(f"dq_run.{run_id}")` — without passing the logger object through all task arguments. Since all tasks in a Prefect flow run in the same worker process, the named logger is always available.

**Why shared FileHandler with dq_logic and llm_enrichment:**
`dq_logic.py` and `llm_enrichment.py` use `logger = logging.getLogger(__name__)` (module-level). At flow startup, `_setup_file_logger()` attaches the same `FileHandler` to these module loggers. This means all log output from profiling, scoring, LLM calls, and transform code generation lands in the same file as the flow orchestration logs, creating a complete single-file trace of the run.

**Log location:** `/logs/` mounted as `./logs:/logs` in `docker-compose.yaml`. Files are visible on the host at `./logs/` immediately, with no need to exec into the container.

**Log format:** `%(asctime)s  %(levelname)-8s  %(message)s` with `datefmt="%Y-%m-%dT%H:%M:%S"` — ISO 8601 timestamp so log files can be sorted chronologically and correlated across services.

**Prefect UI vs file logs:** Both are written — Prefect UI gets `get_run_logger()` output (orchestration-level INFO), file gets DEBUG-level output including per-column profiling details, LLM raw responses, and transform code results.

---

## 56. LLM Call Audit Logging — Timing and Token Counts

**Decision:** Every LLM API call in `llm_enrichment.py` logs: the request (model, max_tokens, attempt number, prompt character count), the response (elapsed seconds, prompt tokens, completion tokens), and the full raw LLM output text.

**Context:** LLM calls are the least deterministic and most expensive part of the pipeline. Without explicit logging, debugging failures — bad JSON, validator rejections, wrong recommendations — required re-running the entire flow. With call/response logging, the exact input and output of every attempt is available in the run log file.

**What is logged per call:**
```
LLM enrichment request | model=llama-3.3-70b-versatile max_tokens=512 attempt=1 prompt_chars=1823
LLM enrichment response | attempt=1 elapsed=0.84s tokens_in=412 tokens_out=187
LLM enrichment raw output attempt=1:
{"columns": {"salary": {"missing_values": ...}}}
```

**Why raw output, not just the parsed result:**
The raw output captures the LLM's actual response before fence stripping and JSON parsing. If the LLM outputs malformed JSON or adds unexpected markdown, the raw log shows exactly what was received — the parsed result would simply be absent (exception raised).

**Token counts from `response.usage`:** `prompt_tokens` and `completion_tokens` are available in Groq's response object. Logging both makes it possible to track token usage per run, estimate costs, and spot prompt bloat if counts grow unexpectedly.

**Same pattern applied to transform code generation:** Each column-level LLM call for `generate_transform_code()` logs the hint text, the generated lambda, timing, and tokens. This was the specific failure mode that motivated adding file logging — a user-added transform hint was silently skipped, and without per-call logging it was impossible to tell whether the LLM call was made, what it returned, or why the code was not stored.

---

## 57. generate_missing_transform_codes — LLM 2 in Transform Flow

**Decision:** The transform flow calls a new `generate_missing_transform_codes` task between `load_approved_recommendations` and `apply_transform`. This task runs LLM 2 (`generate_transform_code`) for any column that has a `transform_hint` but no `transform_code`.

**Context:** The original design assumed `transform_code` was always generated during the DQ flow (after LLM enrichment) and stored in `recommendations_generated`. When the user submitted approved recommendations, `transform_code` was already present. The transform flow could skip LLM 2 entirely.

**The gap:** Users can manually add `transform_hint` values in the AWAITING_REVIEW UI — the `transform_hint` textarea is editable per column, and any column can have a hint added. When the user submits, `recommendations_approved` contains the hint but no `transform_code` because LLM 2 was never called for the manually-added hint. `apply_recommendations()` step 0b executes `transform_code`, not `transform_hint` directly — so the hint was silently ignored.

**Fix:**
1. `generate_missing_transform_codes` scans `recommendations["columns"]` for entries with `transform_hint` but no `transform_code`.
2. If any are found, it calls `generate_transform_code()` (the same LLM 2 function used in the DQ flow) on the full recommendations dict.
3. The updated recommendations (with newly generated codes) are passed to `apply_transform`.
4. A safety warning was also added to `dq_logic.py` step 0b: if a column has `transform_hint` but no `transform_code` at apply time, a WARNING is logged so the skip is always visible.

**Why not call LLM 2 in the API layer on submission:** The API layer should not run long-running LLM calls synchronously — it would block the HTTP response. The transform flow already runs asynchronously, making it the right place for this work.

**Soft failure contract preserved:** If LLM 2 fails for a column, that column is skipped (no code generated) and a WARNING is logged. The transform still completes — a missing transform_code means the column data is left unchanged, which is better than a failed run.

---

## 58. Download Button Placement — RunHeader, Not COMPLETED Card

**Decision:** The "Download CSV" button lives in `RunHeader` (always visible at the top of the run detail view when `status === "COMPLETED"`) rather than inside the COMPLETED state card in the main content area.

**Context:** The original implementation placed the download button inside the COMPLETED card alongside the score comparison. This required the user to scroll past the score card and issue grid to reach the download action — the most important action after a completed run.

**Why RunHeader:**
- The header is always visible regardless of scroll position
- It is the natural location for primary actions in a document/record view (consistent with most admin UIs)
- The header already shows the run status, so placing the download button there creates a visual `status=COMPLETED → download available` connection
- The COMPLETED card can now be entirely informational (scores + issues) without an action mixed in

**Implementation:** `RunDetailView` passes `onDownload={status === "COMPLETED" ? handleDownload : undefined}` to `RunHeader`. The header shows the button only when `status === "COMPLETED" && onDownload` — no button for other statuses.

---

## 59. AWAITING_REVIEW UI — Issues Bar, Rename Checkboxes, Expandable Column Rows

**Decision:** The AWAITING_REVIEW page was redesigned with three specific improvements: (1) an issues summary banner showing counts for all 6 issue types, (2) rename management via per-column checkbox + "Clear all renames" bulk action, (3) warnings/notes/transform hints displayed via icons with an expandable inline detail row per column.

**Context:** The original AWAITING_REVIEW page showed only the overall DQ score as a plain number. Users had no visual summary of what was wrong with their data before reviewing 30+ column rows. Rename suggestions (from LLM) had no easy way to accept or reject them in bulk. Warnings, LLM notes, and transform hints were crammed into a single "Warnings/Notes" column that showed only a truncated value.

**Issues banner:** All 6 issue types (`missing`, `duplicates`, `type_mismatches`, `sentinel_values`, `outliers`, `format_inconsistencies`) are shown. Zero-count types are greyed out rather than hidden — showing "0 duplicates" is informative (it confirms the check ran and found nothing). Values come from `_metadata.issues_found` which is already in the loaded recommendations.

**Rename checkboxes:** The `rename_to` field now has a paired checkbox (`isRenameActive = config.rename_to != null`). Unchecking sets `rename_to: null`, disabling the rename without losing the text in the input (backed by a `useRef` so the value survives unchecking). "Clear all renames" appears above the table only when any renames are active. This was specifically needed because the LLM aggressively renames abbreviated columns — users often want to accept some renames and reject others quickly.

**Expandable rows:** Each column row has an expand toggle (chevron). The expanded detail row (`<tr colSpan={6}`) shows: full warning list (amber), LLM note (grey, read-only), and a transform hint textarea (always shown, editable). Info icons in the summary row indicate at a glance whether warnings/note/hint are present. The expand pattern uses React Fragment to return two `<tr>` elements from a single component — this is the correct approach for table rows that need to expand inline.

**Transform hint editing:** Users can add, edit, or discard transform hints for any column. Discarding sets `transform_hint: null`. When the user submits, any column with a manually-added or edited hint gets `generate_transform_code()` called in the transform flow (Decision #57). The `×` discard button only appears when a hint is present, keeping the UI clean for columns without hints.

---

## 60. MinIO Object Lifecycle Policies — Raw and Curated Buckets

**Context:** MinIO accumulates uploaded CSVs (raw bucket) and cleaned outputs (curated bucket) indefinitely. For a development and demo environment this creates unbounded storage growth with no benefit — users always have their original files locally, and cleaned outputs are downloaded immediately after processing.

**Options considered:**
1. Manual cleanup — delete files periodically by hand
2. Application-level deletion — have the backend delete MinIO objects after a run completes or after a download
3. Bucket-level ILM (lifecycle) rules — MinIO handles expiry automatically, no application code required

**Why ILM rules:** Option 3 requires zero application changes, is enforced at the storage layer regardless of application behaviour, and is the standard S3-compatible approach. Option 2 risks data loss on application bugs and couples storage lifecycle to business logic. Option 1 is not sustainable.

**Retention periods chosen:**
- `raw`: 7 days — once the DQ flow runs, the original CSV is no longer needed by the pipeline. Users retain their source file locally.
- `curated`: 14 days — cleaned outputs should remain available long enough for the user to download and use them after processing completes.

**Versioning and delete markers:** Versioning is not enabled on either bucket. `EXPIRE DELETEMARKER` is `false` and irrelevant — objects are permanently deleted after their expiry days with no tombstones.

**Applied via `mc` CLI** (rules survive container restarts via the `minio-data` volume, but must be reapplied if the volume is wiped):

```bash
docker exec minio mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec minio mc ilm rule add --expiry-days 7  local/raw
docker exec minio mc ilm rule add --expiry-days 14 local/curated
```

**Active rules:**

`raw` bucket:
```
┌───────────────────────────────────────────────────────────────────────────────────────┐
│ Expiration for latest version (Expiration)                                            │
├──────────────────────┬─────────┬────────┬──────┬────────────────┬─────────────────────┤
│ ID                   │ STATUS  │ PREFIX │ TAGS │ DAYS TO EXPIRE │ EXPIRE DELETEMARKER │
├──────────────────────┼─────────┼────────┼──────┼────────────────┼─────────────────────┤
│ d6qb7t9roltdr612lsv0 │ Enabled │ -      │ -    │              7 │ false               │
└──────────────────────┴─────────┴────────┴──────┴────────────────┴─────────────────────┘
```

`curated` bucket:
```
┌───────────────────────────────────────────────────────────────────────────────────────┐
│ Expiration for latest version (Expiration)                                            │
├──────────────────────┬─────────┬────────┬──────┬────────────────┬─────────────────────┤
│ ID                   │ STATUS  │ PREFIX │ TAGS │ DAYS TO EXPIRE │ EXPIRE DELETEMARKER │
├──────────────────────┼─────────┼────────┼──────┼────────────────┼─────────────────────┤
│ d6qb7t9roltdrbuu62a0 │ Enabled │ -      │ -    │             14 │ false               │
└──────────────────────┴─────────┴────────┴──────┴────────────────┴─────────────────────┘
```

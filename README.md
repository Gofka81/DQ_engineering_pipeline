# Data Quality Engineering Pipeline

Automated data quality analysis and transformation system. Upload a CSV, get scored recommendations, review and edit them, apply transformations, download the cleaned file.

**Goal:** Reduce manual work for data engineers and make data quality analysis accessible, reproducible, and auditable.

---

## Features

- **Automated DQ Analysis** — Upload CSV → profile data → get completeness, uniqueness, validity, and consistency scores (5 scores: overall + 4 dimensions)
- **Smart Recommendations** — Deterministic rule-based suggestions aligned with DAMA DMBOK dimensions: missing value strategies (MCAR/MAR/MNAR-aware), duplicate removal, type casting, normalization, and outlier detection
- **LLM Enrichment** — Groq (Llama 3.3 70B) enriches recommendations with semantic reasoning: MNAR leave_null detection, column rename suggestions, and plain-English notes per column
- **Interactive Review** — Review and edit recommendations JSON via API before applying any changes
- **Transform Pipeline** — Apply approved transformations, recalculate DQ scores, save cleaned file to curated storage
- **Before / After Comparison** — Every run stores DQ scores before and after transform for full auditability
- **User Isolation** — Secure multi-tenant system with JWT authentication; each user sees only their own files
- **Async Architecture** — FastAPI + Prefect 3 + Redis for scalable, non-blocking processing

---

## Architecture

```
User uploads CSV
      │
      ▼
┌─────────────────────────────────────────┐
│            FastAPI Backend              │
│  JWT Auth · File Upload · Status Poll   │
└────┬──────────────────────────┬─────────┘
     │ Save to MinIO (raw)      │ Save metadata
     │ Push job → Redis         │       │
     ▼                          ▼       ▼
┌──────────┐            ┌───────────────────┐
│  MinIO   │            │    PostgreSQL      │
│  • raw   │            │  users / files /  │
│  • curated│           │  runs (JSONB DQ   │
└──────────┘            │  scores + recs)   │
                        └───────────────────┘
     │
     ▼
┌─────────────────────────────────────────┐
│          prefect-worker container       │
│                                         │
│  ┌────────────────┐                     │
│  │ redis_worker   │  BRPOP on "jobs"    │
│  │ (background)   │  routes by job_type │
│  └────┬───────────┘                     │
│       │ run_deployment()                │
│       ▼                                 │
│  ┌────────────────┐  ┌───────────────┐  │
│  │  DQ Analysis   │  │   Transform   │  │
│  │  Flow          │  │   Flow        │  │
│  │  • profile     │  │  • load recs  │  │
│  │  • score (5)   │  │  • apply recs │  │
│  │  • recommend   │  │  • score (5)  │  │
│  └────────────────┘  │  • upload CSV │  │
│                      └───────────────┘  │
└─────────────────────────────────────────┘
     │
     ▼
User reviews recommendations (AWAITING_REVIEW)
     │ PUT /recommendations
     ▼
Transform triggered → status COMPLETED
     │
     ▼
Presigned download URL for cleaned CSV
```

---

## Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **API** | FastAPI | REST endpoints, async request handling |
| **Auth** | JWT + Argon2 | Secure authentication and password hashing |
| **Orchestration** | Prefect 3 | DQ Analysis and Transform workflow automation |
| **Queue** | Redis | Job queue (LPUSH / BRPOP pattern) |
| **Database** | PostgreSQL 16 | Metadata: users, files, runs, JSONB DQ scores |
| **Object Storage** | MinIO | S3-compatible CSV storage (raw / curated buckets) |
| **AI** | Groq + Llama 3.3 70B | LLM enrichment — MNAR leave_null, rename suggestions, notes |

---

## Prerequisites

- **Docker** and **Docker Compose**
- **Python 3.11+** (for running the backend locally)
- **curl** or any HTTP client for API testing

---

## Setup Instructions

### Step 1: Clone the Repository

```bash
git clone <repository-url>
cd DQ_engineering_pipeline
```

### Step 2: Configure Environment Variables

```bash
cp .env.example .env
```

Edit `.env` with your values:

```bash
# PostgreSQL
POSTGRES_USER=prefect
POSTGRES_PASSWORD=prefect
POSTGRES_DB=prefect
POSTGRES_HOST=localhost
BACKEND_DB=backend

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_ENDPOINT=localhost:9000

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_JOBS_QUEUE=jobs

# JWT
SECRET_KEY=your-super-secret-key-change-this
ACCESS_TOKEN_EXPIRE_MINUTES=1440
ENVIRONMENT=development

# File Upload
MAX_FILE_SIZE_MB=200

# Groq API (LLM enrichment — optional, falls back to baseline if unset)
# LLM_API_KEY=your-groq-key-here
```

> Generate a strong secret key: `python -c "import secrets; print(secrets.token_urlsafe(32))"`

### Step 3: Start Infrastructure

```bash
docker-compose up -d postgres-prefect redis minio
```

### Step 4: Initialise the Database

The PostgreSQL container auto-creates the `prefect` database. The `backend` database must be created manually:

```bash
# Create backend database
docker exec -it postgres psql -U prefect -d prefect -c "CREATE DATABASE backend;"

# Apply schema (users, files, runs tables + run_status enum)
docker exec -i postgres psql -U prefect -d backend < init.sql
```

Verify:

```bash
docker exec postgres psql -U prefect -d backend -c "\dt"
# Expected: users, files, runs
```

### Step 5: Start Prefect Server

```bash
docker-compose up -d prefect-server
```

Wait ~30 seconds, then verify at http://localhost:4200.

### Step 6: Build and Start Prefect Worker

```bash
docker-compose up -d --build prefect-worker
```

This container automatically:
1. Runs `prefect deploy --all` — registers both `DQ Analysis/dq-analysis` and `Transform/transform` deployments
2. Starts `redis_worker.py` as a background process (BRPOP on `jobs` queue)
3. Starts the Prefect worker process (executes flow runs)

Verify both deployments registered:

```bash
docker logs prefect-worker | grep "successfully created"
# Deployment 'DQ Analysis/dq-analysis' successfully created...
# Deployment 'Transform/transform' successfully created...
```

### Step 7: Start the FastAPI Backend

```bash
cd backend
python -m venv venv
source venv/bin/activate       # Windows: venv\Scripts\activate
pip install -r requirements.txt
python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

API docs available at http://localhost:8000/docs.

---

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| FastAPI docs | http://localhost:8000/docs | — |
| Prefect UI | http://localhost:4200 | — |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| PostgreSQL | localhost:5432 | prefect / prefect |
| Redis | localhost:6379 | — |

---

## Quick Start: End-to-End Test

### 1. Register

```bash
curl -X POST http://localhost:8000/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username": "testuser", "email": "test@example.com", "password": "password123"}'
```

### 2. Login

```bash
curl -X POST http://localhost:8000/api/auth/login \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=testuser&password=password123"
```

Save the `access_token` from the response.

### 3. Upload a CSV

```bash
TOKEN="your-token-here"

curl -X POST http://localhost:8000/api/files/upload \
  -H "Authorization: Bearer $TOKEN" \
  -F "file=@your_data.csv"
```

Response:
```json
{
  "id": "uuid-file-id",
  "run_id": "uuid-run-id",
  "status": "PENDING"
}
```

### 4. Poll Status

```bash
FILE_ID="uuid-file-id"

curl http://localhost:8000/api/files/$FILE_ID/status \
  -H "Authorization: Bearer $TOKEN"
```

Status progression: `PENDING → ANALYZING → AWAITING_REVIEW → TRANSFORMING → COMPLETED`

When `AWAITING_REVIEW`, response includes all 5 DQ scores:
```json
{
  "status": "AWAITING_REVIEW",
  "dq_scores_before": {
    "overall": 97.77,
    "completeness": 96.29,
    "uniqueness": 100.0,
    "validity": 96.29,
    "consistency": 100.0
  },
  "dq_scores_after": null
}
```

### 5. Get Recommendations

```bash
curl http://localhost:8000/api/files/$FILE_ID/recommendations \
  -H "Authorization: Bearer $TOKEN"
```

Response:
```json
{
  "recommendations": {
    "columns": {
      "salary": {
        "type": "float", "nullable": true,
        "missing_values": {"strategy": "median", "value": 72500.0},
        "normalize": true, "warning": null,
        "note": "Right-skewed distribution — median is more robust than mean here."
      },
      "age": {
        "type": "int", "nullable": false,
        "missing_values": null,
        "normalize": false, "warning": null, "note": null
      }
    },
    "duplicates": {"strategy": "drop", "subset": [], "keep": "first"},
    "custom_transforms": [],
    "_metadata": {
      "dq_score": 97.77,
      "generated_at": "2026-03-01T...",
      "issues_found": {"missing": 26, "duplicates": 0, "type_mismatches": 0}
    }
  }
}
```

### 6. Approve (and optionally edit) Recommendations

```bash
curl -X PUT http://localhost:8000/api/files/$FILE_ID/recommendations \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"recommendations": { ...edited json... }}'
```

This triggers the Transform flow immediately. Status moves to `TRANSFORMING`.

### 7. Download Cleaned File

When status is `COMPLETED`:

```bash
RUN_ID="uuid-run-id"

curl http://localhost:8000/api/runs/$RUN_ID/download \
  -H "Authorization: Bearer $TOKEN"
```

Response:
```json
{
  "download_url": "http://localhost:9000/curated/file-id/run-id/cleaned.csv?...",
  "expires_in_hours": 1
}
```

Use the presigned URL to download the cleaned CSV directly.

---

## DQ Score Formula

Five scores are stored per run (before and after transform):

| Dimension | Weight | Formula |
|-----------|--------|---------|
| **Completeness** | 35% | `(1 - missing_cells / total_cells) × 100` |
| **Uniqueness** | 25% | `(1 - duplicate_rows / total_rows) × 100` |
| **Validity** | 25% | `(type_conforming_cells / total_cells) × 100` |
| **Consistency** | 15% | `(pattern_matching_cells / total_cells) × 100` |
| **Overall** | — | Weighted composite of the four above |

Validity checks object-typed columns for type conformance (e.g. `"N/A"` in a numeric column = invalid).
Consistency checks string columns against known patterns (email, phone, URL, ISO date, UUID, UK postcode).

---

## Recommendations Schema

Column-centric format — all decisions for a column live in one place:

```json
{
  "columns": {
    "<col>": {
      "type":           "int|float|string|date|bool",
      "nullable":       true,
      "missing_values": {"strategy": "median|mean|mode|fill|drop_row|drop_column|leave_null", "value": null},
      "normalize":      false,
      "warning":        "human-readable impact message or null",
      "note":           "LLM explanation or null",
      "rename_to":      "new_name or null"
    }
  },
  "duplicates": {"strategy": "drop", "subset": [], "keep": "first|last"},
  "custom_transforms": [],
  "_metadata": {
    "generated_at": "ISO timestamp",
    "dq_score": 97.77,
    "issues_found": {"missing": 0, "duplicates": 0, "type_mismatches": 0}
  }
}
```

**Fill strategies:**
- `median` — skewed numeric columns (outliers present; robust to extreme values)
- `mean` — symmetric numeric columns (no significant outliers)
- `mode` — bool and low-cardinality string columns (likely categorical)
- `drop_row` — ID/key columns, pattern columns (email/phone/UUID), date columns
- `drop_column` — columns ≥50% missing with no structural null pattern
- `leave_null` — intentional nulls (MAR: correlated with another column; MNAR: domain-sparse e.g. event dates)
- `fill` — literal value from the `value` field (user-defined)

See `docs/recommendations.md` for the full framework and decision rules.

---

## Project Structure

```
.
├── backend/
│   ├── app/
│   │   ├── api/
│   │   │   ├── auth.py             # Register, login endpoints
│   │   │   └── files.py            # Upload, status, recommendations, download
│   │   ├── core/
│   │   │   ├── config.py           # Pydantic Settings (env vars)
│   │   │   ├── security.py         # JWT + Argon2
│   │   │   ├── minio_service.py    # MinIO client singleton
│   │   │   └── redis_service.py    # Redis push_job()
│   │   ├── db/
│   │   │   ├── engine.py           # Async PostgreSQL pool (asyncpg)
│   │   │   └── models/             # User, File, Run SQLAlchemy models
│   │   ├── schemas/                # Pydantic request/response models
│   │   ├── dependencies.py         # get_current_active_user
│   │   └── main.py                 # FastAPI app
│   └── requirements.txt
│
├── prefect/
│   ├── flows/
│   │   ├── dq_flow.py              # DQ Analysis Prefect flow (6 tasks)
│   │   ├── transform_flow.py       # Transform Prefect flow (6 tasks)
│   │   └── dq_logic.py             # Pure business logic (no Prefect imports)
│   ├── tests/
│   │   ├── test_dq_logic.py        # Unit tests for dq_logic.py
│   │   ├── test_llm_enrichment.py  # Unit tests for llm_enrichment.py (220 total)
│   │   └── test_llm_live.py        # Live LLM tests with prompt logging + token analytics
│   ├── clients.py                  # MinIO + PostgreSQL client factories
│   ├── redis_worker.py             # BRPOP worker → routes to Prefect deployments
│   ├── prefect.yaml                # Deployment config (dq-analysis + transform)
│   ├── Dockerfile
│   └── requirements.txt
│
├── test_data/                      # Sample CSVs for LLM live testing (generated)
│   ├── generate_datasets.py        # Generator script (run once to create CSVs)
│   ├── ecomm_orders_small.csv      # 100 rows, 7 cols — skewed amounts, sentinel discounts
│   ├── medical_labs_medium.csv     # 300 rows, 10 cols — MNAR leave_null (adv_evt_dt 74% null)
│   ├── finance_txn_medium.csv      # 350 rows, 9 cols — MAR leave_null (merchant null on ATM)
│   ├── iot_sensors_large.csv       # 1200 rows, 8 cols — sentinel -999, real outliers
│   ├── retail_catalog_wide.csv     # 500 rows, 13 cols — sparse column, sentinel "42%"
│   └── crm_contacts_xlarge.csv     # 2000 rows, 12 cols — correlated nulls, bimodal values
│
├── init.sql                        # DB schema (users, files, runs + run_status enum)
├── docker-compose.yaml
├── .env.example
└── CLAUDE.md
```

---

## API Reference

### Auth

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/auth/register` | Register new user |
| POST | `/api/auth/login` | Login → JWT token |

### Files

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/files/upload` | Upload CSV → triggers DQ Analysis flow |
| GET | `/api/files` | List user's files |
| GET | `/api/files/{file_id}` | File details |
| GET | `/api/files/{file_id}/status` | Latest run status + DQ scores |
| GET | `/api/files/{file_id}/recommendations` | Generated recommendations JSON |
| PUT | `/api/files/{file_id}/recommendations` | Approve recommendations → triggers Transform flow |
| GET | `/api/files/{file_id}/runs` | All runs for a file |

### Runs

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/runs/{run_id}` | Run details |
| GET | `/api/runs/{run_id}/download` | Presigned download URL (COMPLETED only) |

---

## Development

### Running Tests

```bash
# Unit tests (no API key needed) — 220 tests, all must pass before committing
.venv/bin/python3 -m pytest prefect/tests/test_dq_logic.py prefect/tests/test_llm_enrichment.py -v

# Live LLM tests (requires LLM_API_KEY)
LLM_API_KEY=<key> .venv/bin/python3 prefect/tests/test_llm_live.py          # all datasets
LLM_API_KEY=<key> .venv/bin/python3 prefect/tests/test_llm_live.py medical  # filter by name
```

### Updating Prefect Flows

After any change to `prefect/flows/`:

```bash
docker-compose up -d --build prefect-worker
docker logs prefect-worker | grep "successfully created"
```

### Useful Commands

```bash
# Start all services
docker-compose up -d

# Check running containers
docker-compose ps

# Follow worker logs (redis worker + flow execution)
docker logs -f prefect-worker

# Inspect DB
docker exec postgres psql -U prefect -d backend -c "SELECT id, status, dq_scores_before, dq_scores_after FROM runs ORDER BY created_at DESC LIMIT 5;"

# Check Redis queue depth
docker exec redis redis-cli LLEN jobs
```

---

## Troubleshooting

**`database "backend" does not exist`**
```bash
docker exec postgres psql -U prefect -d prefect -c "CREATE DATABASE backend;"
docker exec -i postgres psql -U prefect -d backend < init.sql
```

**DQ flow never starts**
```bash
docker logs prefect-worker | grep -E "ERROR|Listening"
docker exec redis redis-cli LLEN jobs    # should be 0 after worker picks up
```

**Transform flow fails with DB error**
Check that the migration was applied (columns must be JSONB not FLOAT):
```bash
docker exec postgres psql -U prefect -d backend -c "\d runs"
# dq_scores_before and dq_scores_after must show type: jsonb
```

---

## Roadmap

### Phase 1 — DQ Analysis Flow ✅
- [x] CSV upload → MinIO (raw bucket)
- [x] Redis job queue (LPUSH / BRPOP)
- [x] Prefect DQ Analysis flow (profile → score → recommend)
- [x] Completeness, Uniqueness, Validity, Consistency metrics
- [x] Invalid cell detection (minority wrong-type values in object columns)
- [x] 5-score output stored as JSONB (overall + 4 dimensions)

### Phase 2 — Transform Flow ✅
- [x] `apply_recommendations()` — schema cast, fill/drop, dedup, normalize
- [x] Prefect Transform flow (load → apply → upload → score → save)
- [x] Before / after DQ score comparison
- [x] Presigned download URL for cleaned CSV
- [x] Single Redis queue with `job_type` routing

### Phase 3 — LLM Enrichment (In Progress)
- [x] Groq + Llama 3.3 70B integration (replaced Anthropic)
- [x] Compact CSV profile format for LLM (4× smaller than JSON)
- [x] Partial diff format — LLM returns only changes, merged onto baseline
- [x] Multi-attempt runner with validation and retry prompt
- [x] Post-processing guards (echo strip, zero-null strip, no-op rename strip, note-only drop)
- [x] Rate limit backoff
- [ ] MAR leave_null — move `_correlated_nulls()` to `build_recommendations()` (currently in wrong file, not applied)
- [ ] Mean vs median selection based on skewness (currently always median)
- [ ] Narrow LLM prompt scope to semantic decisions only (MNAR + rename + notes)
- [ ] Remove CORRELATED NULLS section from LLM profile (LLM ignores it; MAR moves to code)

### Phase 4 — DQ Framework Completion (Next)
- [ ] Sentinel value detection and recommendations (e.g. -999, "N/A" as string in all-string columns)
- [ ] Outlier treatment integration into transform flow (currently detected, not applied)
- [ ] Custom transform generation from natural language (LLM → pandas code)
- [ ] Date format standardisation to ISO 8601 in transform

### Phase 5 — Frontend
- [ ] React UI for upload, review, and download
- [ ] Visual DQ score dashboard (before / after)
- [ ] Inline recommendation editor

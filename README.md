# Data Quality Engineering Pipeline

Automated data quality analysis and transformation system. Upload a CSV, get AI-powered recommendations, review and apply transformations, download cleaned data.

**Goal:** Reduce manual work for data engineers and make data quality analysis accessible, reproducible, and intelligent.

---

## 🌟 Features

- **Automated DQ Analysis** - Upload CSV → get completeness, uniqueness, validity, and consistency scores
- **AI-Powered Recommendations** - Smart suggestions for handling missing values, duplicates, normalization, and custom transforms
- **Interactive Review** - Review and edit recommendations via API before applying
- **Transformation Pipeline** - Apply approved transformations with before/after quality comparison
- **User Isolation** - Secure multi-tenant system with JWT authentication
- **Async Architecture** - FastAPI + Prefect + Redis for scalable processing

---

## 🏗️ Architecture

```
┌─────────────┐
│   User      │
└──────┬──────┘
       │ 1. Upload CSV
       ↓
┌─────────────────────────────────────────────────────────────┐
│                     FastAPI Backend                         │
│  • JWT Auth  • File Upload  • Status Polling                │
└──────┬──────────────────────────────────────────────────────┘
       │ 2. Save to MinIO (raw) + Metadata to PostgreSQL
       ↓
┌─────────────────────┐        ┌──────────────────┐
│      MinIO          │        │   PostgreSQL     │
│  Buckets:           │        │  Tables:         │
│  • raw-zone         │        │  • users         │
│  • curated-zone     │        │  • files         │
└─────────────────────┘        │  • runs          │
       │                       └──────────────────┘
       │ 3. Push job to Redis queue
       ↓
┌─────────────────────┐
│   Redis Queue       │
│   (dq_jobs)         │
└──────┬──────────────┘
       │ 4. Redis Worker picks up job
       ↓
┌─────────────────────────────────────────────────────────────┐
│                    Prefect Worker                           │
│  ┌────────────────┐              ┌────────────────┐         │
│  │  DQ Flow       │              │ Transform Flow │         │
│  │  • Profile     │              │ • Apply recs   │         │
│  │  • Score       │              │ • Save result  │         │
│  │  • Recommend   │─────────────>│ • Update score │         │
│  └────────────────┘              └────────────────┘         │
└─────────────────────────────────────────────────────────────┘
       │ 5. Save recommendations to DB
       ↓
┌─────────────────────┐
│   User Reviews &    │
│   Edits JSON        │
└──────┬──────────────┘
       │ 6. Approve recommendations
       ↓
┌─────────────────────┐
│  Cleaned CSV in     │
│  curated-zone       │
└─────────────────────┘
```

---

## 🛠️ Tech Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **API** | FastAPI | REST endpoints with async support |
| **Auth** | JWT + Argon2 | Secure authentication & password hashing |
| **Orchestration** | Prefect 3 | Workflow automation (DQ + Transform flows) |
| **Database** | PostgreSQL 16 | Metadata storage (users, files, runs) |
| **Object Storage** | MinIO | S3-compatible CSV storage (raw/curated zones) |
| **Queue** | Redis | Job queue (LPUSH/BRPOP pattern) |
| **AI** | Anthropic Claude | LLM for custom transforms (planned) |

---

## 📋 Prerequisites

- **Docker** & **Docker Compose** (v3.8+)
- **Python 3.11+** (for local backend development)
- **Git**
- **curl** or **Postman** (for API testing)

---

## 🚀 Setup Instructions (From Scratch)

### Step 1: Clone Repository

```bash
git clone <repository-url>
cd DQ_engineering_pipeline
```

### Step 2: Configure Environment Variables

Create a `.env` file in the project root:

```bash
cp .env.example .env  # If example exists, otherwise create manually
```

**Required `.env` configuration:**

```bash
# PostgreSQL
POSTGRES_USER=prefect
POSTGRES_PASSWORD=prefect
POSTGRES_DB=prefect
POSTGRES_HOST=localhost
BACKEND_DB=backend

# MinIO (S3-compatible storage)
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_ENDPOINT=localhost:9000

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DQ_QUEUE=dq_jobs

# JWT Authentication
SECRET_KEY=your-super-secret-key-change-this-in-production
ACCESS_TOKEN_EXPIRE_MINUTES=1440
ENVIRONMENT=development

# File Upload
MAX_FILE_SIZE_MB=200

# Anthropic API (for LLM features - optional for now)
ANTHROPIC_API_KEY=your-anthropic-api-key
```

> **Security Note:** Change `SECRET_KEY` to a strong random value in production. Generate with:
> ```bash
> python -c "import secrets; print(secrets.token_urlsafe(32))"
> ```

### Step 3: Start Infrastructure Services

```bash
docker-compose up -d postgres-prefect redis minio
```

**Verify services are running:**

```bash
docker ps
# You should see: postgres, redis, minio containers running
```

### Step 4: Initialize Databases

The PostgreSQL container only creates the `prefect` database automatically. We need to manually create the `backend` database.

**Connect to PostgreSQL:**

```bash
docker exec -it postgres psql -U prefect -d prefect
```

**Create backend database and tables:**

```sql
-- Create backend database
CREATE DATABASE backend;

-- Connect to backend database
\c backend

-- Run init.sql schema
-- Exit psql (Ctrl+D) and run:
```

```bash
# Apply schema from host machine
docker exec -i postgres psql -U prefect -d backend < init.sql
```

**Verify tables were created:**

```bash
docker exec -it postgres psql -U prefect -d backend -c "\dt"
```

You should see: `users`, `files`, `runs` tables.

### Step 5: Create MinIO Buckets

MinIO requires manual bucket creation.

**Option A: Using MinIO Console (GUI)**

1. Open browser: http://localhost:9001
2. Login with credentials:
   - Username: `minioadmin`
   - Password: `minioadmin`
3. Create two buckets:
   - `raw-zone`
   - `curated-zone`

**Option B: Using MinIO Client (CLI)**

```bash
# Install mc (MinIO Client)
docker run --rm --network dq_engineering_pipeline_default \
  --entrypoint=/bin/sh minio/mc -c "
  mc alias set local http://minio:9000 minioadmin minioadmin &&
  mc mb local/raw-zone &&
  mc mb local/curated-zone
  "
```

### Step 6: Start Prefect Server

```bash
docker-compose up -d prefect-server
```

**Wait for Prefect server to be ready (~30 seconds):**

```bash
docker logs -f prefect-server
# Wait for: "Uvicorn running on http://0.0.0.0:4200"
```

**Verify Prefect UI:**

Open browser: http://localhost:4200

### Step 7: Start Prefect Worker (Auto-deploys flows)

```bash
docker-compose up -d --build prefect-worker
```

This container:
1. **Auto-deploys** Prefect flows via `prefect deploy --all`
2. Starts **Redis worker** (background process listening on `dq_jobs` queue)
3. Starts **Prefect worker** (picks up flow runs from Prefect server)

**Verify deployment:**

```bash
docker logs prefect-worker
# Look for: "Successfully created/updated all deployments!"
```

**Check Prefect UI:**

Go to http://localhost:4200/deployments - you should see `dq-analysis` deployment.

### Step 8: Run FastAPI Backend

**Option A: Run in Docker (Production-like)**

```bash
# Add backend service to docker-compose.yaml (not yet included)
# For now, run locally:
```

**Option B: Run Locally (Development - Recommended)**

```bash
cd backend

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run FastAPI with auto-reload
python -m uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

**Verify backend is running:**

Open browser: http://localhost:8000/docs (FastAPI interactive documentation)

---

## ✅ Verification Checklist

After setup, verify all services:

```bash
# Check all containers are running
docker ps

# Expected containers:
# - postgres
# - redis
# - minio
# - prefect-server
# - prefect-worker

# Test connectivity
curl http://localhost:8000/docs        # FastAPI docs
curl http://localhost:4200             # Prefect UI
curl http://localhost:9001             # MinIO console
```

---

## 🧪 Quick Start Guide

### 1. Register a User

```bash
curl -X POST "http://localhost:8000/api/auth/register" \
  -H "Content-Type: application/json" \
  -d '{
    "username": "testuser",
    "email": "test@example.com",
    "password": "securepassword123"
  }'
```

**Response:**
```json
{
  "id": 1,
  "username": "testuser",
  "email": "test@example.com",
  "disabled": false,
  "created_at": "2026-02-16T..."
}
```

### 2. Login (Get JWT Token)

```bash
curl -X POST "http://localhost:8000/api/auth/login" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=testuser&password=securepassword123"
```

**Response:**
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "bearer"
}
```

**Save this token** - you'll need it for subsequent requests.

### 3. Upload a CSV File

Create a test CSV file:

```bash
cat > test.csv << EOF
name,age,salary,department
Alice,30,75000,Engineering
Bob,25,,Sales
Charlie,35,90000,Engineering
Alice,30,75000,Engineering
EOF
```

Upload the file:

```bash
TOKEN="your-access-token-from-step-2"

curl -X POST "http://localhost:8000/api/files/upload" \
  -H "Authorization: Bearer $TOKEN" \
  -F "file=@test.csv"
```

**Response:**
```json
{
  "file_id": "a1b2c3d4-...",
  "run_id": "e5f6g7h8-...",
  "message": "File uploaded successfully. DQ analysis job queued.",
  "status": "PENDING"
}
```

### 4. Poll Run Status

```bash
FILE_ID="a1b2c3d4-..."  # From upload response

curl -X GET "http://localhost:8000/api/files/$FILE_ID/status" \
  -H "Authorization: Bearer $TOKEN"
```

**Status progression:**
- `PENDING` → File uploaded, job queued
- `ANALYZING` → Prefect flow is running DQ analysis
- `AWAITING_REVIEW` → Recommendations generated, ready for review
- `TRANSFORMING` → Applying approved transformations
- `COMPLETED` → Done, cleaned file available
- `FAILED` → Error occurred (check `error_message`)

### 5. Get Recommendations (When status = AWAITING_REVIEW)

```bash
curl -X GET "http://localhost:8000/api/files/$FILE_ID/recommendations" \
  -H "Authorization: Bearer $TOKEN"
```

**Response (example):**
```json
{
  "schema": {
    "name": {"type": "string", "nullable": false},
    "age": {"type": "int", "nullable": false},
    "salary": {"type": "float", "nullable": true},
    "department": {"type": "string", "nullable": false}
  },
  "missing_values": {
    "salary": {
      "strategy": "median",
      "value": null
    }
  },
  "duplicates": {
    "strategy": "drop",
    "subset": [],
    "keep": "first"
  },
  "normalization": {
    "columns": ["name", "department"]
  },
  "custom_transforms": [],
  "_metadata": {
    "generated_at": "2026-02-16T10:30:00Z",
    "dq_score": 67.5,
    "issues_found": {
      "missing_values": 1,
      "duplicate_rows": 1
    }
  }
}
```

### 6. Review & Edit Recommendations (Optional)

Edit the JSON as needed, then submit:

```bash
curl -X PUT "http://localhost:8000/api/files/$FILE_ID/recommendations" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "schema": { ... },
    "missing_values": {
      "salary": {
        "strategy": "mean",
        "value": null
      }
    },
    "duplicates": {
      "strategy": "drop",
      "keep": "first"
    },
    "normalization": {
      "columns": ["name", "department"]
    },
    "custom_transforms": []
  }'
```

This triggers the **Transform Flow** (when implemented).

### 7. Download Cleaned File

```bash
RUN_ID="e5f6g7h8-..."  # From upload response

curl -X GET "http://localhost:8000/api/runs/$RUN_ID/download" \
  -H "Authorization: Bearer $TOKEN"
```

**Response:**
```json
{
  "download_url": "http://localhost:9000/curated-zone/...",
  "filename": "test_cleaned.csv",
  "expires_in_seconds": 3600
}
```

Use the `download_url` to download the cleaned CSV (presigned URL, valid for 1 hour).

---

## 📂 Project Structure

```
.
├── backend/                    # FastAPI application
│   ├── app/
│   │   ├── api/
│   │   │   ├── auth.py        # Auth endpoints (register, login)
│   │   │   └── files.py       # File upload, status, recommendations, download
│   │   ├── core/
│   │   │   ├── config.py      # Pydantic Settings
│   │   │   ├── security.py    # JWT + Argon2 hashing
│   │   │   ├── minio_service.py   # MinIO client (singleton)
│   │   │   └── redis_service.py   # Redis job queue client
│   │   ├── db/
│   │   │   ├── engine.py      # Async PostgreSQL connection pool
│   │   │   └── models/        # User, File, Run models (asyncpg)
│   │   ├── schemas/           # Pydantic request/response models
│   │   ├── dependencies.py    # FastAPI dependencies (auth, DB)
│   │   └── main.py           # FastAPI app initialization
│   └── requirements.txt
│
├── prefect/                   # Prefect workflows
│   ├── flows/
│   │   └── dq_flow.py        # DQ Analysis flow (7 tasks)
│   ├── redis_worker.py       # Redis BRPOP worker → triggers Prefect
│   ├── prefect.yaml          # Deployment configuration
│   ├── Dockerfile            # Prefect worker image
│   └── requirements.txt
│
├── init.sql                  # Database schema (users, files, runs)
├── docker-compose.yaml       # Service orchestration
├── .env                      # Environment variables
├── CLAUDE.md                 # AI assistant instructions
└── README.md                 # This file
```

---

## 🔌 API Endpoints

### Authentication

| Method | Endpoint | Description | Auth Required |
|--------|----------|-------------|---------------|
| POST | `/api/auth/register` | Register new user | No |
| POST | `/api/auth/login` | Login → JWT token | No |

### Files

| Method | Endpoint | Description | Auth Required |
|--------|----------|-------------|---------------|
| POST | `/api/files/upload` | Upload CSV → triggers DQ flow | Yes |
| GET | `/api/files` | List user's files | Yes |
| GET | `/api/files/{file_id}` | Get file details | Yes |
| GET | `/api/files/{file_id}/status` | Get latest run status | Yes |
| GET | `/api/files/{file_id}/recommendations` | Get generated recommendations JSON | Yes |
| PUT | `/api/files/{file_id}/recommendations` | Submit edited recommendations → triggers Transform flow | Yes |
| GET | `/api/files/{file_id}/runs` | List all runs for file | Yes |

### Runs

| Method | Endpoint | Description | Auth Required |
|--------|----------|-------------|---------------|
| GET | `/api/runs/{run_id}` | Get run details | Yes |
| GET | `/api/runs/{run_id}/download` | Download cleaned file (presigned URL) | Yes |

**Full interactive documentation:** http://localhost:8000/docs

---

## 🔧 Development Workflow

### Making Code Changes

**Backend changes:**
```bash
cd backend
# Code is auto-reloaded if running with --reload flag
```

**Prefect flow changes:**
```bash
# Edit files in prefect/flows/
# Rebuild and restart worker:
docker-compose up -d --build prefect-worker

# Verify deployment updated:
docker logs prefect-worker | grep "Successfully"
```

### Viewing Logs

```bash
# FastAPI backend (if running locally)
# Logs appear in terminal

# Prefect worker (redis worker + flow execution)
docker logs -f prefect-worker

# Prefect server
docker logs -f prefect-server

# PostgreSQL
docker logs postgres

# Redis
docker logs redis

# MinIO
docker logs minio
```

### Database Access

```bash
# Connect to PostgreSQL
docker exec -it postgres psql -U prefect -d backend

# Run queries
SELECT * FROM users;
SELECT * FROM files;
SELECT * FROM runs;

# Exit
\q
```

### Testing Prefect Flows Manually

```bash
# Enter prefect-worker container
docker exec -it prefect-worker bash

# Run flow manually (bypass Redis queue)
python -c "from flows.dq_flow import dq_analysis_flow; dq_analysis_flow(run_id='test-123', file_id='test-456', minio_raw_path='raw-zone/test.csv')"
```

---

## 🐛 Troubleshooting

### Issue: Prefect worker can't connect to server

**Symptoms:** Logs show connection refused errors.

**Solution:**
```bash
# Verify prefect-server is running
docker ps | grep prefect-server

# Restart services in order
docker-compose restart prefect-server
docker-compose restart prefect-worker
```

### Issue: Backend can't connect to PostgreSQL

**Symptoms:** `asyncpg.exceptions.InvalidCatalogNameError: database "backend" does not exist`

**Solution:**
```bash
# Manually create backend database (Step 4)
docker exec -it postgres psql -U prefect -d prefect -c "CREATE DATABASE backend;"
docker exec -i postgres psql -U prefect -d backend < init.sql
```

### Issue: File upload returns 500 error

**Check:**
1. MinIO buckets exist (`raw-zone`, `curated-zone`)
2. Redis is running (`docker ps | grep redis`)
3. Backend logs for detailed error

### Issue: DQ flow never starts

**Check:**
```bash
# Verify Redis worker is running
docker logs prefect-worker | grep "Redis worker"

# Check Redis queue has jobs
docker exec -it redis redis-cli LLEN dq_jobs

# Verify deployment exists in Prefect UI
# http://localhost:4200/deployments
```

### Issue: "Permission denied" when running docker commands

**Solution:**
```bash
# Add user to docker group (Linux)
sudo usermod -aG docker $USER
# Log out and back in

# Or use sudo
sudo docker-compose up -d
```

---

## 🔐 Security Considerations

- **Never commit `.env` to version control** (add to `.gitignore`)
- **Change default passwords** in production (PostgreSQL, MinIO, JWT secret)
- **Use HTTPS** in production (not HTTP)
- **Limit file upload size** (configured via `MAX_FILE_SIZE_MB`)
- **Validate user input** in custom transforms (prevent code injection)
- **Enable MinIO access policies** for production deployments

---

## 📊 DQ Score Formula

The data quality score is a weighted average of four components:

| Component | Weight | Calculation |
|-----------|--------|-------------|
| **Completeness** | 35% | `(1 - missing_cells / total_cells) × 100` |
| **Uniqueness** | 25% | `(1 - duplicate_rows / total_rows) × 100` |
| **Validity** | 25% | `(type_conforming_cells / total_cells) × 100` |
| **Consistency** | 15% | `(pattern_matching_cells / total_cells) × 100` |

**Current Status:**
- ✅ Completeness & Uniqueness: Fully implemented
- ⚠️ Validity & Consistency: Hardcoded to 100% (TODO)

---

## 🗺️ Roadmap

### Phase 0: Documentation ✅
- [x] Comprehensive README with setup instructions

### Phase 1: Complete DQ Flow (In Progress)
- [ ] Real MinIO file downloads in Prefect
- [ ] Database writes for run status updates
- [ ] Implement Validity & Consistency metrics

### Phase 2: Transform Flow
- [ ] Create `transform_flow.py` in Prefect
- [ ] Apply transformations based on approved JSON
- [ ] Calculate before/after DQ scores
- [ ] Save cleaned file to `curated-zone`

### Phase 3: LLM Integration
- [ ] Anthropic API integration for custom transforms
- [ ] Natural language → pandas code generation
- [ ] Validation & retry logic (max 3 attempts)

### Phase 4: Polish
- [ ] Comprehensive error handling
- [ ] Integration tests
- [ ] User preference tracking (ML model)
- [ ] Performance optimization

---

## 🤝 Contributing

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Make changes and test locally
3. Commit with descriptive message: `git commit -m "Add feature X"`
4. Push and create pull request

---

## 📝 License

[Add your license here]

---

## 📞 Support

- **Issues:** [GitHub Issues](https://github.com/your-repo/issues)
- **Documentation:** This README + http://localhost:8000/docs
- **Prefect Docs:** https://docs.prefect.io

---

## 🎯 Quick Reference

**Service URLs:**
- FastAPI Backend: http://localhost:8000
- FastAPI Docs: http://localhost:8000/docs
- Prefect UI: http://localhost:4200
- MinIO Console: http://localhost:9001
- PostgreSQL: localhost:5432
- Redis: localhost:6379

**Default Credentials:**
- PostgreSQL: `prefect` / `prefect`
- MinIO: `minioadmin` / `minioadmin`

**Key Commands:**
```bash
# Start all services
docker-compose up -d

# Stop all services
docker-compose down

# View logs
docker logs -f <container-name>

# Rebuild specific service
docker-compose up -d --build <service-name>

# Clean restart (removes volumes)
docker-compose down -v && docker-compose up -d
```

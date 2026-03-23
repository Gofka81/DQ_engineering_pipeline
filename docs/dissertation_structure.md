# Dissertation Structure

**Project:** Data Quality Engineering Pipeline — Automated DQ Analysis, LLM Enrichment & Reproducible Transformation
**Date:** 2026-03-23
**Total pages:** 50

---

## Table of Contents

| # | Chapter / Section | Pages |
|---|-------------------|-------|
| 1 | Introduction | 4 |
| 1.1 | Problem Statement & Motivation | 2 |
| 1.2 | Project Objectives | 1 |
| 1.3 | Scope & Constraints | 0.5 |
| 1.4 | Report Structure | 0.5 |
| 2 | Background & Related Work | 10 |
| 2.1 | Data Quality Concepts & Frameworks | 3 |
| 2.2 | Missing Data Mechanisms — MCAR/MAR/MNAR | 2 |
| 2.3 | Outlier Detection & Treatment | 1 |
| 2.4 | Type Casting, Normalization & Deduplication | 1 |
| 2.5 | LLM Applications in Tabular Data | 2 |
| 2.6 | Technology Landscape | 1 |
| 3 | System Design & Architecture | 9 |
| 3.1 | Functional & Non-Functional Requirements | 2 |
| 3.2 | High-Level Architecture | 2 |
| 3.3 | Pipeline Design | 2 |
| 3.4 | Recommendations JSON Schema | 2 |
| 3.5 | LLM Integration Design & Boundaries | 1 |
| 4 | Implementation | 13 |
| 4.1 | DQ Profiling & Scoring | 3 |
| 4.2 | Recommendation Generation Logic | 3 |
| 4.3 | LLM Enrichment Pipeline | 3 |
| 4.4 | Transform Application | 2 |
| 4.5 | API & Infrastructure Layer | 1 |
| 4.6 | Frontend | 1 |
| 5 | Evaluation | 10 |
| 5.1 | Test Strategy | 1 |
| 5.2 | Functional Correctness | 3 |
| 5.3 | DQ Score Improvement — Before/After | 3 |
| 5.4 | LLM Enrichment Quality | 2 |
| 5.5 | Performance & Scalability | 1 |
| 6 | Discussion & Conclusion | 4 |
| 6.1 | Key Design Decisions & Trade-offs | 1 |
| 6.2 | Limitations | 1 |
| 6.3 | Future Work | 1 |
| 6.4 | Conclusion | 1 |
| | **Total** | **50** |

---

## Chapter 1: Introduction (4p)

### 1.1 Problem Statement & Motivation (2p)
- Data quality work is predominantly manual, ad-hoc, and non-reproducible — data engineers spend significant time profiling datasets, deciding on strategies, and applying transforms without a structured audit trail
- Opportunity to automate the full cycle: statistical profiling → scored recommendations → LLM-enriched semantic context → structured user review → reproducible transform application → before/after score comparison
- Framing: DQ as a first-class engineering concern, not a preprocessing afterthought

### 1.2 Project Objectives (1p)
- Automate DQ analysis across 4 DAMA DMBOK dimensions (completeness, uniqueness, validity, consistency)
- Generate user-editable JSON recommendations combining rule-based decisions with LLM semantic enrichment
- Apply approved transforms reproducibly and record before/after DQ scores per dimension
- Provide a structured review interface so engineers can audit and override every recommended change

### 1.3 Scope & Constraints (0.5p)
- CSV files only, maximum 200MB per file
- Single-file analysis — no multi-file joins or cross-dataset comparisons
- Within-column checks only — cross-column constraint validation (e.g. start\_date < end\_date) is out of scope
- Numeric range validation is out of scope; validity dimension = type conformance only

### 1.4 Report Structure (0.5p)
- Signposting paragraph: what each chapter covers and how they relate

---

## Chapter 2: Background & Related Work (10p)

### 2.1 Data Quality Concepts & Frameworks (3p)
- DAMA DMBOK dimensions as theoretical grounding — completeness, uniqueness, validity, consistency; rationale for selecting these four over broader frameworks
- Project-specific scoring weights (35/25/25/15 — completeness/uniqueness/validity/consistency) disclosed explicitly as project heuristics; no standards body specifies numeric weights for these dimensions
- ISO/IEC 25012 (data quality model for software) vs ISO 8000 (master data for supply chain) — clarify why ISO 8000 is not applicable; ISO/IEC 25012 is the relevant reference but this project uses DAMA DMBOK terminology
- Custom profiler rationale over existing tools: ydata-profiling and Great Expectations offer broad coverage but limited programmatic control over recommendation logic and transform application; custom profiler enables tight integration with the recommendation schema and LLM pipeline (Engineering Decision #7)

### 2.2 Missing Data Mechanisms — MCAR/MAR/MNAR (2p)
- Van Buuren (2018) taxonomy: Missing Completely At Random (MCAR), Missing At Random (MAR), Missing Not At Random (MNAR)
- Formal MAR detection implemented: point-biserial correlation (numeric targets) and chi-square test (categorical targets), α = 0.05; if a column's missingness is significantly associated with another column's values, it is classified as MAR and the system overrides the fill strategy to `leave_null`
- MNAR is undetectable by statistical tests (missingness depends on the unobserved value itself) — handled by LLM semantic reasoning + user override
- MCAR assumed as default when MAR tests fail — this is a documented simplification, not a formal test result
- Decision precedence hierarchy: MAR detection (hard override) → LLM note → user edit → system default

### 2.3 Outlier Detection & Treatment (1p)
- IQR method: lower fence = Q1 − 1.5×IQR, upper fence = Q3 + 1.5×IQR; bounds stored in profiling output and surfaced in recommendations
- Advisory-only: outlier presence does not directly penalise the DQ score (outside validity/consistency scope for numeric range); user selects strategy: keep / winsorise / remove / cap
- IQR chosen over Z-score: no normality assumption required; more robust to heavy-tailed distributions common in real datasets (salary, sensor readings, transaction amounts)
- Heuristic default strategy selection: `_outlier_strategy()` selects treatment based on outlier prevalence (< 100 rows → winsorise; < 1% → remove; 1–5% → winsorise; > 5% → keep); thresholds disclosed as project heuristics with practitioner-source grounding, not formal statistical standards
- LLM domain note: LLM receives IQR bounds + heuristic-selected strategy and must add a per-column `outliers[col].note` with domain reasoning (errors vs legitimate extremes, strategy assessment); LLM cannot change the strategy

### 2.4 Type Casting, Normalization & Deduplication (1p)
- Type cast guard: `_would_cast_safely()` skips int/float cast if > 5% of non-null values would become NaN — prevents silent data loss
- Normalization selection: `min_max` when no outliers detected; `z_score` when outliers present (IQR count > 0); only triggered when `col_max_abs > 100 AND col_range > 0`
- Sentinel detection: string sentinels via frozenset lookup; numeric sentinels via 3×IQR fence with count ≥ 5 threshold; replaced with NaN in step 0 of apply
- Deduplication: exact row match only; subset + keep strategy user-controlled; fuzzy deduplication out of scope

### 2.5 LLM Applications in Tabular Data (2p)
- LLM for semantic tasks that resist rule-based treatment: MNAR reasoning, column rename suggestions for opaque names (e.g. guessing "temperature" from numeric values in a CRM dataset), transform hints for per-cell value-level issues (% suffixes, currency prefixes, mixed date formats)
- Task decomposition: two-LLM pipeline — LLM 1 enriches recommendations (adds notes, renames, transform hints); LLM 2 converts transform hints to validated pandas lambdas
- AST safety validation: whitelist of safe operations, blocks `eval`/`exec`/`import`; execution-tested on 200 sample rows before accepting
- LLM non-determinism as an accepted property: Llama 3.3 70B at temperature=0 is not fully deterministic on distributed GPU; rename suggestions and transform hints may vary across runs; documented as expected behaviour, not a bug
- Groq + Llama 3.3 70B: sub-second inference latency, zero development cost at research volumes

### 2.6 Technology Landscape (1p)
- Prefect 3 vs Airflow: Prefect chosen for Python-native flow definition, lightweight local deployment, and simpler Docker integration; Airflow adds scheduling overhead not needed for on-demand pipeline runs
- MinIO: S3-compatible object storage with raw/curated zone separation — raw preserves originals, curated stores cleaned outputs; ILM policy handles retention
- Redis LPUSH/BRPOP: decouples FastAPI (producer) from Prefect worker (consumer); single `jobs` queue with `job_type` routing avoids queue proliferation
- PostgreSQL with JSONB: structured metadata (users, files, runs) alongside flexible recommendation storage in JSONB columns; asyncpg for non-blocking I/O throughout

---

## Chapter 3: System Design & Architecture (9p)

### 3.1 Functional & Non-Functional Requirements (2p)
- Functional: upload CSV → trigger DQ analysis → review/edit recommendations → submit → apply transforms → download cleaned file
- All state transitions observable: SSE event stream exposes run status changes to the frontend in real time
- Per-user data isolation: UUID primary keys for files and runs (not SERIAL) prevent enumeration; all queries filter by `user_id`
- Full audit trail: `recommendations_generated` (auto) and `recommendations_approved` (user-edited) stored separately in `runs` table; before/after DQ scores stored as JSONB
- Run status lifecycle: `PENDING → ANALYZING → AWAITING_REVIEW → TRANSFORMING → COMPLETED | FAILED`
- Non-functional: async throughout (FastAPI + asyncpg); presigned download URLs (no proxying large files through backend); Redis push failure returns 503 + leaves run as PENDING (safe for retry, not FAILED)

### 3.2 High-Level Architecture (2p)
- Component diagram: FastAPI ↔ PostgreSQL ↔ MinIO ↔ Redis ↔ Prefect Worker (two flows)
- Docker multi-process pattern: `prefect-worker` container runs redis\_worker.py (background) and prefect worker (foreground) using `bash -c` with `trap` + `wait -n` for proper signal handling; Alpine uses dash for `sh` which does not support `wait -n` — must use `bash`
- Single `jobs` queue with `job_type` routing: one Redis queue, worker routes by `job_type: "dq_analysis" | "transform"` to the correct Prefect deployment; avoids per-job-type queue proliferation
- MinIO two-zone layout: `raw/` bucket stores original uploads; `curated/` bucket stores cleaned outputs

### 3.3 Pipeline Design (2p)
- DQ Analysis flow: `profile_dataframe()` → `score_dataframe()` → `build_recommendations()` → `enrich_recommendations()` (LLM 1) → `build_dropmasks()` → save to DB + MinIO
- Transform flow: load from MinIO → `generate_missing_transform_codes()` (LLM 2 for user-added hints) → `apply_recommendations()` → upload curated CSV → `score_dataframe()` after → save before/after scores → status COMPLETED
- Thin Prefect wrappers: all business logic lives in `dq_logic.py` (pure functions, no Prefect imports); Prefect tasks are wrappers that call logic functions and handle status updates; enables unit testing without Prefect context
- `prefect/flows/common.py`: shared utilities — `publish_status()` (Redis pubsub for SSE), `update_run_status` task, `setup_file_logger()`

### 3.4 Recommendations JSON Schema (2p)
- Column-centric design: all per-column data lives in one dict under `columns[col]` — type, nullable, missing\_values strategy, normalize, warnings, note, sentinel\_values, rename\_to, transform\_hint, transform\_code
- Top-level non-column keys: `duplicates`, `outliers` (per-column bounds + strategy), `custom_transforms`, `_metadata` (generation timestamp, DQ score, issue counts), `_eda` (display-only profile snapshot)
- `_eda` is never sent to LLM (`_lean_baseline()` excludes it) and never read by `apply_recommendations()`; it exists solely to populate the frontend EDA dashboard without a second API call
- Fixed contract between flows: analysis flow writes recommendations; transform flow reads them; schema is the interface; no shared code paths
- Partial diff merge: LLM returns only changed columns and changed keys; deep-merged onto the baseline; reduces token cost by ~60% and keeps LLM output focused on changes

### 3.5 LLM Integration Design & Boundaries (1p)
- Hard boundary: code handles all quantitative decisions (fill strategy selection, outlier bounds, normalize condition, type cast guard, sentinel thresholds); LLM handles semantic decisions only (MNAR notes, rename suggestions, transform hints)
- LLM cannot override MAR-detected `leave_null` — post-processing guard enforces this regardless of LLM output
- Lean baseline: `_lean_baseline()` strips default/unchanged fields before sending to LLM; compact JSON; approximately 60% token reduction vs full schema
- Runner → Validator pattern: `enrich_recommendations()` calls LLM, validates schema and individual fields, applies post-processing guards, then deep-merges the partial diff

---

## Chapter 4: Implementation (13p)

### 4.1 DQ Profiling & Scoring (3p)
- `profile_dataframe()` — per-column profiling: type detection (int, float, string, date, bool), null counts, invalid\_count, IQR bounds, bool variant detection (yes/no/y/n/on/off/1/0), format inconsistency detection (partial castability: `castable_count ≥ 5 AND castable_pct < 80%`)
- `score_dataframe()` — 4-dimension weighted formula: completeness 35% (`(1 − missing_cells/total_cells) × 100`), uniqueness 25% (`(1 − duplicate_rows/total_rows) × 100`), validity 25% (`type_conforming_cells/total_cells × 100`), consistency 15% (`pattern_matching_cells/total_cells × 100`)
- pandas 2.x pin (`>=2.2.0,<3.0.0`): pandas 3.0 changed object dtype to str, breaking all `"object" in str(dtype)` checks throughout the profiler; version constraint is a deliberate stability decision
- Edge cases handled: zero-range normalization (skipped), all-null column (strategy = leave\_null regardless of MAR), single-value column (dedup skipped)

### 4.2 Recommendation Generation Logic (3p)
- `build_recommendations()` — per-column rule hierarchy: sentinel removal first → type detection → MAR override → fill strategy → normalize condition → outlier heuristic strategy → format inconsistency warning
- `_detect_mar_columns()`: for each column with nulls, creates binary missingness indicator; tests against all other columns using point-biserial (numeric targets) or chi-square (categorical targets) at α = 0.05; columns that pass are flagged MAR and forced to `leave_null`
- `_fill_strategy()`: skewness-based mean vs median selection — if outliers detected (IQR count > 0) OR `|mean − median| / std > 0.15` → median; otherwise mean
- Drop-impact bitsets: `build_dropmasks()` precomputes a bit array per column marking which rows would be dropped under each strategy; stored in MinIO at analysis time; `/api/runs/{run_id}/drop-impact` endpoint takes current recommendation state and returns exact row counts in sub-millisecond time; `RecommendationsEditor` polls with 300ms debounce on recommendation edits

### 4.3 LLM Enrichment Pipeline (3p)
- LLM 1 — `enrich_recommendations()`: constructs lean baseline (default fields stripped + outlier section with strategy/count/bounds included), sends to Groq + Llama 3.3 70B with system prompt enforcing format rules; validates returned JSON against schema; post-processing guards: MAR `leave_null` cannot be overridden, `rename_to` must be valid Python identifier, `transform_hint` must be non-empty string if present, outlier `strategy`/`count`/`lower`/`upper` are read-only (guard strips them if hallucinated), self-referential outlier notes ("consider X instead of X") are stripped; deep-merges partial diff onto baseline including outlier notes
- Outlier note prompt quality: generic default phrasings are explicitly banned in the system prompt; the LLM is required to reason on three axes — IQR bound plausibility, prevalence pattern interpretation, and strategy justification; this was a necessary refinement after initial evaluation showed template-like notes repeated across all columns
- Explicit LLM 1 rule (Phase 2.6): if a column has a `format_inconsistency` warning, LLM MUST add `transform_hint`; enforced in `_RUNNER_SYSTEM` prompt rules
- LLM 2 — `generate_transform_code()`: takes `transform_hint` → constructs prompt requesting a pandas lambda expression → AST-validates the returned expression (whitelist: safe builtins, pandas string methods, arithmetic; blocks `eval`/`exec`/`import`/`__`); execution-tested on 200 sample rows; up to 3 retries with error feedback; accepted code stored in `transform_code`
- `generate_missing_transform_codes()` in transform flow: runs LLM 2 for any column with a `transform_hint` but no `transform_code`; handles user-added hints that were not present at analysis time
- Rollback on damage: if applying `transform_code` introduces > 5% new nulls or raises any exception, the transform is rolled back and the original column values are preserved; logged as a warning

### 4.4 Transform Application (2p)
- `apply_recommendations()` — ordered steps ensure deterministic application:
  - Step 0a: sentinel replacement (string frozenset + numeric fence values → NaN)
  - Step 0b: custom transforms (sandboxed `eval` per column; rollback on > 5% null damage or exception)
  - Step 1: type casting (guarded by `_would_cast_safely()`)
  - Step 2: missing value fill (median/mean/mode/fill/drop\_row/drop\_column/leave\_null)
  - Step 3: int recast after fill + outlier treatment (winsorise/remove/cap per column)
  - Step 4: deduplication (exact rows; subset + keep strategy)
  - Step 5: normalization (min\_max or z\_score per column)
  - Step 6: column rename (valid Python identifiers only)
- Before/after scoring: `score_dataframe()` called on original and transformed DataFrames; both stored as JSONB in `runs.dq_scores_before` and `runs.dq_scores_after`

### 4.5 API & Infrastructure Layer (1p)
- FastAPI endpoints: auth (register/login with JWT + Argon2), file upload → MinIO + Redis LPUSH, status polling, recommendations GET/PUT, paginated file listing, SSE event stream, presigned download URL, drop-impact calculation
- SSE endpoint: `GET /api/files/{file_id}/events` — uses `redis.asyncio` pubsub; JWT passed via `?token=` query param (EventSource API does not support Authorization headers); `doneRef` in frontend hook prevents reconnect loop after terminal state
- Redis push failure on upload: returns HTTP 503 to client, leaves run as PENDING; safe for manual retry; deliberately not marked FAILED (Engineering Decision #25)

### 4.6 Frontend (1p)
- React + Vite 7 + TypeScript + Tailwind CSS v4; Node 20+ required (Vite 7 constraint)
- EDA Dashboard (AWAITING\_REVIEW Data Profile tab): DQ radar chart (Recharts), missing heatmap, histogram grid, box plot group, categorical frequency bars — all rendered client-side from `_eda` field in recommendations JSON; no second API call
- AWAITING\_REVIEW layout: three tabs — Recommendations (default) | Data Profile | Data Preview; SubmitBar visible on Recommendations tab only
- COMPLETED layout: three tabs — Recommendations | Data Profile | Data Preview; Data Profile shows ScoreComparison + IssuesGrid (not EDA — raw-data charts are meaningless post-transform)
- Drop-impact bar in RecommendationsEditor: debounced `getDropImpact` call (300ms) shows exact affected rows/cells as user edits drop strategy

---

## Chapter 5: Evaluation (10p)

### 5.1 Test Strategy (1p)
- Unit tests: `test_dq_logic.py` (1966 lines, 19 test classes) covering profiling, scoring, recommendation logic, apply steps; `test_llm_enrichment.py` (619 lines, 5 test classes) covering LLM runner, validator, post-processing guards, partial diff merge
- Integration: `test_pipeline.py` — 7 test datasets spanning sizes (small to xlarge) and schema types (CRM contacts, ecomm orders, finance transactions, IoT sensors, medical labs, opaque fields, retail catalog wide)
- All tests must pass before commit — enforced as development rule; `CLAUDE.md` specifies exact commands

### 5.2 Functional Correctness (3p)
- Unit test results: MAR detection correctly identifies and overrides to `leave_null`; type cast guard correctly skips casts that would damage > 5% of values; sentinel detection catches both string and numeric sentinels; bool variant recognition covers all 8 variants
- Apply step ordering: unit tests verify sentinels are replaced before type casting, type casting occurs before fill, fill occurs before normalization; step order bugs would produce incorrect results silently
- Edge cases: zero-range column (normalization skipped without error), all-null column (leave\_null regardless of MAR test), mixed-type column triggering format\_inconsistency warning, column with all duplicates triggering uniqueness score of 0

### 5.3 DQ Score Improvement — Before/After (3p)
- Tabulated before/after scores per dimension and overall across all 7 test datasets
- Analysis of which transform types yield the largest score improvements per dataset
- Dataset-to-feature mapping: `opaque_fields.csv` → rename + transform\_hint; `iot_sensors_large.csv` → outlier detection + normalization; `retail_catalog_wide.csv` → wide schema stress test (prompt size, lean baseline effectiveness)
- Disclosure: DQ weights (35/25/25/15) are project-specific heuristics — score improvements are relative to this formula, not an absolute standard

### 5.4 LLM Enrichment Quality (2p)
- Rename quality on opaque column names: `llama-3.3-70b-versatile` required for cross-column domain reasoning; smaller models fail to guess "temperature" from numeric values in a CRM dataset context
- Transform hint quality: % suffix stripping and division by 100, currency prefix removal, mixed date format normalisation — examples from test datasets
- Non-determinism characterisation: 2–3 repeated runs on the same dataset at temperature=0; document observed variation in rename and transform\_hint outputs; acknowledge as accepted LLM property, not a defect
- Token reduction: lean baseline reduces prompt size by approximately 60% vs full schema; measured across test datasets

### 5.5 Performance & Scalability (1p)
- Wall-clock times from `test_pipeline.py` across dataset sizes: small (ecomm orders) through xlarge (CRM contacts); times recorded for: profile + score, build\_recommendations, LLM 1 enrichment, LLM 2 code generation, apply\_recommendations
- Groq latency: sub-second per LLM call; 2 LLM calls per run (LLM 1 + LLM 2 per column with transform\_hint); Groq rate limits noted if encountered
- Known bottleneck: wide-schema datasets (retail\_catalog\_wide) increase prompt size even with lean baseline; recommend maximum column count guidance for production use

---

## Chapter 6: Discussion & Conclusion (4p)

### 6.1 Key Design Decisions & Trade-offs (1p)
- Code vs LLM boundary as the central architectural insight: quantitative decisions (fill strategy, outlier bounds, normalization condition, type cast guard) are deterministic and testable in code; semantic decisions (MNAR reasoning, rename, transform hints) are delegated to LLM; the boundary is maintained by post-processing guards that code enforces after every LLM call
- Column-centric schema enabling partial diff merge: all per-column data in one dict allows LLM to return only changed keys; reduces token cost and focuses LLM output; alternative (flat list of column objects) would require full replacement on any change
- Custom profiler maintenance burden vs control: ydata-profiling and Great Expectations provide more features out of the box but less control over recommendation logic and schema; custom profiler required for the LLM integration and step-ordered apply pipeline

### 6.2 Limitations (1p)
- CSV only; maximum 200MB per file — no support for Parquet, Excel, or JSON inputs
- Within-column analysis only — cross-column constraint checks (e.g. start\_date < end\_date, quantity × unit\_price = total\_price) are not detected
- Validity dimension captures type conformance only — numeric range violations (e.g. age = 300, temperature = −9999) are not flagged unless they fall within sentinel detection thresholds
- LLM non-determinism: temperature=0 on distributed GPU is not fully deterministic; rename and transform suggestions may vary across runs
- MCAR assumed as default when MAR tests fail — MCAR is not formally tested, it is the fallback assumption
- No user study or usability evaluation — scope limitation acknowledged; evaluation is quantitative (scores, correctness, performance) only

### 6.3 Future Work (1p)
- ML model on (generated, approved) diff pairs: train on the delta between auto-generated and user-approved recommendations to predict better defaults over time; per-user preference learning
- Fuzzy deduplication: blocking + similarity threshold for approximate duplicate detection; required for real-world entity data (name variants, address formats)

- Per-user preference learning from recommendation edit history: train on (generated, approved) diff pairs to predict better defaults over time (F4 LLM outlier reasoning is now implemented — see §4.2 and §4.3)
- Cross-column consistency dimension: detect constraint violations between columns; would require schema-aware rules or LLM-driven constraint inference

### 6.4 Conclusion (1p)
- Summary of what was built: end-to-end automated DQ pipeline — statistical profiling across 4 DAMA DMBOK dimensions, rule-based recommendations with LLM semantic enrichment, structured user review with drop-impact preview, reproducible transform application with full audit trail
- What before/after scores demonstrate: automated transforms consistently improve DQ scores across all 7 test datasets; improvements are largest for completeness (missing value fills) and validity (type cast corrections and sentinel removal)
- Reflection on code-vs-LLM boundary: the most important architectural decision was defining what the code must decide and what the LLM may suggest; this boundary enables a system that is both statistically grounded and semantically aware, while remaining testable, auditable, and reproducible

---

## Gaps — Items Needed Before Writing

| # | Gap | Severity | Action |
|---|-----|----------|--------|
| G1 | No benchmark comparison vs ydata-profiling / Great Expectations | Medium | Add qualitative comparison table in §2.1; Engineering Decision #7 gives rationale but no evidence table |
| G2 | No documented performance benchmarks | High | Run `test_pipeline.py` on all 7 datasets; record wall-clock times per stage before writing §5.5 |
| G3 | DQ score weights (35/25/25/15) have no literature grounding | Medium | Disclose explicitly in §2.1 and §5.3 as project-specific heuristics; cite that no standards body specifies numeric weights |
| G4 | LLM non-determinism not formally characterised | Low | 2–3 repeated runs on same dataset at temperature=0; record observed variation; or acknowledge as limitation in §6.2 |
| G5 | No user study / usability evaluation | Low | Scope limitation to acknowledge in §6.2; no action required |
| G6 | MCAR assumed as default — never formally tested | Medium | State explicitly in §2.2: MCAR is the fallback assumption when MAR tests fail, not a tested result |
| G7 | `docs/thesis_references.md` Section 11 (LLM non-determinism) listed as TBD | Medium | Fill in references before writing §2.5 and §5.4 |

# System Gap Analysis

**Date:** 2026-03-23
**Branch:** dev
**Purpose:** Structured audit of dead code, test gaps, missing features, architectural observations, and honest scope limitations — sourced from codebase audit, todo.md, and dissertation preparation needs.

---

## 1. Dead Code — Confirmed, Safe to Remove

All items verified present in codebase. Removing these reduces maintenance surface and eliminates misleading code paths.

| Item | Location | Notes |
|------|----------|-------|
| `/api/protected/me` endpoint | `backend/app/main.py:37` | Duplicate of `/api/users/me`; the protected route should be removed |
| `delete_file()` method | `backend/app/core/minio_service.py:124–132` | Never called anywhere; MinIO ILM policy handles object cleanup |
| `User.updated_at` column | `backend/app/db/models/user.py:17` | Never written on update, never read; unused column in both model and DB |
| `TokenData` class | `backend/app/schemas/auth.py:10–12` | Unused; `dependencies.py` extracts claims directly from the JWT without this class |
| `formatDate()` function | `frontend/src/lib/formatters.ts:1` | Defined but never imported by any component |
| Duplicate `formatNum()` in charts | `frontend/src/components/charts/BoxPlotGroup.tsx` + `HistogramGrid.tsx` | Identical local helper in both files; should be moved to `formatters.ts` and imported from there |

**To action:** remove each item in a single cleanup commit; run unit tests after to confirm nothing relied on them.

---

## 2. Test Coverage Gaps

| Gap | Current state | Action |
|-----|--------------|--------|
| `build_dropmasks()` | Tested indirectly via the `/drop-impact` endpoint in integration; no isolated unit test class | Add a `TestBuildDropmasks` class to `test_dq_logic.py` covering: single-column drop, multi-column drop, empty dataset, all-null column |
| `generate_missing_transform_codes()` | No test — this is the user-added hints path in the transform flow | Add a test in `test_llm_enrichment.py` (or a new `test_transform_flow.py`) that inserts a column with `transform_hint` but no `transform_code` and verifies code is generated and valid |

---

## 3. Dissertation Preparation Gaps

These items are needed before writing specific sections. They are not system bugs but missing evidence or documentation.

| ID | Gap | Severity | Needed for | Action |
|----|-----|----------|-----------|--------|
| G2 | No documented performance benchmarks | High | §5.5 Performance & Scalability | Run `test_pipeline.py` on all 7 datasets; record wall-clock time per pipeline stage (profile, score, build\_recommendations, LLM 1, LLM 2, apply); store results in `docs/` |
| G1 | No qualitative comparison vs ydata-profiling / Great Expectations | Medium | §2.1 Data Quality Frameworks | Write a 3-row comparison table (feature coverage, programmatic control, LLM integration); Engineering Decision #7 gives rationale but contains no evidence table |
| G3 | DQ score weights (35/25/25/15) undocumented as heuristics | Medium | §2.1, §5.3 | Explicitly disclose in both sections that these are project-specific heuristics; no standards body specifies numeric dimension weights |
| G7 | `docs/thesis_references.md` Section 11 (LLM non-determinism) marked TBD | Medium | §2.5, §5.4 | Fill in references before writing LLM sections; candidates: Ouyang et al. 2022 (RLHF), Brown et al. 2020 (GPT-3 few-shot), vendor docs on sampling behaviour |
| G4 | LLM non-determinism not formally characterised | Low | §5.4 LLM Enrichment Quality | Run 2–3 identical analysis jobs on the same dataset; record rename and transform\_hint variation; or acknowledge as limitation in §6.2 without measurement |
| G6 | MCAR assumption implicit, never stated explicitly in docs | Medium | §2.2 Missing Data Mechanisms | Add explicit statement: "MCAR is the fallback assumption when MAR tests fail; MCAR is not itself tested — the system assumes randomness when no significant association is detected" |

---

## 4. Missing System Features

From `docs/todo.md` — confirmed as future scope, not in-progress.

| ID | Feature | Current state | Notes |
|----|---------|--------------|-------|
| F4 | LLM reasoning for outlier strategy selection | Default strategy is `keep`; user must manually change to winsorise/remove/cap | LLM could recommend a strategy with domain rationale; requires a targeted prompt and a new `strategy_note` field in the outlier section |


---

## 5. Architectural Observations

Not bugs, but design properties worth addressing before production use.

**`transform_code` not persisted post-generation**
- Generated at transform time by LLM 2; not stored back to the `runs` table or MinIO after generation
- If a run is retried and the LLM API changes (model version, weights update), the regenerated code may differ from the originally reviewed output
- For full reproducibility, `transform_code` per column should be persisted to `runs.recommendations_approved` after generation, so retries use the already-generated code rather than regenerating

**Drop-impact dropmasks have no aligned cleanup policy**
- `build_dropmasks()` stores precomputed bitsets in MinIO at analysis time
- MinIO ILM policy covers raw and curated objects but dropmask objects (`dropmasks/` prefix) are not currently included in the ILM rules
- If raw files are deleted by ILM, the corresponding dropmask objects become orphaned; ILM policy should be extended to cover `dropmasks/` with the same TTL

---

## 6. Known Scope Limitations

Honest statements for dissertation §6.2. These are deliberate constraints, not omissions.

| Limitation | Detail |
|-----------|--------|
| CSV only, max 200MB | No support for Parquet, Excel, or JSON inputs; single file per run |
| Within-column analysis only | Cross-column constraint checks (e.g. `start_date < end_date`, `quantity × unit_price = total`) are not detected |
| Validity = type conformance only | Numeric range violations (e.g. `age = 300`) are not flagged unless they cross the numeric sentinel threshold (3×IQR fence, count ≥ 5) |
| MCAR assumed as default | MCAR is not formally tested; it is the fallback assumption when MAR association tests find no significant relationship |
| MNAR undetectable | MNAR missingness depends on the unobserved value; no statistical test can confirm it; handled by LLM note + user override only |
| Exact deduplication only | Fuzzy/approximate duplicate detection (name variants, address formats) is out of scope |
| No user study | Evaluation is quantitative only (scores, correctness, performance); no usability study was conducted |
| LLM non-determinism | Llama 3.3 70B at temperature=0 is not fully deterministic on distributed GPU; rename and transform suggestions may vary across identical runs; documented as expected, not a defect |

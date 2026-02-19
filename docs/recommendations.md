# Data Quality Recommendations

How the pipeline generates, structures, and uses recommendations — and why each decision was made.

---

## Purpose

When a user uploads a CSV, the system produces a **recommendations JSON** that tells the transform step what to do with the data. The recommendations answer three questions:

1. What does this data look like? (schema inference)
2. What's wrong with it? (missing values, duplicates, type inconsistencies)
3. How should it be fixed? (fill strategy, deduplication, normalization)

The goal is to make the user's job as a data engineer easier: they review the JSON, tweak anything they disagree with, and the system applies the transforms — reproducibly, without manual scripting.

---

## Pipeline Position

```
parse_csv → profile_dataframe → score_profile → build_recommendations
                                                         ↓
                                             Stored in runs.recommendations_generated (JSONB)
                                                         ↓
                                             User reviews via GET /api/files/{id}/recommendations
                                                         ↓
                                             User approves/edits via PUT (stored in recommendations_approved)
                                                         ↓
                                             Transform flow applies approved recommendations
```

Recommendations are generated once, stored in PostgreSQL, and never regenerated unless the user re-runs the DQ flow. The approved copy is kept separately from the generated copy so the system can always diff them.

---

## How Recommendations Are Built

### Step 1 — Parse

`parse_csv(stream)` reads the CSV via pandas with `on_bad_lines` wired to a counter. Malformed rows (wrong field count) are skipped rather than raising. The count is passed downstream so it appears in the profile.

### Step 2 — Profile

`profile_dataframe(df, malformed_rows)` produces objective facts — no strategy decisions, no LLM. It returns:

**Dataset-level stats**

| Field | What it measures |
|---|---|
| `total_rows`, `total_columns`, `total_cells` | Shape |
| `missing_cells` | NaN count across all cells |
| `duplicate_rows` | Exact full-row duplicates |
| `malformed_rows` | Rows skipped during CSV parse |
| `invalid_cells` | Cells in object columns that don't conform to the column's dominant type |

**DQ score inputs** (see [DQ Score](#dq-score))

| Field | Calculation |
|---|---|
| `completeness` | `(1 - missing_cells / total_cells) * 100` |
| `uniqueness` | `(1 - duplicate_rows / total_rows) * 100` |
| `validity` | `(type_conforming_cells / total_cells) * 100` |
| `consistency` | `(pattern_matching_cells / total_cells) * 100` |

**Per-column profiles** (`column_profiles`)

Each column gets a profile dict. The structure depends on the detected type:

Numeric / date columns:
```json
{
  "null_count": 12,
  "null_pct": 3.2,
  "stats": { "min": 20000, "max": 120000, "mean": 65000, "median": 60000, "std": 18500 },
  "outliers": { "count": 4, "method": "IQR" },
  "detected_type": "numeric",
  "pandas_dtype": "float64"
}
```

String / bool columns:
```json
{
  "null_count": 0,
  "null_pct": 0.0,
  "unique_count": 5,
  "cardinality_pct": 4.2,
  "top_values": { "Engineering": 40, "Sales": 30, "HR": 15, "Finance": 10, "Legal": 5 },
  "pattern": null,
  "detected_type": "string",
  "pandas_dtype": "object"
}
```

### Step 3 — Column type detection

`_detect_column_type(series)` runs on every column to assign a semantic type used throughout profiling and recommendations.

Priority order:
1. If pandas already parsed as int/float → `"numeric"`
2. If pandas parsed as bool → `"bool"`
3. If pandas parsed as datetime → `"date"`
4. Object columns: check for bool-like values (`"true"/"false"/True/False`) → `"bool"`
5. Try numeric coercion — if ≥80% of non-null values parse → `"numeric"`
6. Try datetime coercion — if ≥80% of non-null values parse → `"date"`
7. Otherwise → `"string"`

The 80% threshold is intentional: real-world columns often have a few dirty values. A column that's 95% salaries and 5% `"N/A"` strings should still be treated as numeric. The invalid 5% is captured in `validity`.

Bool-like detection runs before numeric coercion because `pd.to_numeric` converts `True` → 1 and `False` → 0, which would misclassify a bool-with-nulls object column as numeric.

### Step 4 — Outlier detection

Only run when a numeric column has ≥30 non-null values. Below that threshold the IQR method is statistically unreliable. Outliers are **advisory only** — they are not penalised in the DQ score and the recommendations do not prescribe a fix. The count is surfaced so the user can decide.

### Step 5 — Pattern detection

`_PATTERNS` is a dict of compiled regexes for common structured formats:

| Pattern | Used for |
|---|---|
| `email` | Email addresses |
| `date_iso` | `YYYY-MM-DD` strings |
| `phone` | International phone numbers |
| `url` | HTTP/HTTPS URLs |
| `postcode_uk` | UK postcodes |
| `uuid` | UUIDs |

**Ordering matters:** `date_iso` is checked before `phone` because ISO dates (`2021-03-15`) match the phone regex (digits + hyphens). First match wins.

A pattern is considered dominant if >50% of non-null values match. This feeds both the `consistency` DQ metric and the `_fill_strategy` logic.

### Step 6 — Score

`score_profile(profile)` computes the DQ score as a weighted average:

| Component | Weight | Rationale |
|---|---|---|
| Completeness | 35% | Missing data is the most common and costly data quality problem |
| Uniqueness | 25% | Duplicate rows silently inflate counts and skew aggregates |
| Validity | 25% | Type violations cause downstream pipeline failures |
| Consistency | 15% | Format inconsistency matters but is often recoverable |

Score is rounded to 2 decimal places and stored as `dq_score_before` in the run record.

### Step 7 — Build recommendations

`build_recommendations(profile, dq_score)` converts the profiled facts into actionable instructions.

---

## Recommendations JSON Schema

```json
{
  "schema": {
    "<column>": {
      "type": "int | float | string | date | bool",
      "nullable": true
    }
  },
  "missing_values": {
    "<column>": {
      "strategy": "median | mean | mode | fill | drop_row",
      "value": null
    }
  },
  "duplicates": {
    "strategy": "drop",
    "subset": [],
    "keep": "first"
  },
  "normalization": {
    "columns": ["<column>"]
  },
  "custom_transforms": [],
  "_metadata": {
    "generated_at": "2024-01-15T10:30:00",
    "dq_score": 72.5,
    "issues_found": {
      "missing": 120,
      "duplicates": 8,
      "type_mismatches": 14
    }
  }
}
```

`_metadata` is prefixed with `_` to signal that it is informational — the transform step ignores it. `custom_transforms` is always an empty list from the code path; it exists so users can append LLM-generated or hand-written transforms before approval.

---

## Schema Type Inference

`_schema_type(detected_type, col_profile)` maps a column's detected type to a schema type string:

| Detected type | Schema type | Notes |
|---|---|---|
| `"bool"` | `"bool"` | Direct pass-through |
| `"date"` | `"date"` | Direct pass-through |
| `"numeric"` with float pandas dtype | `"float"` | pandas_dtype checked first — whole-number floats (e.g. salary = 75000.0) must not be called int |
| `"numeric"` with whole-number min/max | `"int"` | Distinguish int vs float from the stats |
| `"numeric"` otherwise | `"float"` | Default for numerics |
| `"string"` with `date_iso` pattern | `"date"` | ISO date strings stored as object columns |
| `"string"` otherwise | `"string"` | |

The pandas_dtype check takes priority over the min/max heuristic. Without it, a salary column like `[25000.0, 75000.0, 50000.0]` — all whole numbers — would be classified as int, which is semantically wrong.

---

## Fill Strategy Rules

`_fill_strategy(detected_type, col_profile)` picks a default imputation strategy. All rules are deterministic — no LLM.

| Condition | Strategy | Rationale |
|---|---|---|
| `numeric` | `median` | Robust to outliers; mean is sensitive to skew |
| `bool` | `mode` | Fill with the most common true/false value |
| `date` | `drop_row` | Dates can't be imputed without domain knowledge |
| `string` with pattern | `drop_row` | Can't invent a valid email or phone number |
| `string`, cardinality <10% | `mode` | Low cardinality = likely a category; fill with most common |
| `string`, cardinality ≥10% | `drop_row` | High cardinality = likely an ID or free text; imputation would fabricate data |

The 10% cardinality threshold is a heuristic. A column with 3 unique values in 100 rows (3%) is clearly categorical. A column with 80 unique values in 100 rows (80%) is clearly an identifier or name.

---

## Normalization Advisory

Normalization is suggested for numeric columns where:

```
abs(max) / abs(min) > 100
```

This means the values span more than two orders of magnitude (e.g., revenue in dollars alongside a percentage column). Normalization is **advisory** — the recommendations include the column name but the transform step requires explicit user approval before applying min-max or z-score scaling.

The condition requires `min > 0` to avoid division by zero and to exclude columns that cross zero (where ratio-based normalization doesn't make sense).

---

## Validity vs Consistency

These two metrics are often confused:

**Validity** — is a cell the right *type* for this column?
A salary column with a cell containing `"N/A"` fails validity. The value is a string in what should be a numeric column.

**Consistency** — is a cell the right *format* for this column?
An email column where 90% of values are `user@example.com` and 10% are `user[at]example.com` passes validity (all strings) but fails consistency (format mismatch).

Both are penalised in the DQ score. Consistency has lower weight (15%) because it's often a presentation problem, not a semantic one.

---

## Code vs LLM Boundary

Everything in `dq_logic.py` is deterministic and LLM-free. This is a deliberate design choice:

- **Reproducibility**: the same CSV always produces the same profile and the same base recommendations.
- **Testability**: all logic has unit tests with exact assertions. LLM responses can't be unit tested the same way.
- **Trust**: users can understand and verify the rules. A "median fill" recommendation is self-explanatory.

The LLM layer (Phase 3, not yet implemented) sits between `build_recommendations` and the user. It will receive the `column_profiles` and may enhance the recommendations with semantic context — for example, recognising that a column named `employee_id` should use `drop_row` even if it has low cardinality. The code-generated recommendations are always the starting point; the LLM can only refine them.

---

## What the User Can Edit

The user receives the generated JSON and can change any field before approving it. Common edits:

- Change `"strategy": "drop_row"` to `"strategy": "fill"` with a custom `"value"`
- Remove a column from `normalization.columns`
- Add an entry to `custom_transforms` with a natural-language description
- Override a `schema.type` that was inferred incorrectly

The approved JSON is stored separately (`recommendations_approved`) and is the input to the transform flow. The original generated JSON is preserved for auditing and future ML training (to learn which defaults users change most often).

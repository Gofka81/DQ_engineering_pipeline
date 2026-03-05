# Data Quality Recommendations

This document defines every recommendation type the pipeline produces, the business rules behind each one, and which decisions are made by code vs the LLM.

---

## Framework — DQ Dimensions

The pipeline is structured around the six practitioner dimensions of data quality from **DAMA DMBOK** (Data Management Body of Knowledge) — the standard used by working data engineers. These six dimensions overlap with the "inherent" data quality characteristics defined in **ISO/IEC 25012**, though ISO/IEC 25012 defines 15 characteristics in total; we implement four of them.

> **Note on ISO 8000:** ISO 8000 governs data quality for supply-chain master data (GDSN product records, asset registers). It is not applicable to general-purpose CSV profiling. The earlier citation of "ISO 8000 alignment" in this project's documentation was inaccurate and has been corrected.

> **Scope disclaimer:** This pipeline adopts practitioner interpretations of DAMA DMBOK dimensions. It is not ISO-certified and does not implement full standard compliance. Every threshold (50% null, IQR 1.5×, cardinality 10%, 80% MAR correlation) is a configurable heuristic, not a statistically universal rule. Rules labelled "deterministic" are reproducible code rules; rules labelled "heuristic" are calibrated defaults that may not generalise to all domains.

Each dimension maps to one or more recommendation types. The "Implementation scope" column is an honest statement of what the pipeline currently checks.

| Dimension | What it measures | Recommendation types | Implementation scope |
|---|---|---|---|
| **Completeness** | Missing / null values | Missing values strategies | Null ratio at value level only. Does not check population completeness (missing rows) or column-level completeness against a schema. |
| **Validity** | Values that violate domain rules | Sentinel detection, type recast, outliers | Type mismatches on `object`-dtype columns only. Native `int`/`float` columns are not range-validated. Sentinel detection for string columns only (numeric sentinels like `-999` are a known gap). |
| **Uniqueness** | Duplicate records | Duplicate handling | Exact row duplicates only. Fuzzy / near-duplicate matching is a known gap. |
| **Consistency** | Format regularity within a column | Type recast, date standardisation | Within-column format patterns (mixed date formats, email pattern matching). Cross-column rule validation ("end_date > start_date") is a known gap. |
| **Accuracy** | Values match reality | (none — requires ground truth) | Out of scope. |
| **Timeliness** | Data staleness | (none) | Out of scope. |

---

## 1. Completeness — Missing Values

Missing data is classified by its mechanism before a strategy is chosen. The mechanism determines whether imputation is safe or whether it would destroy a signal in the data.

### Missing Data Mechanisms (MCAR / MAR / MNAR)

| Mechanism | What it means | How we detect it | Correct treatment |
|---|---|---|---|
| **MCAR** — Missing Completely At Random | Nulls have no pattern. The scale ran out of batteries. | Little's test / no correlation found | Safe to impute |
| **MAR** — Missing At Random | Nulls correlate with an *observed* column. ATM rows → no merchant name. | Statistical correlation check (see below) | `leave_null` — imputing destroys the signal |
| **MNAR** — Missing Not At Random | Nulls correlate with an *unobserved* value. High earners skip salary field. | Cannot detect from data alone — needs domain reasoning | `leave_null` — inferred by LLM from column name + context |

**MAR detection (code):** For each nullable column, the pipeline checks whether its null rows cluster on a specific value in another column (≥80% of nulls appear when column B = value V, and V is not the majority value of B overall). If detected, `leave_null` is applied directly in the baseline — this is a statistical fact, not an LLM decision.

**MNAR detection (LLM):** When a column is >50% null and its name suggests a sparse-by-design attribute (`adv_evt_dt`, `resolved_at`, `conv_dt`, `incident_dt`), the LLM overrides `drop_column` to `leave_null` using semantic reasoning about the column's domain role.

### Decision Precedence Hierarchy

When multiple rules could apply to a column, this is the explicit priority order — higher rows win:

| Priority | Rule | Owner | Can be overridden by |
|---|---|---|---|
| 1 | MAR detection — nulls cluster on a specific value in another column → `leave_null` | Code (todo 1.3 — not yet in baseline) | User only |
| 2 | ≥50% null → `drop_column` | Code | LLM (MNAR only — event/optional columns), User |
| 3 | Type-based rules (ID/pattern → `drop_row`, numeric → `median`, etc.) | Code | LLM, User |
| 4 | LLM semantic override (MNAR `leave_null`, `rename_to`) | LLM | User |
| 5 | User edit of recommendations JSON | User | Nothing |

**What the LLM can and cannot do:**
- The LLM **can** override `drop_column` → `leave_null` for event-date or optional-attribute columns (MNAR case). This is intentional.
- The LLM **cannot** invent new strategies — the validator enforces a fixed `_VALID_STRATEGIES` set.
- The LLM **cannot** change `_metadata`, `duplicates`, or `custom_transforms` — these are always taken from the baseline.
- The LLM **cannot** override a column that has zero nulls with an imputation strategy — a post-processing guard strips this.

---

### Strategy Selection Rules (applied in order)

| Condition | Strategy | Rationale |
|---|---|---|
| Nulls correlate with another column's value (MAR) | `leave_null` | Nulls carry structural information — imputing removes it |
| Column is ≥50% null AND not MAR | `drop_column` | More than half the column is missing — imputation would fabricate the majority of the data |
| ID / key column (`*_id`, `*_key`, `*_code`, high cardinality) | `drop_row` | Identifiers are unique — any imputed value is fabricated |
| Pattern column (email, phone, UUID, URL — detected by regex) | `drop_row` | Structured values cannot be invented |
| Numeric column — skewed (IQR outliers present, or \|mean − median\| / std > 0.15) | `median` | Median is robust to outliers; mean is pulled by extreme values |
| Numeric column — symmetric (no outliers, mean ≈ median) | `mean` | Mean is the efficient estimator when distribution is symmetric |
| Bool column | `mode` | Fill with the more common true/false |
| Date column (non-event, no MAR signal) | `drop_row` | Cannot invent a date without domain knowledge |
| String column — low cardinality (<10% unique values) | `mode` | Likely categorical — fill with most frequent value |
| String column — high cardinality (≥10% unique values) | `drop_row` | Likely an ID or free-text name — imputation fabricates data |

The `drop_column` rule (≥50%) is checked first and overrides type-based rules.

### Available Strategies (full list)

| Strategy | Auto-recommended | Effect |
|---|---|---|
| `median` | Yes — symmetric numeric | Fill nulls with column median |
| `mean` | Yes — skewed numeric | Fill nulls with column mean |
| `mode` | Yes — bool, low-cardinality string | Fill nulls with the most frequent value |
| `fill` | User / LLM only | Fill nulls with a literal value (e.g. `0`, `"unknown"`) |
| `drop_row` | Yes — ID, pattern, date, high-cardinality string | Drop the row where this column is null or invalid |
| `drop_column` | Yes — ≥50% missing, not MAR | Remove the entire column |
| `leave_null` | Code (MAR), LLM (MNAR), or user | Keep nulls as-is — no fill, no rows dropped |

---

## 2. Validity — Sentinel Values

**Sentinel values** are special values stored in a column to represent "no data" or "not applicable" rather than using a proper null. Examples: `-999` in a temperature column, `"N/A"` or `"unknown"` in a string column, `99` in an age column meaning "not recorded".

These are **validity issues, not completeness issues** — the cell is not null, but the value is invalid for the column's domain. Treating them as valid data biases statistics and corrupts imputation.

### Detection

The profiler detects sentinel patterns and reports them as `invalid_count` per column:
- String sentinels: `"N/A"`, `"n/a"`, `"NA"`, `"none"`, `"null"`, `"unknown"`, `"undefined"`, `"-"`, `"?"`, `""`
- Numeric sentinels: values that are implausibly extreme for the column's distribution (e.g. `-999`, `9999`, `99`) and appear with suspiciously high frequency
- Sample rows surfaced to the LLM so it can spot domain-specific patterns

### Recommendation

When `invalid_count > 0` for a column, the recommendation flags the column with a `warning` describing the sentinel pattern. The user decides whether to convert sentinels to null (then impute) or filter the rows. **Automatic conversion is not applied** — sentinel replacement is a destructive operation that requires user confirmation.

---

## 3. Validity — Type Recast and Date Standardisation

For every column: infer the correct type from the actual values and recommend a cast if the stored type differs.

| Rule | Inferred type |
|---|---|
| All non-null values are whole numbers | `int` |
| Numeric values with decimals present | `float` |
| ≥80% of string values parse as ISO 8601 date or common date format | `date` |
| Values are only true/false variants (`true`, `false`, `1`, `0`, `yes`, `no`) | `bool` |
| Everything else | `string` |
| `nullable: true` | Column has any nulls or invalid cells |

**Date standardisation:** When a column is recast to `date`, the transform pipeline normalises all values to ISO 8601 format (`YYYY-MM-DD`). Columns storing dates as `"2003/01/15"`, `"15-01-2003"`, or `"Jan 15, 2003"` are all standardised to `"2003-01-15"` on transform.

---

## 4. Validity — Outliers

Outliers are values that are statistically extreme relative to the column's distribution. They are profiled per column and an `outliers` section is generated in the recommendations. The default strategy is `keep` — the user must explicitly change it to `winsorise`, `remove`, or `cap` to apply treatment. The selected strategy is then applied during the transform step.

**Scope constraint:** IQR detection is univariate — it evaluates each column independently. It does not detect multivariate anomalies (e.g. an unusual combination of age + salary). It is not a fraud or anomaly detection method.

**Minimum sample:** IQR bounds are only computed when a column has ≥ 30 non-null values. For smaller columns the outlier count is 0 and no `outliers` entry is generated — this is a known limitation for small datasets.

### Detection Methods

| Method | When to use | Threshold | Implemented |
|---|---|---|---|
| **IQR (Interquartile Range)** | Skewed distributions (most real-world data) | Below Q1 − 1.5×IQR or above Q3 + 1.5×IQR | ✓ |
| **Z-score** | Normally distributed data | \|z\| > 3 (i.e. >3 standard deviations from mean) | Not yet |

The pipeline uses IQR by default (more robust for skewed data). The outlier count reported in the profile is IQR-based.

### Treatment Options (user selects)

| Strategy | Effect | When appropriate |
|---|---|---|
| `keep` | No action — outliers remain | When outliers are real rare events (fraud, sensor spikes) |
| `winsorise` | Cap at the nearest non-outlier boundary (Q3 + 1.5×IQR or Q1 − 1.5×IQR) | Preserves row count; reduces extreme influence on models |
| `remove` | Drop rows where the column value is an outlier | Only when outliers are confirmed data entry errors |
| `cap` | Cap at user-defined min/max | When domain bounds are known (e.g. human age: 0–120) |

**Domain violations** (age = -1, temperature = -999) are a **validity** issue (sentinel value), not a statistical outlier — see section 2 above.

---

## 5. Uniqueness — Duplicates

### Row-Level (Exact Duplicates)

Exact duplicate rows (every column identical) are always unintentional in a clean dataset. They arise from import errors, double-submission, or ETL bugs.

| Option | Effect |
|---|---|
| `drop` — keep `first` | Keep the first occurrence, drop subsequent |
| `drop` — keep `last` | Keep the last occurrence, drop earlier ones |
| `ignore` | User confirms the duplicates are intentional (e.g. log table with repeated events) |

`subset` can be specified (list of columns to check) — duplicate on key columns only, not all columns.

The recommendation defaults to `drop / keep: first` when any exact duplicates are detected.

### Fuzzy / Near-Duplicates

Records representing the same entity but with slight variation ("John Doe" vs "Jon Doe", "123 Main St" vs "123 Main Street") are **not detected automatically** — fuzzy matching requires domain-specific similarity thresholds and identity resolution logic. This is out of scope for the current pipeline but noted as a known gap.

---

## 6. Usability — Normalization / Standardisation

Normalisation scales numeric column values so they are comparable across different ranges. It is relevant beyond machine learning:

- **ML models**: distance-based algorithms (k-means, SVM, KNN) are biased toward high-range features without scaling
- **Cross-feature comparison**: salary (0–500K) and age (0–100) cannot be aggregated or compared without scaling
- **Clustering and PCA**: assume features on comparable scales
- **Multi-source data**: standardise measurements from different instruments or collection sites

### When recommended

For numeric columns where `col_max_abs > 100` AND the column has a non-zero range: suggest normalisation. `col_max_abs = max(|min|, |max|)` — handles zero-min and negative-min columns correctly (previous ratio approach `max/min > 100` failed for those).

### Methods

| Method | Effect | When to prefer | Implemented |
|---|---|---|---|
| **Min-max** [0, 1] | Scales to fixed range | When distribution bounds are known and outliers are few | ✓ |
| **Z-score** (standardisation) | Mean=0, std=1 | When outliers are present or distribution is unknown | Not yet |

**Current implementation:** Only min-max is applied. The `normalize: true` field triggers min-max scaling. Z-score is documented for future implementation (todo 4.4).

Advisory only — the user reviews the recommendation in the JSON and may reject it by setting `normalize: false`.

---

## 7. Metadata — Column Renaming

Column renaming is a **metadata quality** improvement. It does not change data values but makes the dataset more readable and self-documenting.

The LLM identifies three rename scenarios:

| Scenario | Example | Signal |
|---|---|---|
| **Abbreviation** | `sal` → `salary`, `dept_cd` → `department_code` | Column name is short and cryptic, clearly abbreviates a known word |
| **Semantic clarification** | `name` (containing "John Smith") → `full_name` | Sample values reveal the column's true meaning |
| **Incorrect name** | Column called `age` containing values like `2003-01-15` | Type detection and sample values contradict the column name — treated as type recast first, rename second |

Rename suggestions are **user-facing proposals**, not enforced. The user reviews them in the recommendations JSON and may keep or discard each one. The no-op guard (stripping `rename_to` when the new name equals the original) is the only hard enforcement.

---

## 8. LLM Enrichment — Scope and Boundary

After the code generates the baseline recommendations, an LLM (Groq — llama-3.3-70b-versatile) reviews column profiles and sample rows to improve them.

### What the LLM decides (semantic decisions only)

| Task | Why LLM | Why not code |
|---|---|---|
| MNAR `leave_null` | Column name + domain context required — cannot detect from statistics | Cannot determine from data patterns alone |
| `rename_to` suggestions | Requires reading sample values and understanding semantic meaning | No deterministic mapping from abbreviation to full name |
| `note` — plain-English explanation | Requires natural language generation | Not a code concern |

### What code decides (deterministic decisions)

| Task | Why code |
|---|---|
| MAR `leave_null` (correlated nulls) | Statistical fact — correlate null rows with other column values |
| Median vs mean selection | Computable from skewness statistics in the profile |
| Mode selection for categoricals | Computable from cardinality_pct |
| Drop row for ID/pattern columns | Column name pattern + cardinality + regex detection |
| Drop column threshold (≥50% null) | Arithmetic |
| Type inference | Syntactic parsing |
| Normalization flag | Range comparison |
| Sentinel detection | Pattern matching |

### Post-processing guards (applied after LLM response)

Even within the LLM's scope, four deterministic guards are applied to catch predictable failure modes:

1. **Strip echoed keys** — remove any key the LLM returned identical to the baseline
2. **Strip `missing_values` when baseline has none** — zero-null columns do not need imputation
3. **Strip no-op renames** — `rename_to` equal to the column name is meaningless
4. **Drop note-only columns** — a column with only a `note` and no substantive change is noise

### Fallback

If `LLM_API_KEY` is not set, the `groq` package is not installed, or all 3 retry attempts fail, the code-generated baseline is returned unchanged.

---

## Complete Example JSON

```json
{
  "columns": {
    "customer_id": {
      "type":           "string",
      "nullable":       false,
      "missing_values": null,
      "normalize":      false,
      "warning":        null,
      "note":           null
    },
    "age": {
      "type":           "int",
      "nullable":       true,
      "missing_values": { "strategy": "median", "value": 34 },
      "normalize":      false,
      "warning":        null,
      "note":           null
    },
    "salary": {
      "type":           "float",
      "nullable":       true,
      "missing_values": { "strategy": "mean", "value": null },
      "normalize":      true,
      "warning":        null,
      "note":           "Distribution is symmetric — mean is appropriate; consider z-score normalization for ML use."
    },
    "joined_at": {
      "type":           "date",
      "nullable":       true,
      "missing_values": { "strategy": "drop_row", "value": null },
      "normalize":      false,
      "warning":        "drop_row will remove up to 3 rows (1.5% of dataset) where 'joined_at' is null",
      "note":           null
    },
    "is_active": {
      "type":           "bool",
      "nullable":       true,
      "missing_values": { "strategy": "fill", "value": false },
      "normalize":      false,
      "warning":        null,
      "note":           "Defaulting to inactive — safer assumption for records with no known activity"
    },
    "email": {
      "type":           "string",
      "nullable":       true,
      "missing_values": { "strategy": "drop_row", "value": null },
      "normalize":      false,
      "warning":        "drop_row will remove up to 8 rows (4.0% of dataset) where 'email' is null or invalid",
      "note":           null
    },
    "department": {
      "type":           "string",
      "nullable":       true,
      "missing_values": { "strategy": "mode", "value": "Engineering" },
      "normalize":      false,
      "warning":        null,
      "note":           null
    },
    "adv_evt_dt": {
      "type":           "date",
      "nullable":       true,
      "missing_values": { "strategy": "leave_null", "value": null },
      "normalize":      false,
      "warning":        null,
      "note":           "74% null — adverse event dates are null for patients who had no adverse event. Nulls are intentional and clinically meaningful."
    },
    "merchant_nm": {
      "type":           "string",
      "nullable":       true,
      "missing_values": { "strategy": "leave_null", "value": null },
      "normalize":      false,
      "warning":        null,
      "note":           "Nulls correlate with txn_typ=atm (100% of nulls). ATM transactions have no merchant — structural null."
    },
    "sparse_col": {
      "type":           "string",
      "nullable":       true,
      "missing_values": { "strategy": "drop_column", "value": null },
      "normalize":      false,
      "warning":        "drop_column will remove the entire 'sparse_col' column (62% of values are missing)",
      "note":           null
    }
  },
  "duplicates": {
    "strategy": "drop",
    "subset":   [],
    "keep":     "first"
  },
  "outliers": {
    "salary": {
      "count":    3,
      "method":   "iqr",
      "lower":    42000.0,
      "upper":    158000.0,
      "strategy": "winsorise"
    }
  },
  "custom_transforms": [],
  "_metadata": {
    "generated_at": "2026-02-28T12:00:00.000000",
    "dq_score":     67.4,
    "issues_found": {
      "missing":         42,
      "duplicates":       5,
      "type_mismatches":  3,
      "invalid_values":   8
    }
  }
}
```

### Field reference

**Per-column fields** (`columns.<name>`)

| Field | Values | Set by |
|---|---|---|
| `type` | `int` `float` `string` `date` `bool` | Code |
| `nullable` | `true` if column has any nulls or invalid cells | Code |
| `missing_values` | `null` if no action needed, otherwise `{strategy, value}` | Code (MAR + threshold rules), LLM (MNAR), user |
| `normalize` | `true` if scaling is suggested | Code |
| `warning` | Human-readable impact message for `drop_row` / `drop_column` | Code |
| `note` | Explanation of why this recommendation was made or changed | LLM / user |

**`missing_values.strategy` options**

| Strategy | Auto-recommended | Effect |
|---|---|---|
| `median` | Yes — skewed numeric (outliers present) | Fill nulls with column median |
| `mean` | Yes — symmetric numeric (no outliers) | Fill nulls with column mean |
| `mode` | Yes — bool, low-cardinality string | Fill nulls with most frequent value |
| `fill` | User / LLM only | Fill nulls with a literal `value` |
| `drop_row` | Yes — ID, pattern, date, high-cardinality string | Drop rows where this column is null or invalid |
| `drop_column` | Yes — columns ≥50% missing and not MAR | Remove the entire column |
| `leave_null` | Code (MAR), LLM (MNAR), or user | Keep nulls as-is — no fill, no rows dropped |

**`outliers.<col>` fields** *(advisory — not yet applied automatically)*

| Field | Values |
|---|---|
| `count` | Number of outliers detected |
| `method` | `iqr` (default) or `zscore` |
| `lower` | Lower IQR bound (Q1 − 1.5×IQR) — used by `winsorise` and `remove` |
| `upper` | Upper IQR bound (Q3 + 1.5×IQR) — used by `winsorise` and `remove` |
| `strategy` | `keep` `winsorise` `remove` `cap` |

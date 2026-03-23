# DQ Pipeline — TODO

Derived from reading all Python files against `docs/recommendations.md` (DAMA DMBOK framework).

---

## Priority 1 — Logic Correctness (`dq_logic.py`)

These are correctness bugs — the baseline recommendations are wrong in cases the LLM cannot fix.

### 1.1 Mean vs Median Based on Skewness

**File:** `prefect/flows/dq_logic.py:552` — `_fill_strategy()`

**Problem:** Always returns `"median"` for numeric. The research consensus (and our own
`docs/recommendations.md` Section 1) is: use `mean` for symmetric distributions (|mean−median|/std ≤ 0.15),
`median` when skewed. The stats (`mean`, `median`, `std`) are already in `col_profile["stats"]`.

**Fix:** Check BOTH skewness indicators inside `_fill_strategy()` — recommendations.md specifies either condition is sufficient:
```python
if detected_type == "numeric":
    stats        = col_profile.get("stats", {})
    mean         = stats.get("mean", 0)
    median       = stats.get("median", 0)
    std          = stats.get("std", 1) or 1
    has_outliers = col_profile.get("outliers", {}).get("count", 0) > 0
    is_skewed    = abs(mean - median) / std > 0.15
    return "median" if (has_outliers or is_skewed) else "mean"
```

---

### 1.2 `drop_column` — Single Threshold at 50% (no change needed, docs corrected)

**File:** `prefect/flows/dq_logic.py:565` — `_fill_strategy()`

**Status:** The code has exactly ONE threshold (`>= 50%`) and this is correct. An earlier version of this
todo and of `docs/recommendations.md` described two thresholds (50% and 80%) as two separate policies —
that was inaccurate. The 80% figure comes from academic references (van Buuren 2018; Eekhout et al.) and
represents "extreme missingness" where even the 50% rule is obviously met; it is not a separate decision
point that changes the outcome. `docs/recommendations.md` has been corrected to show one rule.

**No code change required.** After todo 1.3 (MAR override), the 50% rule correctly implements the only threshold.

---

### 1.3 MAR Detection → `leave_null` in Baseline (move `_correlated_nulls()`)

**Files:**
- `prefect/flows/llm_enrichment.py:176` — `_correlated_nulls()` defined here
- `prefect/flows/llm_enrichment.py:295` — called inside `_build_llm_profile()` for the CORRELATED NULLS section
- `prefect/flows/dq_logic.py:289` — `build_recommendations()` has no MAR awareness

**Problem:** MAR detection (nulls in column A cluster on a specific value of column B) is computed in
`llm_enrichment.py` only to add a text section to the LLM prompt. The LLM proved unreliable at acting
on it (Finance ATM case: `merchant_nm` null when `txn_typ=ATM` at 100%, LLM still returned `mode`).
MAR is a statistical fact — it belongs in deterministic code.

**Fix (three steps):**
1. Move `_correlated_nulls()` from `llm_enrichment.py` to `dq_logic.py`
2. Call it at the start of `build_recommendations()`, build a `mar_cols` set
3. For any column in `mar_cols`, override strategy to `"leave_null"` regardless of what `_fill_strategy()` returned
4. (Cleanup) Remove `_correlated_nulls()` call and CORRELATED NULLS section from `_build_llm_profile()`
5. (Cleanup) Remove `_correlated_nulls()` function from `llm_enrichment.py`

```python
# In build_recommendations(), before the per-column loop:
mar_cols = {c["column"] for c in _correlated_nulls(df)}

# In the per-column loop, after calling _fill_strategy():
if col in mar_cols:
    strategy = "leave_null"
```

Note: `build_recommendations()` currently only receives `profile` and `dq_score` — it will need `df`
passed in as a third argument. Update `dq_flow.py` to pass `df` through.

---

### 1.4 Outlier IQR Bounds Missing from Profile

**File:** `prefect/flows/dq_logic.py:447` — `_profile_numeric_column()`

**Problem:** IQR outlier detection runs and produces `outlier_count`, but the actual bounds
(`lower`, `upper`) are not stored. `apply_recommendations()` would need the bounds to winsorise or cap.

**Fix:** Return bounds in the profile:
```python
"outliers": {
    "count": outlier_count,
    "method": "iqr",
    "lower": round(float(lower), 4) if len(numeric_vals) >= 30 else None,
    "upper": round(float(upper), 4) if len(numeric_vals) >= 30 else None,
}
```

---

### 1.5 Outlier Recommendations Missing from `build_recommendations()`

**File:** `prefect/flows/dq_logic.py:289` — `build_recommendations()`

**Problem:** Outlier counts are in the profile per numeric column but `build_recommendations()` doesn't
generate an `outliers` section. The schema (`file.py`) and transform (`apply_recommendations()`) don't
handle outliers yet either.

**Fix:** After building `columns`, add an `outliers` top-level dict:
```python
recommendations["outliers"] = {}
for col, cp in column_profiles.items():
    if cp.get("detected_type") != "numeric":
        continue
    outlier_info = cp.get("outliers", {})
    if outlier_info.get("count", 0) > 0:
        recommendations["outliers"][col] = {
            "strategy": "keep",   # default — user can change to winsorise/remove/cap
            "method": "iqr",
            "lower": outlier_info.get("lower"),
            "upper": outlier_info.get("upper"),
        }
```

---

### 1.6 Normalize Suggestion Logic Broken

**File:** `prefect/flows/dq_logic.py:330-334` — `build_recommendations()`

**Problem:** The condition for suggesting normalization almost never fires in practice:
```python
col_min = abs(stats.get("min", 0))
col_max = abs(stats.get("max", 0))
if col_min > 0 and col_max / col_min > 100:
    normalize = True
```

Three concrete failures:
- Column `0 → 1,000,000` (e.g. revenue): `abs(0) = 0` → condition `col_min > 0` is False → **no normalize suggestion**
- Column `10,000 → 500,000` (salary): ratio = 50, below 100 → **no normalize suggestion**
- Column `-1000 → 10`: `abs(-1000)=1000`, `abs(10)=10`, ratio = 0.01 → **no normalize suggestion**, despite spanning 1010 units

**Fix:** Base the check on the actual value range, not the ratio of abs(min) to abs(max):
```python
col_range = stats.get("max", 0) - stats.get("min", 0)
col_max_abs = max(abs(stats.get("max", 0)), abs(stats.get("min", 0)))
if col_range > 0 and col_max_abs > 100:
    normalize = True
```

This correctly suggests normalization for salary (range=490,000), revenue (range=1,000,000), but not for age (range=50, max_abs=75).

**Also note:** `apply_recommendations()` only implements min-max normalization. Z-score is not implemented anywhere. The schema and docs should not describe it as an option until it is.

---

### 1.7 Warning Logic Only Fires for Drop Strategies

**File:** `prefect/flows/dq_logic.py:339` — `build_recommendations()`

**Problem:** Warnings are only generated when strategy is `drop_row` or `drop_column`. A column
with 70% nulls and strategy `leave_null` (MAR) gets no warning — the user sees it silently with
no explanation. Same for `mode` on a high-null column.

**Current condition:**
```python
if needs_fill and strategy in ("drop_row", "drop_column"):
```

**Fix:** Extend to cover other significant cases:
```python
# drop_row / drop_column — already warns about data loss
# leave_null on high-null — warn so user understands it's intentional, not an oversight
elif strategy == "leave_null" and cp.get("null_pct", 0) > 30:
    warning = f"'{col}' has {cp['null_pct']}% nulls — kept as-is (MAR detected or intentional)."
```

---

### 1.8 Bool Type Detection Missing `"yes"` / `"no"` Variants

**File:** `prefect/flows/dq_logic.py:399` — `_detect_column_type()`

**Problem:** recommendations.md line 98 defines bool as:
> "Values are only true/false variants (`true`, `false`, `1`, `0`, `yes`, `no`)"

But `_BOOL_LIKE` only contains:
```python
_BOOL_LIKE = {True, False, "true", "false", "True", "False", "TRUE", "FALSE"}
```

A column with `["Yes", "No", "Yes"]` or `["1", "0", "1"]` (as strings) returns `"string"` not `"bool"`. It then gets string profiling, wrong fill strategy, and no bool cast on transform.

**Fix:** Extend `_BOOL_LIKE` to match the spec:
```python
_BOOL_LIKE = {
    True, False,
    "true", "false", "True", "False", "TRUE", "FALSE",
    "yes", "no", "Yes", "No", "YES", "NO",
    "y", "n", "Y", "N",
    "1", "0",
    "on", "off",
}
```

Note: this is separate from todo 1.9 (bool cast mapping in apply_recommendations). Detection and casting both need to cover the same set.

---

### 1.9 Boolean Cast Mapping Silently Drops Unrecognised Values

**File:** `prefect/flows/dq_logic.py:199` — `apply_recommendations()`, step 1 schema cast

**Problem:** The bool cast uses a fixed map:
```python
bool_map = {
    True: True, False: False,
    "true": True, "false": False,
    "True": True, "False": False,
    "1": True, "0": False, 1: True, 0: False,
}
df[col] = df[col].map(bool_map)
```
Any value not in the map (e.g., `"TRUE"`, `"FALSE"`, `"yes"`, `"no"`, `"Y"`, `"N"`, `"t"`, `"f"`) becomes `NaN` silently. A column with `"Yes"/"No"` values would be entirely nulled out after the cast.

**Fix:** Replace fixed map with string-normalised lookup (must match todo 1.8 set):
```python
bool_map = {
    True: True, False: False,
    1: True, 0: False,
}
true_strs  = {"true", "1", "yes", "y", "t", "on"}
false_strs = {"false", "0", "no", "n", "f", "off"}
df[col] = df[col].map(
    lambda x: True  if str(x).strip().lower() in true_strs
         else False if str(x).strip().lower() in false_strs
         else bool_map.get(x)
)
```

---

### 1.10 Pattern Columns: `invalid_count` Not Computed for Non-Matching Values ✓

**Status:** Done. Added `elif detected_type == "string"` branch to the `invalid_count` block in `profile_dataframe()`. For string columns with a detected pattern (`email`, `phone`, `url`, etc.), values that fail `pat.fullmatch()` are now counted as invalid and surfaced to the LLM profile table.

---

## Priority 2 — LLM Prompt Cleanup (`llm_enrichment.py`)

After Priority 1, the LLM no longer reasons about MAR or skewness — these are now code. Clean up the prompt.

### 2.1 Remove Statistical Reasoning from `_RUNNER_SYSTEM`

**File:** `prefect/flows/llm_enrichment.py:27-31`

**Remove these bullets** (all handled in code after Priority 1):
- `"Skewed numeric columns (median differs significantly from mean) — prefer median over mean..."` → done in `_fill_strategy()`
- `"Low-cardinality strings (cardinality_pct < 10%) — likely categorical, mode fill is appropriate..."` → already in `_fill_strategy()`

**Keep these** (semantic, cannot be computed):
- ID/key columns → `drop_row`
- Pattern columns (email, phone, UUID) → `drop_row`
- Event-date / optional-attribute high-null → `leave_null`
- Abbreviated/cryptic names → `rename_to`
- Sentinel strings in sample rows → note, do not add missing_values

**Update the `drop_column` rule** (line 57) — remove the CORRELATED NULLS reference:
```
- drop_column: use only when a column is >50% missing AND has no domain significance
```

**Update the `leave_null` rule** (line 56) — remove the CORRELATED NULLS reference:
```
- leave_null: use when nulls are intentional — appropriate for event-date or optional-attribute columns
```

---

### 2.2 Date Columns Appear in Wrong LLM Profile Table

**File:** `prefect/flows/llm_enrichment.py:258-292` — `_build_llm_profile()`

**Problem:** `profile_dataframe()` calls `_profile_numeric_column()` for both `"numeric"` AND `"date"` columns (line 84 of dq_logic.py). Their profile therefore has the numeric shape: `{null_count, null_pct, stats: {mean, median, std, min, max}, outliers}`.

But in `_build_llm_profile()`, the split at line 259 is:
```python
numeric = {col: cp for col, cp in ... if cp.get("detected_type") == "numeric"}
other   = {col: cp for col, cp in ... if cp.get("detected_type") != "numeric"}
```
Date columns (`detected_type == "date"`) go into `other`, which is rendered using the string table headers (`unique, cardinality_pct, top_values, pattern`). Date columns have none of these fields, so they produce a malformed row with empty/zeroed statistics:
```
birth_date,date,5.0,,0,,0
```
The LLM receives no date range, no stats, no useful information about date columns.

**Fix:** Add date columns to the numeric table:
```python
numeric = {
    col: cp for col, cp in profile["column_profiles"].items()
    if cp.get("detected_type") in ("numeric", "date")
}
other = {
    col: cp for col, cp in profile["column_profiles"].items()
    if cp.get("detected_type") not in ("numeric", "date")
}
```

---

### 2.3 Remove CORRELATED NULLS Section from `_build_llm_profile()`

**File:** `prefect/flows/llm_enrichment.py:294-301`

Delete lines 294–301 (the `# 4. Correlated nulls` block) after todo 1.3 is done.
Renumber the comment for Sample rows from "5." to "4.".

---

### 2.4 `max_tokens=512` May Truncate Wide Datasets

**File:** `prefect/flows/llm_enrichment.py:347`

**Problem:** The runner uses `max_tokens=512`. For a dataset with many columns where the LLM wants to change several of them, the partial diff JSON could exceed 512 tokens and be cut off mid-JSON, causing a `JSONDecodeError` on the next attempt. The retry loop will catch it, but if all 3 attempts hit the limit the whole enrichment silently falls back to baseline.

**Fix:** Increase to `max_tokens=1024` or make it dynamic based on the number of columns:
```python
max_tokens = min(512 + len(base_recs.get("columns", {})) * 30, 2048)
```

---

### 2.6 Column Enrichment LLM: Explicit Prompt Rule for `transform_hint` on Format Inconsistency Columns

**File:** `prefect/flows/llm_enrichment.py` — `_RUNNER_SYSTEM` only

**What's already done:** `transform_hint` field exists in schema (`file.py:35`), is validated as non-empty string in `validate_llm_output()`, is stripped from `_lean_baseline()`, and is a `_SUBSTANTIVE_KEYS` entry. The LLM can already output it for any string column based on sample rows. 4.8 (code generation from hints) is fully implemented.

**Status: Done.** Added explicit `_RUNNER_SYSTEM` rule: "if a column's warnings mention mixed value formats or not numeric-castable → MUST add transform_hint". The non-castable examples in the warning text serve as the concrete evidence the LLM needs.

**Problem:** When a column has mixed formats (e.g. `margin_pct` has `"46%"` and `"68.0"`), the column enrichment LLM should not change the type — but it also needs to describe what normalization is needed so the code generation LLM (4.8) can act on it.

**Fix — add `transform_hint` to the column enrichment output schema:**

```json
{
  "columns": {
    "margin_pct": {
      "note": "Contains percentage strings mixed with plain floats",
      "transform_hint": "strip % suffix and divide by 100 to get decimal"
    }
  }
}
```

`transform_hint` is:
- Always `null` in baseline → stripped from lean baseline (same as `note`, `rename_to`)
- Only set when the column's `warnings` mention mixed value formats
- Validated as a non-empty string in `validate_llm_output()`
- Read by `generate_format_transforms()` (4.8) to drive code generation LLM
- Visible to user in recommendations JSON — user can edit before code is generated

**Update `_RUNNER_SYSTEM` rule:**
```
- format inconsistency: if a column's warnings mention "mixed value formats" or
  "not numeric-castable", do NOT change its type — instead set transform_hint to
  a short description of what normalization is needed (e.g. "strip % suffix and
  divide by 100"); only set transform_hint for columns that have a format warning
```

**Update `validate_llm_output()`:** add check that `transform_hint` is a non-empty string when present.

**Update `_lean_baseline()`:** `transform_hint` always null in baseline — never include.

**Why `transform_hint` not a FORMAT ISSUES profile section:** The column enrichment LLM already has full context (profile, sample rows, format warning) to reason semantically about what the transform should do. Surfacing a dedicated profile section would duplicate information the LLM already has. The hint from LLM 1 becomes the description for LLM 2 — clean handoff between semantic reasoning and code synthesis.

**Unifies auto and user-described transforms:** user can also manually write a `transform_hint` for any column, and it flows through the same code generation step (4.8).

**Depends on:** 4.5 ✓ (format inconsistency in profile, already done), 4.8 (code generation consumes the hint).

---

### 2.5 Fix Stale Docstring in `dq_flow.py`

**File:** `prefect/flows/dq_flow.py:129`

`enrich_with_llm` docstring says "Planner → Runner → Validator" — Planner was removed.
Change to: "Runner → Validator — soft failure, returns base_recs on any error."

---

## Priority 3 — Schema Alignment (`backend/app/schemas/file.py`)

### 3.1 Add `"ignore"` to `DuplicatesConfig.strategy`

**File:** `backend/app/schemas/file.py:36`

**Problem:** `strategy: Literal["drop"]` — no way to pass through without deduplication.

**Fix:**
```python
class DuplicatesConfig(BaseModel):
    strategy: Literal["drop", "ignore"] = "ignore"
    subset: list[str] = Field(default_factory=list)
    keep: Literal["first", "last"] = "first"
```

---

### 3.2 Add `OutliersConfig` and `outliers` Field

**File:** `backend/app/schemas/file.py`

**Problem:** No schema for the `outliers` section that `build_recommendations()` will generate (todo 1.5).

**New class:**
```python
class OutliersConfig(BaseModel):
    strategy: Literal["winsorise", "remove", "cap", "keep"] = "keep"
    method: Literal["iqr"] = "iqr"
    lower: float | None = None
    upper: float | None = None
```

**Add to `RecommendationsSchema`:**
```python
outliers: dict[str, OutliersConfig] = Field(default_factory=dict)
```

---

### 3.3 `rename_to` Not Validated for Format

**Files:** `backend/app/schemas/file.py:31`, `prefect/flows/llm_enrichment.py:424`

**Problem:** `rename_to: str | None = None` in `ColumnConfig` and `validate_llm_output()` only checks `isinstance(rename_to, str)`. There is no check that the value is a valid Python/pandas identifier. A value like `"full name"` (with a space) or `"first-name"` (with a hyphen) would pass validation, be applied by `apply_recommendations()`, and produce a CSV with a column name containing spaces or hyphens — which breaks most downstream tooling.

**Fix:** Add a check in both `ColumnConfig` and `validate_llm_output()`:
```python
# In validate_llm_output():
if "rename_to" in col_def:
    rt = col_def["rename_to"]
    if not isinstance(rt, str):
        errors.append(f"columns['{col}'].rename_to must be a string")
    elif not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', rt):
        errors.append(
            f"columns['{col}'].rename_to='{rt}' is not a valid identifier "
            f"(use snake_case, no spaces or hyphens)"
        )
```

---

## Priority 4 — New Features

### 4.1 Outlier Treatment in `apply_recommendations()`

**File:** `prefect/flows/dq_logic.py:169` — `apply_recommendations()`

**Problem:** Outliers are detected and will be in recommendations (todo 1.5) but
`apply_recommendations()` has no step to handle them.

**Add after step 3 (int recast), before step 4 (dedup):**
```python
# Step 3b. Apply outlier treatment
for col, outlier_def in recommendations.get("outliers", {}).items():
    if col not in df.columns:
        continue
    strategy = outlier_def.get("strategy", "keep")
    if strategy == "keep":
        continue
    lower = outlier_def.get("lower")
    upper = outlier_def.get("upper")
    if lower is None or upper is None:
        continue
    if strategy == "winsorise":
        df[col] = df[col].clip(lower, upper)
    elif strategy == "remove":
        df = df[(df[col].isna()) | ((df[col] >= lower) & (df[col] <= upper))]
    elif strategy == "cap":
        df[col] = df[col].clip(lower, upper)
```

---

### 4.2 Sentinel Value Detection in `profile_dataframe()`

**File:** `prefect/flows/dq_logic.py:55` — `profile_dataframe()` / `_profile_string_column()` / `_profile_numeric_column()`

**Problem — string sentinels:** Sentinel strings (`"N/A"`, `"unknown"`, `"none"`, `"-"`, `"null"`, `"?"`, etc.) stored as real values are not counted for pure string columns. `invalid_count` only catches type-mismatch (e.g. `"N/A"` in a column detected as numeric).

**Problem — numeric sentinels (not in any current todo):** recommendations.md line 80 also specifies:
> "Numeric sentinels: values that are implausibly extreme for the column's distribution AND appear with suspiciously high frequency"
Examples: `-999`, `9999`, `99` used as "not recorded" codes in a numeric column. These are currently invisible — they appear as valid numbers and skew statistics.

**Fix — string sentinels:**
```python
# Full sentinel list per recommendations.md (includes "?")
_SENTINEL_STRINGS = {"n/a", "na", "null", "none", "unknown", "missing", "undefined", "-", "?", ""}

def _count_sentinels(series: pd.Series) -> int:
    return int(series.astype(str).str.strip().str.lower().isin(_SENTINEL_STRINGS).sum())
```
Call in `_profile_string_column()`, expose `sentinel_count` in the returned dict.

**Fix — numeric sentinels:**
```python
def _count_numeric_sentinels(series: pd.Series) -> int:
    """Detect suspiciously frequent extreme values (e.g. -999, 9999 as 'not recorded' codes)."""
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if len(numeric) < 10:
        return 0
    q1, q3 = numeric.quantile([0.25, 0.75])
    iqr = q3 - q1
    candidates = numeric[(numeric < q1 - 3 * iqr) | (numeric > q3 + 3 * iqr)]
    vc = candidates.value_counts()
    # A value appearing ≥5% of the time as an extreme outlier is likely a sentinel
    suspicious = vc[vc / len(numeric) >= 0.05]
    return int(suspicious.sum())
```
Call in `_profile_numeric_column()`.

**Surface both** in `_metadata.issues_found.invalid_values` in `build_recommendations()`.

Also add sentinel-specific warning in `build_recommendations()` — recommendations.md line 85:
> "When `invalid_count > 0` for a column, the recommendation flags the column with a `warning`"

Currently warnings only fire for drop strategies. Sentinel detection should also generate a warning regardless of strategy:
```python
if cp.get("sentinel_count", 0) > 0 or cp.get("invalid_count", 0) > 0:
    warning = f"'{col}' contains {sentinel_count} potential sentinel values — review before imputation."
```

---

### 4.3 Date Format Standardisation to ISO 8601 on Export

**File:** `prefect/flows/dq_logic.py:197` — `apply_recommendations()`, step 1 schema cast

**Problem:** Date columns are cast to datetime but written to CSV as pandas default format, which
is not consistent ISO 8601.

**Fix:** After datetime cast, format to `%Y-%m-%d` (or `%Y-%m-%dT%H:%M:%S` for datetime with time):
```python
elif col_type == "date":
    df[col] = pd.to_datetime(df[col], errors="coerce", format="mixed")
    df[col] = df[col].dt.strftime("%Y-%m-%d")
```

---

### 4.4 Z-Score Normalization Option

**File:** `prefect/flows/dq_logic.py:264` — `apply_recommendations()` step 5

**Problem:** Only min-max normalization is implemented. Z-score (standardization) is the better
choice when outliers are present or the distribution is unknown — the schema field `normalize: bool`
gives no way to choose between them.

**Fix (two parts):**

1. Change `normalize` from `bool` to `Literal["min_max", "z_score", false]` in the schema
   and in `ColumnConfig` in `file.py`
2. In `apply_recommendations()`:
```python
if col_def.get("normalize") == "min_max":
    col_min, col_max = df[col].min(), df[col].max()
    if col_max != col_min:
        df[col] = (df[col] - col_min) / (col_max - col_min)
elif col_def.get("normalize") == "z_score":
    mean, std = df[col].mean(), df[col].std()
    if std > 0:
        df[col] = (df[col] - mean) / std
```

3. Update `build_recommendations()` to set `normalize: "min_max"` or `normalize: "z_score"` based
   on whether outliers are present (use z_score when `outlier_count > 0`).

---

### 4.5 Format Inconsistency Detection in Profiler

**File:** `prefect/flows/dq_logic.py` — `_profile_string_column()`

**Problem:** String columns with mixed value formats (e.g. `"46%"` and `"68.0"` coexisting in `margin_pct`, or `"$42.50"` and `"42.50"` in a price column) score 100% on consistency because our consistency dimension only checks against 5 known patterns (email, phone, UUID, URL, date). Mixed format within a column is completely invisible to the DQ score — and invisible to the LLM since the profile doesn't flag it.

**Real impact seen in testing:** LLM changed `margin_pct` from `string → float` (a semantically correct call), `apply_recommendations` ran `pd.to_numeric(errors='coerce')`, and 300 of 500 values with `%` suffix became `NaN` — dropping the DQ score by 2.48 points.

**Fix — regex shape clustering:**

In `_profile_string_column()`, group non-null values by their abstract regex shape and count occurrences of each shape. If ≥2 shapes each cover >5% of values, flag the column as format-inconsistent and store which formats were found.

```python
_FORMAT_PATTERNS = [
    ("percentage",  r"^-?\d+\.?\d*%$"),
    ("currency",    r"^\$-?\d[\d,]*\.?\d*$"),
    ("unit_value",  r"^-?\d+\.?\d*\s*[a-zA-Z]+$"),   # e.g. "5km", "70kg"
    ("integer",     r"^-?\d+$"),
    ("float",       r"^-?\d+\.\d+$"),
]

def _detect_format_patterns(series: pd.Series) -> dict[str, int]:
    """Return counts of each recognized format shape present in the series."""
    counts: dict[str, int] = {}
    non_null = series.dropna().astype(str).str.strip()
    for name, pattern in _FORMAT_PATTERNS:
        n = int(non_null.str.match(pattern).sum())
        if n > 0:
            counts[name] = n
    return counts
```

Return from `_profile_string_column()`:
```python
format_counts = _detect_format_patterns(series)
dominant = max(format_counts.values(), default=0)
threshold = max(5, len(non_null) * 0.05)   # at least 5 occurrences, or 5%
active_formats = {k: v for k, v in format_counts.items() if v >= threshold}
format_inconsistency = len(active_formats) > 1

return {
    ...,
    "format_patterns": active_formats,       # e.g. {"percentage": 300, "float": 200}
    "format_inconsistency": format_inconsistency,
}
```

Also surface in `_metadata.issues_found` in `build_recommendations()` and generate a warning for any column where `format_inconsistency: true`.

---

### 4.6 Destructive Type Cast Guard in `apply_recommendations()`

**File:** `prefect/flows/dq_logic.py` — `apply_recommendations()`, step 1 schema cast

**Problem:** Currently, if a column has type `string` in the raw data and the recommendations say `type: float`, `apply_recommendations` runs `pd.to_numeric(errors='coerce')` unconditionally. For columns with mixed format (e.g. `"46%"` values), this silently converts all unparseable values to `NaN`, corrupting the data without any warning. The user sees a lower DQ score after apply with no explanation.

**Fix:** Before any `string → float/int` cast, simulate the cast on a sample and measure the new-null rate. If >5% of non-null values would become `NaN`, skip the cast, log a warning, and leave the column as-is.

```python
def _would_cast_safely(series: pd.Series, target_type: str, threshold: float = 0.05) -> bool:
    """Return True if casting to target_type would produce fewer than threshold% new NaN."""
    if target_type not in ("float", "int"):
        return True
    non_null = series.dropna()
    if len(non_null) == 0:
        return True
    converted = pd.to_numeric(non_null, errors="coerce")
    new_null_rate = converted.isna().sum() / len(non_null)
    return new_null_rate <= threshold

# In step 1, before each cast:
if not _would_cast_safely(df[col], col_type):
    logger.warning(
        f"Skipping type cast for '{col}': casting to {col_type} would null "
        f">5% of values. Use a custom_transform to normalize format first."
    )
    continue
```

This is a safety net that prevents silent data corruption. The column remains as-is rather than being partially destroyed.

---

### 4.7 Sentinel Detection Threshold: Frequency % → Absolute Count

**File:** `prefect/flows/dq_logic.py` — `_detect_numeric_sentinels()`

**Problem:** The current implementation flags a value as a sentinel only when it appears at **≥5% frequency** among non-null rows. This worked for small datasets but fails silently on large ones.

Confirmed in E2E testing: `iot_sensors_large.csv` has 34 occurrences of `-999.0` in `temp_c` (1152 non-null rows = **2.8%**) — below the 5% threshold. The sentinel was not detected, so `sentinel_values: null` in the recommendations, step 0 of `apply_recommendations` did nothing, and the 34 values survived z-score normalization as anomalous z-scores of **~-5.86**. The cleaned column had 34 points at -5.86 and 1166 points between 0.16–0.23 — a severely bimodal output that would corrupt any downstream model.

**Why absolute count is better:**

| Dataset size | 5% threshold means | Absolute ≥5 means |
|---|---|---|
| 100 rows | need ≥5 occurrences | need ≥5 occurrences |
| 1 000 rows | need ≥50 occurrences | need ≥5 occurrences |
| 10 000 rows | need ≥500 occurrences | need ≥5 occurrences |

The 3×IQR fence is already a very tight constraint — only genuinely extreme values reach this stage. An absolute count of 5 is enough to distinguish "systematic coded null" from "random isolated noise". The frequency % adds no safety and actively breaks on large datasets.

**Fix:**

```python
def _detect_numeric_sentinels(series: pd.Series) -> tuple[int, list[float]]:
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    if len(numeric) < 10:
        return 0, []
    q1, q3 = numeric.quantile([0.25, 0.75])
    iqr = q3 - q1
    if iqr == 0:
        return 0, []
    candidates = numeric[(numeric < q1 - 3 * iqr) | (numeric > q3 + 3 * iqr)]
    vc = candidates.value_counts()
    # OLD: suspicious = vc[vc / len(numeric) >= 0.05]
    # NEW: any extreme value appearing ≥5 times is a sentinel regardless of dataset size
    suspicious = vc[vc >= 5]
    return int(suspicious.sum()), sorted(float(v) for v in suspicious.index)
```

Update `TestDetectNumericSentinels` in `test_dq_logic.py` — add a case where the sentinel appears at 2–4% frequency in a large dataset (≥200 rows) and verify it is now caught.

---

### 4.8 Custom Transform Generation (Two-LLM Pipeline)

**Context:** Original project spec item #3. Two-step: LLM 1 (column enrichment, already exists) reasons semantically and sets `transform_hint`. LLM 2 (new) converts the hint to validated pandas code.

---

**Architecture:**

```
DQ Flow:
  profile_dataframe()          ← format_inconsistency detected (4.5 ✓)
  build_recommendations()      ← format warning added to column (4.5 ✓)
  enrich_recommendations()     ← LLM 1: adds transform_hint (2.6)
  generate_format_transforms() ← LLM 2: hint → pandas code (4.8)
  save to DB

User reviews recommendations:
  - sees custom_transforms[] with generated code + description
  - can edit transform_hint and trigger regeneration
  - can add new manual transform descriptions
  - approves via PUT /recommendations

Transform Flow:
  [pre-step] user-described transforms without logic → LLM 2 again
  apply_recommendations() step 0b: execute validated transforms
```

---

**Inputs to LLM 2 (one call for all pending transforms):**

```
TRANSFORM TASKS:
col,transform_hint,not_castable_pct,examples,target_type
margin_pct,strip % suffix and divide by 100,40,46%;52.3%,float
price_usd,strip $ prefix,35,$42.50;$100.00,float
```

Plus any user-described transforms from `custom_transforms[]` that have a `description` but no `logic`.

**Output from LLM 2:**
```json
{
  "transforms": [
    {
      "column": "margin_pct",
      "description": "Strip % suffix and divide by 100 to get decimal",
      "logic": "df['margin_pct'].str.rstrip('%').astype(float) / 100"
    }
  ]
}
```

---

**Validation — 3 layers (per transform):**

1. **Syntax** — `compile(logic, '<string>', 'exec')` — catches malformed code
2. **Safety** — AST whitelist inspection (see below) — rejects dangerous operations
3. **Execution test** — run on a sample that includes `non_castable_sample` values — catches runtime errors; verify `len(df)` unchanged and result is castable to `target_type`

Up to 3 retries per transform. All fail → skip that transform, log warning, 4.6 guard catches any downstream cast attempt.

**AST Whitelist — allowed operations:**
```python
ALLOWED_ATTRS = {
    "str.rstrip", "str.lstrip", "str.strip", "str.replace",
    "str.lower", "str.upper", "str.extract", "str.removeprefix",
    "str.removesuffix", "str.split", "astype",
}
ALLOWED_CALLS = {"pd.to_numeric", "pd.to_datetime", "round", "abs", "fillna", "clip"}
ALLOWED_NAMES = {"df", "pd"}   # no other free names
BLOCKED_NODES = {ast.Import, ast.ImportFrom}
BLOCKED_CALLS = {"eval", "exec", "open", "compile", "__import__"}
```

**Critical:** walk ALL AST nodes recursively including lambda bodies, list comprehensions, and nested functions — a lambda body can contain anything if not explicitly checked.

---

**Execution in `apply_recommendations()` — new Step 0b:**

```
Step 0:   replace sentinel values → NaN
Step 0b:  execute custom transforms           ← new
Step 1:   schema cast (now safe — format normalised)
```

Per transform:
- Save original column: `original = df[col].copy()`
- Execute in restricted namespace: `{"df": df, "pd": pd, "__builtins__": {}}`
- If exception → restore `df[col] = original`, log warning, continue
- If `len(df)` changed → restore and reject (transforms must not filter rows)

---

**Schema changes (`backend/app/schemas/file.py`):**

Add `CustomTransformEntry`:
```python
class CustomTransformEntry(BaseModel):
    column: str
    description: str
    type: Literal["format_normalization", "computed_column", "user_defined"] = "user_defined"
    logic: str | None = None       # null until LLM 2 generates it
    validated: bool = False
```

Update `RecommendationsSchema`:
```python
custom_transforms: list[CustomTransformEntry] = Field(default_factory=list)
```

---

**Known risks and mitigations:**

| Risk | Mitigation |
|------|------------|
| Semantically wrong but syntactically valid code (46.0 vs 0.46) | **User review** — the only gate; pipeline cannot auto-validate semantics |
| Lambda body bypasses AST whitelist | Recursive walk of ALL AST nodes including lambda bodies |
| Execution test misses bad values | Include `non_castable_sample` values in test rows |
| Transform crashes mid-apply, column corrupted | Save + restore original column on exception |
| Transform filters rows (changes `len(df)`) | Assert `len(df)` unchanged; reject and restore if violated |
| Stale `logic` after user edits `transform_hint` | Clear `logic` and set `validated: false` on PUT if `transform_hint` changed |
| LLM 1 sets `transform_hint` AND changes type | Post-processing: strip type change when `transform_hint` is also set |
| Rate limits from two consecutive LLM calls | Existing backoff handles; LLM 2 only runs if there are pending transforms |

---

**Files to change:**

| File | Change |
|------|--------|
| `llm_enrichment.py` | `generate_format_transforms()`, `_TRANSFORM_SYSTEM` prompt, AST validator |
| `dq_flow.py` | New task after `enrich_with_llm()` |
| `transform_flow.py` | Pre-step: LLM 2 for user-described transforms without `logic` |
| `dq_logic.py` | Step 0b in `apply_recommendations()` with rollback + row count check |
| `file.py` | `CustomTransformEntry` schema |
| `test_llm_enrichment.py` | Transform generation + AST validation tests |
| `test_dq_logic.py` | Step 0b execution + rollback tests |

**Blocked by:** 2.6 (transform_hint in LLM 1 output). Largest remaining feature.

---

### 4.9 String Sentinel Values + `replace_sentinels` Flag

**Files:** `prefect/flows/dq_logic.py`, `backend/app/schemas/file.py`, `prefect/tests/test_dq_logic.py`

**Problem — string sentinels detected but never replaced:**

`_count_sentinels()` returns only an integer count. String sentinel values (`"N/A"`, `"unknown"`, `"-"`, etc.) are detected and generate a warning that says "will be replaced with null before imputation" — but `sentinel_values` in the recommendation is always `null` for string columns, so step 0 of `apply_recommendations()` skips them entirely. The warning is a lie.

Numeric sentinels (`_detect_numeric_sentinels()`) correctly return both count and values, and are replaced in step 0.

**Fix — part 1: return string sentinel values:**

```python
# today: returns int
# after: returns tuple[int, list[str]]
def _count_sentinels(series: pd.Series) -> tuple[int, list[str]]:
    non_null = series.dropna()
    if len(non_null) == 0:
        return 0, []
    as_str = non_null.astype(str).str.strip()
    normalised = as_str.str.lower()
    mask = normalised.isin(_SENTINEL_STRINGS)
    # Original casing — so df.replace() hits the actual value
    found = sorted(as_str[mask].unique().tolist())
    return int(mask.sum()), found
```

**No threshold for string sentinels** — the frozenset is a curated list of known placeholder patterns. Any match is a sentinel regardless of frequency. This differs from numeric sentinels which use ≥5 occurrences because numeric detection is statistical (3×IQR fence) and a single extreme value could be real data.

**Update `_profile_string_column()`:** Store both count and values:
```python
sentinel_count, sentinel_values = _count_sentinels(series)
# add sentinel_values to the profile dict alongside sentinel_count
```

**Fix — part 2: `replace_sentinels` flag:**

Add a per-column boolean flag so users can see detected sentinels without replacing them. Useful when a detected sentinel is a legitimate domain value (e.g. `"unknown"` as a valid category, `-999` as a real measurement).

**`build_recommendations()`:** Add `"replace_sentinels": True` when `sentinel_values` is non-null.

**`apply_recommendations()` step 0:** Guard replacement with the flag:
```python
sentinels = col_def.get("sentinel_values")
if sentinels and col_def.get("replace_sentinels", True) and col in df.columns:
    df[col] = df[col].replace(sentinels, float("nan"))
```

**Schema (`file.py`):**
```python
sentinel_values: list[float | str] | None = None
replace_sentinels: bool = True
```

**Frontend:** Checkbox per column for `replace_sentinels` next to sentinel values display in AWAITING_REVIEW.

**Files to change:**

| File | Change |
|------|--------|
| `dq_logic.py` — `_count_sentinels()` | Return `tuple[int, list[str]]` with original-casing values, no threshold |
| `dq_logic.py` — `_profile_string_column()` | Store `sentinel_values` from new return |
| `dq_logic.py` — `build_recommendations()` | Add `replace_sentinels: True` when sentinels present |
| `dq_logic.py` — `apply_recommendations()` step 0 | Check `replace_sentinels` flag before replacing |
| `file.py` — `ColumnConfig` | `sentinel_values: list[float \| str] \| None`, add `replace_sentinels: bool = True` |
| `test_dq_logic.py` | Tests for string sentinel return shape, replace flag on/off |
| Frontend — AWAITING_REVIEW | Checkbox for `replace_sentinels` per column |

---

## Known Limitations (Not Implemented — Thesis Should State These Explicitly)

These are genuine gaps between what the pipeline does and what full ISO 25012 compliance would require.
They are not bugs — they are scope decisions. Each should be documented in the thesis.

### L1 — Validity Does Not Catch Range Violations

**What the code does:** Validity (`_calculate_validity()`) only checks **type conformance** —
whether a string column contains values that fail numeric or date coercion. A native `int` or
`float` column is **completely skipped** (`if "object" not in str(df[col].dtype): continue`).

**What this means:** `age = -5`, `age = 300`, `salary = -99999` all score as 100% valid. Range
violations are undetectable without domain-specific rules (what is a valid age? what is a valid salary?).

**Why it's out of scope:** Range validation requires a domain rule per column. These cannot be
inferred automatically from data alone without either a schema contract or LLM reasoning per column.

**Thesis framing:** "Validity is measured as type-format conformance. Domain-range validity
(e.g. age ∈ [0,120]) is a known limitation and would require user-supplied constraint rules
or LLM-assisted inference — outside the current scope."

---

### L2 — Consistency Does Not Check Cross-Field Contradictions

**What the code does:** Consistency (`_calculate_consistency()`) only checks **within-column
format uniformity** — if a column is detected as mostly emails, rows that don't match the email
pattern count as inconsistent. Nothing else.

**What this means:** `hire_date = 1995`, `birth_year = 2000` (hired before born) is not detected.
`end_date < start_date` is not detected. These cross-field logical contradictions score as 100% consistent.

**Why it's out of scope:** Cross-field consistency requires rules that relate two or more columns.
These rules are dataset-specific and cannot be inferred automatically. They would require either
user-supplied constraint pairs or LLM reasoning about field relationships.

**Thesis framing:** "Consistency is measured as within-column format pattern uniformity (email,
phone, UUID, date patterns). Cross-field consistency — logical contradictions between related
columns — is a known limitation requiring dataset-specific constraint rules."

---

## Priority 5 — Test Updates

### 5.1 `test_dq_logic.py` — Updates for Logic Changes

After completing Priority 1 items:
- `TestFillStrategy`: add case where a symmetric numeric column (mean ≈ median) gets `"mean"`
- `TestFillStrategy`: add case where a skewed numeric column gets `"median"`
- `TestFillStrategy`: verify existing `drop_column` threshold test covers the ≥50% case (no change needed)
- `TestBuildRecommendations`: add case where a MAR column (correlated nulls) gets `"leave_null"` in baseline
- `TestBuildRecommendations`: add case where `_metadata.issues_found` contains the right keys
- `TestApplyRecommendations`: add case for outlier `"winsorise"` strategy

### 5.2 `test_llm_enrichment.py` — Updates for Prompt Cleanup

After completing Priority 2 items:
- `TestBuildLlmProfile`: verify CORRELATED NULLS section is NOT in output (after removal)
- `TestEnrichRecommendations`: add case that a `leave_null` strategy in baseline is NOT overridden by LLM

---

## Priority 6 — Frontend Visualisations (Teacher Suggestion)

Charts and dynamic statistics for thesis demo. Suggested by supervisor.

**Principle:** All computation is client-side where possible — no new API calls needed at render time. One small backend field addition (`null_count` per column) unlocks all dynamic stats.

---

### F.1 Box Plot in EDA Dashboard ✓

**Status:** Done. `BoxPlotGroup.tsx` (`frontend/src/components/charts/`) renders Tukey box plots for all numeric columns that have detected outliers. Shown in the EDA Dashboard (Data Profile tab of AWAITING_REVIEW). Decision #65.

Note: the original spec placed the box plot inline in `OutliersSection.tsx`. It was instead placed in the EDA Dashboard where it fits more naturally alongside other distribution visualizations. The `OutliersSection` remains as a strategy-selection table.

---

### F.2 Radar Chart — DQ Score Breakdown ✓

**Status:** Done. `DQRadarChart.tsx` (`frontend/src/components/charts/`) renders a single radar polygon on 4 axes (Completeness, Uniqueness, Validity, Consistency) using Recharts. Shown in the EDA Dashboard (Data Profile tab of AWAITING_REVIEW). Decision #65.

Note: original spec placed this in `ScoreComparison.tsx` as a before/after overlay in COMPLETED view. It was instead placed in the EDA Dashboard showing the before-state breakdown. The COMPLETED Data Profile tab retains the bar-based `ScoreComparison` + `IssuesGrid` layout.

---

### F.3 Issues Summary in RecommendationsEditor ✓

**Status:** Done. `RecommendationsEditor.tsx` shows a compact pill row at the top: DQ score, total rows, total columns, and amber pills for each non-zero issue type (missing, duplicates, type_mismatches, sentinel_values, format_inconsistencies, outliers). Data from `initialRecs._metadata.issues_found`. Decision #66.

---

### F.4 Live Impact Preview (Dynamic Stats)

**Where:** `frontend/src/components/recommendations/RecommendationsEditor.tsx` — sticky bar just above `SubmitBar`.

**What:** As user changes column strategies, a summary line updates live showing projected impact:
```
Will drop  47 rows  |  Fill 312 cells  |  Remove 2 columns  |  Projected score ~74.2 ↑6.8
```

**How it works:** Pure `useMemo` over the `recs` state — no API call. For each column:
- `drop_row` → `droppedRows += null_count`
- `drop_column` → `droppedCols += 1`
- `median / mean / mode / fill` → `filledCells += null_count`
- `leave_null` → no change

Duplicates: if `strategy === "drop"` → `dupRowsDropped = issues_found.duplicates`.
Outliers: if `strategy === "remove"` → `outlierRowsDropped += outlier.count`.

Projected completeness = `(original_missing - filledCells) / total_cells * 100`, plug into DQ formula for rough score estimate.

**Backend change needed — add `null_count` to `ColumnConfig`:**

`prefect/flows/dq_logic.py` — `build_recommendations()`: add `"null_count": cp["null_count"]` to each column entry.

`backend/app/schemas/file.py` — `ColumnConfig`: add `null_count: int | None = None`.

`frontend/src/types/index.ts` — `ColumnConfig`: add `null_count?: number`.

---

### F.5 DQ Score History Chart (Future / Nice-to-have)

**Where:** Sidebar or a dedicated History tab.

**What:** Line/area chart of DQ scores (before → after) across all past runs. Shows cumulative pipeline value over time.

**Blocked by:** Needs enough run history to be meaningful. Low priority until there are ≥5 runs to display.

**No backend change needed** — `GET /api/files` already returns all runs with scores.

---

---

## Priority 6 — Documentation & Guardrails (from ChatGPT audit)

Items identified from a critical cross-review of all documentation. These are documentation accuracy and design clarity items, not code bugs.

### 6.1 Scope Disclaimer Added to `recommendations.md` ✓

**Status:** Done. Added to the Framework section. Text:
> "This pipeline adopts practitioner interpretations of DAMA DMBOK dimensions. It is not ISO-certified and does not implement full standard compliance. Every threshold is a configurable heuristic, not a statistically universal rule."

### 6.2 Decision Precedence Hierarchy Added to `recommendations.md` ✓

**Status:** Done. Explicit 5-level priority table added to the Missing Values section documenting: MAR (code) > 50% threshold (code) > type rules (code) > LLM MNAR override > user. Also documents what the LLM can and cannot change.

### 6.3 MAR Detection Renamed in Code Comment ✓

**File:** `prefect/flows/llm_enrichment.py:176` — `_correlated_nulls()`

**Status:** Done. Docstring now explicitly says "heuristic co-occurrence check" and clarifies it is NOT formal statistical MAR detection (which would require logistic regression or Little's MCAR test).

### 6.4 Add Scope Constraints to Outlier Section in `recommendations.md`

**Status:** Done.

**Problem:** Documentation does not explicitly state that IQR outlier detection is:
- Univariate (per-column only)
- Not for fraud/anomaly detection
- Not multivariate

Risk: future descriptions could overclaim ("detects anomalous transactions").

**Fix:** Add a note under Section 4 (Outliers) in `recommendations.md`:
> "IQR detection is univariate — it evaluates each column independently. It does not detect multivariate anomalies (e.g. an unusual combination of age + salary). It is not a fraud or anomaly detection method."

---

## Frontend + Backend Collaboration Items

### FC1 — Dynamic Drop-Row Count via Simulate Endpoint ✓

**Status:** Done. `build_dropmasks()` precomputes per-operation bitsets at DQ analysis time and stores them in MinIO. The backend `/api/runs/{run_id}/drop-impact` endpoint evaluates the current strategy selection against bitsets to return exact row counts (union, not sum). `RecommendationsEditor` calls `getDropImpact` with 300ms debounce on every recs state change. Decision #69.

**Context:** `drop_row` warnings are per-column upper bounds. When multiple columns have `drop_row` strategy, the actual rows dropped is the set union — not the sum — because some rows are null in multiple columns at once.

**Example:** 3 columns, each 10 nulls, 5 rows overlapping across all three:
- All 3 as `drop_row` → 5 (overlap) + 5 (A unique) + 5 (B unique) + 5 (C unique) = 20 rows dropped
- User denies A (→ `leave_null`) → rows where B OR C is null → 5 + 5 + 5 = 15 rows dropped
- User denies A and B → only C → 10 rows dropped

The frontend cannot compute this from per-column counts alone — it needs the actual row-level overlap. Sentinel values converted to NaN in step 0 also contribute to drop counts but are not currently reflected in warnings.

**Solution options considered:**

| Approach | Pros | Cons |
|---|---|---|
| Simulate endpoint (load from MinIO) | Exact, always correct | Loads CSV on every toggle — too slow for 200MB files |
| Precompute all combinations at profile time | No extra API calls | Exponential terms for 5+ columns, stale after user edits |
| **Null-bitmap per column (preferred)** | Fast simulate, no MinIO load | Extra storage per run (~125KB/column for 1M rows) |

**Preferred approach — null-bitmap:**

At `build_recommendations()` time, for each column that is a candidate for `drop_row`, compute a compact null-bitmap (one bit per row: 1 = null or sentinel, 0 = present). Store in the run record (PostgreSQL JSONB or a sidecar blob in MinIO). The simulate endpoint then:
1. Loads the precomputed bitmaps (fast — no CSV parsing)
2. Computes the bitwise OR across the user's selected `drop_row` columns
3. Returns the popcount (exact dropped row count)

For 1M rows: 1M bits = 125KB per column. 5 drop_row columns = 625KB. Manageable in JSONB or as a small MinIO object alongside the recommendations.

```
POST /api/files/{file_id}/recommendations/simulate-drop
body: {"drop_row_columns": ["order_id", "order_date"]}
→ {"rows_that_would_be_dropped": 15, "total_rows": 350, "pct": 4.3}
```

**Also needed for:** sentinel-adjusted counts (step 0 converts sentinels to NaN — these should be included in the bitmap as "effectively null").

**Blocked by:** Frontend recommendations editor (not yet built). Add endpoint when frontend UI for recommendations is designed.

---

## Future Considerations (Rejected for Now — Research Prototype Scope)

Items raised in a ChatGPT audit that are valid in principle but over-engineered for the current stage. Saved here for potential future work.

### F1 — Confidence Level per Recommendation

**Proposal:** Add a `confidence_level: "deterministic" | "heuristic" | "semantic_inference"` field to every column recommendation.

**Why rejected now:** Would require a schema change across `ColumnConfig`, `build_recommendations()`, `apply_recommendations()`, and the LLM prompt. The benefit (clearer transparency to the user) does not justify the complexity at prototype stage.

**If implemented:** Each field in the column config would carry a label. `type` and `nullable` are `deterministic`; `missing_values.strategy` is `heuristic` (code rules) or `semantic_inference` (LLM); `rename_to` is `semantic_inference`.

### F2 — Numeric Confidence Score for LLM Overrides

**Proposal:** LLM outputs a confidence score (0.0–1.0) alongside each recommendation change.

**Why rejected now:** LLM confidence scores are themselves probabilistic and unreliable. We already have the `note` field which gives qualitative reasoning. A numeric score would give false precision. The `note` field is the right mechanism.

**If revisited:** Consider asking the LLM to classify its own certainty as low/medium/high (not a number) and surface that to the user alongside the note.

### F4 — LLM Reasoning for Outlier Strategy Selection

**Context:** The `outliers` section is fully deterministic — IQR detects outliers, the default strategy is `"keep"`, and the user changes it via the API. The LLM currently has no involvement in outlier decisions.

**Opportunity:** The LLM could semantically reason about *whether* outliers should be winsorised, removed, or kept. For example:
- `temp_c` with outliers at 120°C → "These exceed the physical sensor maximum — likely measurement errors, winsorise"
- `vibr_mm` with outliers at 95th percentile → "Vibration spikes may represent real fault events — keep and investigate"
- `salary` with outliers at $800k → "Plausible for senior roles — keep"

**Why deferred:** The `outliers` section is user-editable today. The LLM adding notes/recommendations to outlier columns (via the `outliers[col].note` field — not yet defined) is a natural extension once the core enrichment flow is stable.

**Implementation sketch (when ready):**
- Add `note: str | null` to `OutliersConfig`
- Pass `outliers` section to LLM alongside `columns` in the profile
- Add a rule to `_RUNNER_SYSTEM`: "For outlier sections, you may suggest a strategy change and add a note explaining the domain reasoning. Do not change `lower`/`upper` bounds."

---

### F5 — rename_to v2: Opaque Column Names ✓

**Status:** Done — resolved by the split rename LLM call (Decision #61).

**Proof:** Log `logs/dq_opaque_fields_2026-03-21T00-43-58.log` shows 8/8 opaque columns correctly renamed from sample values alone:
`f001→email`, `f002→phone_number`, `f003→start_date`, `f004→end_date`, `f005→city`, `f006→score`, `f007→status`, `f008→rating`

The dedicated rename call with its data-validated prompt ("both conditions must hold: name is cryptic AND sample values confirm the meaning") handles opaque names naturally — the LLM reads the sample rows, infers meaning, and proposes the name. No code-side flagging needed.

---

### F3 — Log LLM Overrides of Deterministic Baseline ✓

**Status:** Done. `llm_enrichment.py:736–748` logs each override after the merge. Format: `LLM override | <col> | <key>: <before> → <after>`. `note` changes are skipped (noise); `missing_values` shows strategy string not the full dict. Visible in run log files.

---

### UI5 — `storage_expired` Computed Field + Frontend Download Gating

**Files:**
- `backend/app/schemas/file.py` — `RunOut`
- `frontend/src/types/index.ts` — `RunOut` interface
- `frontend/src/components/run/RunDetailView.tsx` (or wherever the download button lives)

**Problem:** Raw files are deleted after 7 days, curated files after 14 days (MinIO ILM lifecycle rules, Decision #60). The API currently returns no signal to the frontend that a run's download is no longer available. Users will see a download button that silently fails after 14 days.

**Fix:** Add a server-side computed field `storage_expired: bool` to `RunOut`. No schema migration, no cron job, no DB writes — compute at serialisation time using a Pydantic `model_validator`.

**Backend — `backend/app/schemas/file.py`:**
```python
from datetime import datetime, timezone, timedelta
from pydantic import model_validator

class RunOut(BaseModel):
    ...existing fields...
    storage_expired: bool = False

    @model_validator(mode="after")
    def compute_storage_expired(self) -> "RunOut":
        if self.status == RunStatus.COMPLETED and self.created_at:
            cutoff = self.created_at + timedelta(days=14)
            self.storage_expired = datetime.now(timezone.utc) > cutoff
        return self
```

Note: `created_at` is timezone-aware (asyncpg returns `datetime` with tzinfo for `TIMESTAMPTZ` columns).

**Frontend — `frontend/src/types/index.ts`:**
```typescript
export interface RunOut {
  ...
  storage_expired: boolean
}
```

**Frontend — download button:**
- When `runData.storage_expired` is true: show greyed-out button with tooltip "File expired (curated files kept for 14 days)" instead of a live download link.
- No network request needed — the field is always present.

**Why no cron / migration:**
- Computing at read time is always accurate (no stale data).
- No additional DB column to maintain.
- The 14-day window matches the curated bucket ILM policy exactly. If the policy changes, update the `timedelta` constant in one place.

---

## Summary Table

| # | Item | File | Type | Effort |
|---|------|------|------|--------|
| 1.1 | Mean vs Median — skewness check | `dq_logic.py` | Bug fix | **Done** |
| 1.2 | `drop_column` threshold — single 50% rule (no change needed, docs corrected) | — | Docs fix | **Done** |
| 1.3 | MAR detection moved to `build_recommendations()` | `dq_logic.py`, `llm_enrichment.py` | Refactor | **Done** |
| 1.4 | Outlier IQR bounds stored in profile | `dq_logic.py` | Enhancement | **Done** |
| 1.5 | Outlier recommendations in `build_recommendations()` | `dq_logic.py` | New logic | **Done** |
| 1.6 | Normalize suggestion logic broken (`col_min > 0`) | `dq_logic.py` | Bug fix | **Done** |
| 1.7 | Warning only fires for drop strategies | `dq_logic.py` | Enhancement | **Done** |
| 1.8 | Bool type detection missing "yes"/"no" variants | `dq_logic.py` | Bug fix | **Done** |
| 1.9 | Bool cast mapping silently drops unrecognised values | `dq_logic.py` | Bug fix | **Done** |
| 2.1 | Narrow LLM prompt — remove statistical bullets | `llm_enrichment.py` | Cleanup | **Done** |
| 2.2 | Date columns in wrong LLM profile table | `llm_enrichment.py` | Bug fix | **Done** |
| 2.3 | Remove CORRELATED NULLS from profile builder | `llm_enrichment.py` | Cleanup | **Done** |
| 2.4 | `max_tokens=512` may truncate wide datasets | `llm_enrichment.py` | Risk | **Done** |
| 2.5 | Fix stale "Planner" docstring | `dq_flow.py` | Trivial | **Done** |
| 2.6 | Column enrichment LLM: explicit prompt rule for `transform_hint` on format_inconsistency columns | `llm_enrichment.py` | Prompt update | **Done** |
| 3.1 | Add `"ignore"` to `DuplicatesConfig` | `file.py` | Schema fix | **Done** |
| 3.2 | Add `OutliersConfig` and `outliers` field | `file.py` | New schema | **Done** |
| 3.3 | `rename_to` not validated for identifier format | `file.py`, `llm_enrichment.py` | Schema fix | **Done** |
| 4.1 | Outlier treatment in `apply_recommendations()` | `dq_logic.py` | New feature | **Done** |
| 4.2 | Sentinel detection — string + numeric + sentinel warning | `dq_logic.py` | New feature | **Done** |
| 4.3 | Date ISO 8601 export format (+ NaT bug fix) | `dq_logic.py` | Enhancement | **Done** |
| 4.4 | Z-score normalization option | `dq_logic.py`, `file.py` | New feature | **Done** |
| 4.5 | Format inconsistency detection in profiler (partial castability) | `dq_logic.py` | New feature | **Done** |
| 4.6 | Destructive type cast guard in `apply_recommendations()` | `dq_logic.py` | Safety fix | **Done** |
| 4.7 | Sentinel threshold: frequency % → absolute count ≥ 5 | `dq_logic.py` | Bug fix | **Done** |
| 4.8 | Custom transform generation (two-LLM pipeline) | `llm_enrichment.py`, `dq_flow.py`, `transform_flow.py`, `dq_logic.py`, `file.py` | New feature | **Done** |
| 4.9 | String sentinel values in `sentinel_values` + `replace_sentinels` flag | `dq_logic.py`, `file.py`, `test_dq_logic.py`, frontend | Enhancement + UX | **Done** |
| 5.1 | Update `test_dq_logic.py` | `test_dq_logic.py` | Tests | **Done** |
| 5.2 | Update `test_llm_enrichment.py` | `test_llm_enrichment.py` | Tests | **Done** |
| 6.1 | Scope disclaimer in `recommendations.md` | `recommendations.md` | Docs | **Done** |
| 6.2 | Decision precedence hierarchy in `recommendations.md` | `recommendations.md` | Docs | **Done** |
| 6.3 | MAR detection renamed to heuristic co-occurrence in code | `llm_enrichment.py` | Docs | **Done** |
| 6.4 | Outlier univariate scope constraint in `recommendations.md` | `recommendations.md` | Docs | **Done** |
| L1 | Validity range violations — known limitation | thesis | Documentation | — |
| L2 | Cross-field consistency — known limitation | thesis | Documentation | — |
| F1 | Confidence level per recommendation | future | Future consideration | — |
| F2 | Numeric LLM confidence score | future | Future consideration | — |
| F3 | Log LLM overrides of deterministic baseline | `llm_enrichment.py` | Future (cheap) | **Done** |
| F4 | LLM reasoning for outlier strategy selection | `llm_enrichment.py`, `file.py` | Future enhancement | **Done** |
| F5 | rename_to v2 — opaque column names | `llm_enrichment.py` | Future enhancement | **Done** |
| UI1 | Data preview of applied recommendations (curated CSV first 50 rows) | `backend/app/api/runs.py`, `frontend/src/components/run/` | New feature | **Done** |
| UI2 | Dark / light mode toggle | `frontend/src/` | Enhancement | **Done** |
| UI3 | Issues detected grid in COMPLETED score card | `frontend/src/components/run/RunDetailView.tsx` | Enhancement | **Done** |
| UI4 | Score delta grey when 0 change | `frontend/src/components/scores/DQScoreBar.tsx` | Polish | **Done** |
| UI5 | Recommendation diff viewer (Generated / Applied / Diff tabs) | `frontend/src/components/run/RecsViewer.tsx` | New feature | **Done** |


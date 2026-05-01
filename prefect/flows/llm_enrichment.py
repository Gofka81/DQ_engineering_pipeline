import ast
import copy
import json
import logging
import os
import re
import time
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

_MODEL_ENRICHMENT = "llama-3.3-70b-versatile"
_MODEL_RENAME     = "llama-3.3-70b-versatile"
_MODEL_TRANSFORM  = "llama-3.3-70b-versatile"

# ---------------------------------------------------------------------------
# Prompt templates
# ---------------------------------------------------------------------------

_RUNNER_SYSTEM = """\
You are a data quality engineer. Given a dataset profile in CSV format \
(two tables — numeric columns and string columns — plus sample rows) and a \
baseline recommendations JSON, return ONLY the columns you want to improve — \
and within each column, ONLY the keys you are changing.

The baseline uses sparse format — fields absent from a column are at their \
default value: no nullable means no nulls; no missing_values means no fill \
needed; no normalize means normalization is not suggested. Absence is an \
explicit default, not missing information.

Before deciding on each column, briefly reason about its semantics:
- ID/key columns (high cardinality, names like *_id, *_key, *_code) — drop_row for nulls, never impute
- Pattern columns (email, phone, UUID, URL) — values cannot be invented or imputed; never use fill/mode/mean; choose drop_row if the row is useless without this field, leave_null if the row is still useful (e.g. a CRM contact without a phone number is still a valid contact)
- High-null event-date or optional-attribute columns (adv_evt_dt, incident_dt, resolved_at, notes) — nulls are by design → prefer leave_null
- Sample rows: spot sentinel strings ("N/A", "unknown", "none") — these are validity issues tracked in invalid count, NOT nulls; always verify null_pct > 0 before recommending any missing_values strategy

Output a JSON object with "columns" and/or "outliers" keys. Include only \
columns you are improving, and only outlier entries for columns with detected \
outliers in the baseline. Within each column include only the keys you are \
changing.

{{
  "columns": {{                                         (optional — only columns you are improving)
    "<col>": {{
      "type": "int|float|string|date|bool",            (optional — only if changing)
      "missing_values": {{"strategy": "median|mean|mode|fill|drop_row|drop_column|leave_null", "value": null}} or null,  (optional — only if changing)
      "transform_hint": "imperative action sentence citing actual values",   (optional — only when sample rows show a concrete value-level issue)
      "note": "one sentence explaining the semantic reasoning"               (required for every column you include)
    }}
  }},
  "outliers": {{                                        (optional — only for columns with detected outliers in the baseline)
    "<col>": {{
      "note": "one or two sentence domain analysis"    (required for every outlier column you include)
    }}
  }}
}}

Rules:
- ONLY include a column if your recommendation genuinely differs from the baseline — do not echo back unchanged columns
- Only include keys within a column that you are actually changing — do not include a key if its value is identical to the baseline
- Column names must exactly match the input column names
- For strategy "median": set value to the median from the numeric table (median column of that row)
- For strategy "mode": set value to the first entry from top_values in the string table (e.g. "FIN" from "FIN(2) MGT(2) HR(1)")
- strategy "fill" requires "value" to be non-null
- leave_null: use when nulls are intentional — appropriate for event-date or optional-attribute columns where null means "not applicable"
- drop_column: use only when a column is >50% missing AND the column has no domain significance
- rename_to: do not use — column renaming is handled separately
- missing_values: only recommend if null_pct > 0 — sentinel strings like "N/A" or "unknown" in sample rows are validity issues (tracked in invalid count), not nulls; never add missing_values for a column with null_pct = 0
- transform_hint: only valid for string-type columns — numeric, date, and bool columns are already correctly typed so there is no string formatting to fix; numeric range anomalies are handled by the outliers section. For string columns, add when sample rows show a concrete per-cell value-level issue that type-casting alone cannot fix. Two conditions must BOTH be true: (1) evidence is visible in the sample rows — cite the actual values you see, (2) the fix is a per-cell operation (regex, arithmetic, string split) that does not require knowing other rows. Write a single imperative sentence. Examples: "strip '%%' suffix from values like '12%%', '0.5%%' and divide by 100"; "extract numeric part from '180cm', '5ft9in' and convert all to cm"; "strip non-numeric characters from '~50', '100 approx' and cast to float"; "split 'New York, NY' pattern on ', ' into city and state".
- format inconsistency (MANDATORY): if a column's warnings list contains a message about "mixed value formats" or "not numeric-castable", you MUST add transform_hint for that column — do not skip it. The non-castable examples shown in the warning (e.g. '$1,200', 'N/A', 'TBD') tell you exactly what format to handle. Not adding transform_hint for a format-inconsistency column is an error.
- note is required for every column you include, but do NOT include a column solely to add a note — a note is only valid when you are also changing type, missing_values, or transform_hint
- outlier notes: for every column in the outliers section of the baseline, you MUST return an \
  outliers entry with a "note". Reason specifically about that column — do NOT use generic \
  phrases like "likely measurement errors" as a default. Address three things: \
  (1) plausibility of the IQR bounds — if a lower bound is negative for a quantity that cannot \
  be negative by definition, flag it; if the upper bound truncates values that are domain-normal \
  at extremes, say so; \
  (2) whether the outlier count and prevalence suggest systematic noise, a heavy-tailed \
  distribution, or genuine entry errors — cite the count and bounds; \
  (3) whether the suggested strategy is appropriate, and if not, which alternative fits better \
  and why. If the suggested strategy is already optimal given your reasoning, say why rather than \
  recommending a change. Do NOT include "strategy", "count", "lower", or "upper" in outlier \
  entries — those are read-only.
- Output ONLY the JSON object — no markdown fences, no commentary\
"""

_RUNNER_USER = """\
Dataset profile (CSV):

<profile>
{profile}
</profile>

Baseline recommendations:
<baseline>
{base_recs_json}
</baseline>

Return only the columns you want to improve and only the keys you are changing.\
"""

_RUNNER_RETRY_USER = """\
Dataset profile (CSV):

<profile>
{profile}
</profile>

Baseline recommendations:
<baseline>
{base_recs_json}
</baseline>

Your previous attempt (attempt {attempt}):
<previous_output>
{previous_output}
</previous_output>

That output failed validation with this error:
<validation_error>
{validation_error}
</validation_error>

Fix the error and return only the columns you want to improve.\
"""

_RENAME_SYSTEM = """\
You are a senior data engineer reviewing column names for clarity.
For each column you are given its name, detected type, and sample values from the actual data.
Rename columns so they are immediately clear to anyone reading the schema.

Rename a column only if BOTH conditions are true:
1. The name contains abbreviations or is cryptic
2. The actual sample values CONFIRM the implied meaning
   (e.g. a _dt column should actually contain date-like values, not codes or IDs)

When renaming, you may only expand abbreviations — never drop parts of the name.
Examples: brand_cd → brand_code; pat_id → patient_id; cmpny_nm → company_name

Well-known domain acronyms that should NOT be expanded: sku, uuid, url, api, sql, ip, atm.
Use your domain knowledge for everything else — if the sample values make sense for the implied meaning, rename it.

Before consider any column names try to reason about dataset domain and use this domain knowledge for column renaming.
If you are uncertain what a column represents, considering BOTH its name and its values in the context
of the other columns in this dataset, do not rename it. A wrong name is worse than an opaque one.

Return a JSON object: {"old_column_name": "new_name", ...}
Only include columns you are actually renaming. Return {} if no renames needed.
Output ONLY the JSON object — no markdown fences, no commentary.\
"""


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------

def _sample_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a representative sample of rows for LLM context.

    Size: min(10% of rows, 25), proportional for small datasets, capped at 25 for large ones.

    Strategy:
      1. Null rows      — rows with any null (up to n//3); show the LLM missing-data patterns
      2. Systematic     — every k-th row from the remainder for positional spread
      3. Random fill    — random from what remains to hit the target n
    """
    n_total = len(df)
    if n_total == 0:
        return df

    n = max(min(5, n_total), min(int(n_total * 0.10), 25))

    if n_total <= n:
        return df.copy()

    sampled_indices = set()

    # 1. Null rows
    null_mask = df.isna().any(axis=1)
    null_indices = df[null_mask].index.tolist()
    null_budget = max(1, n // 3)
    if null_indices:
        step = max(1, len(null_indices) // null_budget)
        for i in range(0, len(null_indices), step):
            sampled_indices.add(null_indices[i])
            if len(sampled_indices) >= null_budget:
                break

    # 2. Systematic from the rest
    remaining_indices = [i for i in df.index if i not in sampled_indices]
    systematic_budget = n - len(sampled_indices)
    if remaining_indices and systematic_budget > 0:
        step = max(1, len(remaining_indices) // systematic_budget)
        for i in range(0, len(remaining_indices), step):
            sampled_indices.add(remaining_indices[i])
            if len(sampled_indices) >= n:
                break

    # 3. Random fill if still short
    if len(sampled_indices) < n:
        leftover = [i for i in df.index if i not in sampled_indices]
        still_needed = n - len(sampled_indices)
        if leftover:
            random_picks = (
                pd.Series(leftover)
                .sample(min(still_needed, len(leftover)), random_state=42)
                .tolist()
            )
            sampled_indices.update(random_picks)

    return df.loc[sorted(sampled_indices)].copy()


# ---------------------------------------------------------------------------
# Profile builder
# ---------------------------------------------------------------------------

def _fmt(x: float | str | None) -> str:
    """Format a number or date string for the LLM profile CSV. None → empty string."""
    if x is None:
        return ""
    if isinstance(x, str):
        return x
    return str(int(x)) if x == int(x) else f"{x:.1f}"


def _build_llm_profile(profile: dict[str, Any], df: pd.DataFrame) -> str:
    """
    Build a compact CSV profile string for the LLM prompt (~4x smaller than JSON).

    Sections:
    1. Dataset-level stats (single header + data row)
    2. Numeric/date columns table (name, type, null_pct, mean, median, std, min, max, outliers)
    3. String/other columns table (name, type, null_pct, unique, cardinality_pct, top_values, pattern, invalid)
    4. SAMPLE ROWS (CSV, stratified sample)

    Does not mutate the input profile.
    top_values trimmed to 3 entries; space-separated as "val(count) val(count) val(count)".
    """
    lines: list[str] = []

    # 1. Dataset header
    lines.append("rows,cols,missing,duplicates,completeness,uniqueness,validity,consistency")
    lines.append(
        f"{profile['total_rows']},{profile['total_columns']},{profile['missing_cells']},"
        f"{profile['duplicate_rows']},{round(profile['completeness'], 1)},"
        f"{round(profile['uniqueness'], 1)},{round(profile['validity'], 1)},"
        f"{round(profile['consistency'], 1)}"
    )
    lines.append("")

    # Split columns by detected type.
    # Date columns share the numeric profile shape (stats: min/max/mean) so
    # they go into the numeric table, not the string table which has no stats.
    numeric = {
        col: cp for col, cp in profile["column_profiles"].items()
        if cp.get("detected_type") in ("numeric", "date")
    }
    other = {
        col: cp for col, cp in profile["column_profiles"].items()
        if cp.get("detected_type") not in ("numeric", "date")
    }

    # 2. Numeric columns table
    if numeric:
        lines.append("name,type,null_pct,mean,median,std,min,max,outliers,outlier_lower,outlier_upper")
        for col, cp in numeric.items():
            s = cp.get("stats", {})
            out = cp.get("outliers", {})
            out_count = out.get("count", 0)
            out_lower = _fmt(out.get("lower")) if out_count > 0 else ""
            out_upper = _fmt(out.get("upper")) if out_count > 0 else ""
            lines.append(
                f"{col},{cp['detected_type']},{_fmt(cp['null_pct'])},"
                f"{_fmt(s.get('mean'))},{_fmt(s.get('median'))},{_fmt(s.get('std'))},"
                f"{_fmt(s.get('min'))},{_fmt(s.get('max'))},{out_count},"
                f"{out_lower},{out_upper}"
            )
        lines.append("")

    # 3. String/other columns table
    if other:
        lines.append("name,type,null_pct,unique,cardinality_pct,top_values,pattern,invalid")
        for col, cp in other.items():
            top3 = list(cp.get("top_values", {}).items())[:3]
            top_str = " ".join(f"{v}({n})" for v, n in top3)
            lines.append(
                f"{col},{cp['detected_type']},{_fmt(cp['null_pct'])},"
                f"{cp.get('unique_count', '')},"
                f"{_fmt(cp.get('cardinality_pct', 0))},"
                f"{top_str},{cp.get('pattern') or ''},{cp.get('invalid_count', 0)}"
            )
        lines.append("")

    # 4. Sample rows
    sample_df = _sample_rows(df)
    lines.append("SAMPLE ROWS:")
    lines.append(sample_df.fillna("").to_csv(index=False).strip())

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Lean baseline builder
# ---------------------------------------------------------------------------

def _lean_baseline(recs: dict[str, Any]) -> dict[str, Any]:
    """
    Build a sparse version of the baseline recommendations for the LLM prompt.

    The "columns" and "outliers" top-level keys are included. The LLM uses
    both to enrich column strategies and add domain notes to outlier entries.
    _eda, _metadata, duplicates, custom_transforms are excluded.

    Per-column fields at their default value are omitted:
      nullable       → omitted when False
      missing_values → omitted when None; "value" key omitted when None
      normalize      → omitted when False
      warnings       → omitted when empty
      rename_to      → always omitted (LLM-set, always None in baseline)
      note           → always omitted (LLM-set, always None in baseline)

    Outlier entries include strategy/count/bounds only (note is LLM-set).

    Also switches to compact JSON (no indent). Caller passes the result
    to json.dumps with separators=(",",":").
    """
    columns: dict[str, Any] = {}
    for col, col_def in recs.get("columns", {}).items():
        lean: dict[str, Any] = {"type": col_def["type"]}

        if col_def.get("nullable"):
            lean["nullable"] = True

        mv = col_def.get("missing_values")
        if mv is not None:
            lean_mv: dict[str, Any] = {"strategy": mv["strategy"]}
            if mv.get("value") is not None:
                lean_mv["value"] = mv["value"]
            lean["missing_values"] = lean_mv

        norm = col_def.get("normalize")
        if norm:
            lean["normalize"] = norm

        warnings = col_def.get("warnings", [])
        if warnings:
            lean["warnings"] = warnings

        columns[col] = lean

    outliers: dict[str, Any] = {}
    for col, od in recs.get("outliers", {}).items():
        outliers[col] = {
            "strategy": od["strategy"],
            "count": od["count"],
            "lower": od.get("lower"),
            "upper": od.get("upper"),
        }

    if outliers:
        return {"columns": columns, "outliers": outliers}
    return {"columns": columns}


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def _runner(
        client,
        llm_profile: str,
        base_recs: dict[str, Any],
        previous_output: str,
        validation_error: str,
        attempt: int,
) -> str:
    """
    One LLM call to generate (or fix) the partial recommendations diff.

    Returns a raw string that may or may not be valid JSON.
    Exceptions propagate to the caller.
    """
    base_recs_json = json.dumps(_lean_baseline(base_recs), separators=(",", ":"), default=str)

    if attempt == 0:
        user_content = _RUNNER_USER.format(
            profile=llm_profile,
            base_recs_json=base_recs_json,
        )
    else:
        user_content = _RUNNER_RETRY_USER.format(
            profile=llm_profile,
            base_recs_json=base_recs_json,
            attempt=attempt,
            previous_output=previous_output,
            validation_error=validation_error,
        )

    logger.debug("LLM_SYSTEM_PROMPT\n%s", _RUNNER_SYSTEM)
    logger.debug("LLM_USER_PROMPT attempt=%d\n%s", attempt + 1, user_content)

    # Budget ~30 tokens per column for the diff JSON, minimum 512, cap at 2048.
    max_tokens = min(512 + len(base_recs.get("columns", {})) * 30, 2048)
    logger.info(
        "LLM enrichment request | model=%s max_tokens=%d attempt=%d prompt_chars=%d",
        _MODEL_ENRICHMENT, max_tokens, attempt + 1, len(user_content),
    )
    t0 = time.time()
    response = client.chat.completions.create(
        model=_MODEL_ENRICHMENT,
        max_tokens=max_tokens,
        temperature=0,
        messages=[
            {"role": "system", "content": _RUNNER_SYSTEM},
            {"role": "user", "content": user_content},
        ],
    )
    elapsed = time.time() - t0
    raw = response.choices[0].message.content.strip()
    usage = response.usage
    logger.info(
        "LLM enrichment response | attempt=%d elapsed=%.2fs tokens_in=%d tokens_out=%d",
        attempt + 1, elapsed,
        usage.prompt_tokens if usage else -1,
        usage.completion_tokens if usage else -1,
    )
    logger.info("LLM enrichment raw output attempt=%d:\n%s", attempt + 1, raw)
    logger.debug("LLM_RESPONSE attempt=%d\n%s", attempt + 1, raw)
    return raw


# ---------------------------------------------------------------------------
# Validator
# _RUNNER_SYSTEM schema must stay in sync with validate_llm_output().
# ---------------------------------------------------------------------------

_VALID_TYPES = {"int", "float", "string", "date", "bool"}
_VALID_STRATEGIES = {"median", "mean", "mode", "fill", "drop_row", "drop_column", "leave_null"}


def validate_llm_output(
        data: dict[str, Any],
        known_columns: set[str],
        known_outlier_columns: set[str] = frozenset(),
) -> tuple[bool, str]:
    """
    Structural validation of the LLM partial diff.

    Accepts {"columns": {...}, "outliers": {...}}. At least one key is required.
    Only changed columns/outlier entries are expected; only changed keys within
    each column.

    Collects ALL errors before returning so the retry prompt gets the full
    picture in one shot rather than one error at a time.

    Returns (True, "") on success or (False, "<all errors joined>") on failure.
    The error string is fed verbatim into the next retry prompt.
    """
    errors: list[str] = []

    has_columns = "columns" in data
    has_outliers = "outliers" in data

    if not has_columns and not has_outliers:
        return False, "Response must contain at least one of 'columns' or 'outliers'"

    if has_columns:
        columns = data["columns"]
        if not isinstance(columns, dict):
            return False, "'columns' must be a dict"

        for col, col_def in columns.items():
            if col not in known_columns:
                errors.append(f"columns['{col}'] — unknown column (not in dataset)")
                continue  # skip further checks for this col, name is wrong

            if not isinstance(col_def, dict):
                errors.append(f"columns['{col}'] must be a dict")
                continue

            if not col_def.get("note"):
                errors.append(f"columns['{col}'] missing 'note' — required for every changed column")

            if "type" in col_def and col_def["type"] not in _VALID_TYPES:
                errors.append(
                    f"columns['{col}'].type='{col_def['type']}' invalid; "
                    f"must be one of {sorted(_VALID_TYPES)}"
                )

            mv = col_def.get("missing_values")
            if mv is not None:
                if not isinstance(mv, dict):
                    errors.append(f"columns['{col}'].missing_values must be a dict or null")
                else:
                    strategy = mv.get("strategy")
                    if strategy not in _VALID_STRATEGIES:
                        errors.append(
                            f"columns['{col}'].missing_values.strategy='{strategy}' invalid; "
                            f"must be one of {sorted(_VALID_STRATEGIES)}"
                        )
                    if strategy == "fill" and mv.get("value") is None:
                        errors.append(
                            f"columns['{col}'].missing_values strategy='fill' "
                            f"but 'value' is null — provide a non-null fill value"
                        )

            if "transform_hint" in col_def:
                th = col_def["transform_hint"]
                if not isinstance(th, str) or not th.strip():
                    errors.append(
                        f"columns['{col}'].transform_hint must be a non-empty string"
                    )

    if has_outliers:
        outliers_section = data["outliers"]
        if not isinstance(outliers_section, dict):
            errors.append("'outliers' must be a dict")
        else:
            for col, outlier_def in outliers_section.items():
                if col not in known_outlier_columns:
                    errors.append(f"outliers['{col}'] — not a known outlier column")
                    continue
                if not isinstance(outlier_def, dict):
                    errors.append(f"outliers['{col}'] must be a dict")
                    continue
                if not outlier_def.get("note"):
                    errors.append(f"outliers['{col}'] missing 'note' — required")
                for forbidden in ("strategy", "count", "lower", "upper"):
                    if forbidden in outlier_def:
                        errors.append(
                            f"outliers['{col}'] must not contain '{forbidden}' — read-only field"
                        )

    if errors:
        return False, "\n".join(f"- {e}" for e in errors)
    return True, ""


# ---------------------------------------------------------------------------
# Rename runner (separate LLM call)
# ---------------------------------------------------------------------------

def _rename_runner(
        client,
        df: pd.DataFrame,
        profile: dict[str, Any],
) -> dict[str, str]:
    """
    Separate LLM call for column renaming.

    Builds per-column input: "col (type): val1, val2, val3"
    Returns {old_col: new_name} for columns that should be renamed.
    Returns an empty dict on any failure. Never raises.
    """
    known_columns = set(profile["column_profiles"].keys())

    lines = []
    for col, cp in profile["column_profiles"].items():
        if col not in df.columns:
            continue
        col_type = cp.get("detected_type", "string")
        vals = df[col].dropna().head(3).astype(str).tolist()
        val_str = ", ".join(vals) if vals else "(no values)"
        lines.append(f"{col} ({col_type}): {val_str}")

    if not lines:
        return {}

    user_content = "\n".join(lines)
    max_tokens = min(128 + len(known_columns) * 15, 512)

    logger.info("LLM rename request | model=%s columns=%d", _MODEL_RENAME, len(lines))
    try:
        t0 = time.time()
        response = client.chat.completions.create(
            model=_MODEL_RENAME,
            max_tokens=max_tokens,
            temperature=0,
            messages=[
                {"role": "system", "content": _RENAME_SYSTEM},
                {"role": "user", "content": user_content},
            ],
        )
        elapsed = time.time() - t0
        raw = response.choices[0].message.content.strip()
        usage = response.usage
        logger.info(
            "LLM rename response | elapsed=%.2fs tokens_in=%d tokens_out=%d",
            elapsed,
            usage.prompt_tokens if usage else -1,
            usage.completion_tokens if usage else -1,
        )
    except Exception as e:
        logger.warning("LLM rename call failed: %s — skipping renames", e)
        return {}

    # Strip markdown fences if present
    clean = raw
    if clean.startswith("```"):
        fence_lines = clean.splitlines()
        fence_lines = fence_lines[1:] if fence_lines else fence_lines
        if fence_lines and fence_lines[-1].strip() == "```":
            fence_lines = fence_lines[:-1]
        clean = "\n".join(fence_lines).strip()

    try:
        result = json.loads(clean)
    except json.JSONDecodeError as e:
        logger.warning("LLM rename returned invalid JSON: %s — skipping renames", e)
        return {}

    if not isinstance(result, dict):
        logger.warning("LLM rename returned non-dict — skipping renames")
        return {}

    validated: dict[str, str] = {}
    for old_name, new_name in result.items():
        if old_name not in known_columns:
            logger.warning("LLM rename: unknown column '%s' — skipping", old_name)
            continue
        if not isinstance(new_name, str) or not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", new_name):
            logger.warning("LLM rename: invalid identifier '%s' for '%s' — skipping", new_name, old_name)
            continue
        if old_name == new_name:
            logger.debug("LLM rename: skipping identity rename '%s'", old_name)
            continue
        validated[old_name] = new_name
        logger.info("LLM rename | %s → %s", old_name, new_name)

    return validated


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def enrich_recommendations(
        profile: dict[str, Any],
        base_recommendations: dict[str, Any],
        df: pd.DataFrame,
) -> dict[str, Any]:
    """
    Enrich code-generated recommendations using an LLM (Runner → Validator → merge).

    The LLM returns a partial diff of only changed columns and only changed keys.
    We deep-merge that diff onto the baseline so _metadata, duplicates, and
    custom_transforms always come from the baseline.

    Never raises (soft failure). Returns base_recommendations if:
    - LLM_API_KEY is not set
    - groq package is not installed
    - All 3 runner attempts produce invalid output
    """
    api_key = os.getenv("LLM_API_KEY", "").strip()
    if not api_key:
        logger.warning("LLM_API_KEY not set — skipping LLM enrichment, using baseline")
        return base_recommendations

    try:
        from groq import Groq
    except ImportError:
        logger.warning("groq package not installed — skipping LLM enrichment")
        return base_recommendations

    client = Groq(api_key=api_key)
    llm_profile = _build_llm_profile(profile, df)
    known_columns = set(profile["column_profiles"].keys())

    previous_output = ""
    validation_error = ""

    for attempt in range(3):
        logger.info(f"LLM Runner attempt {attempt + 1}/3...")
        try:
            raw = _runner(
                client, llm_profile, base_recommendations,
                previous_output, validation_error, attempt,
            )
        except Exception as e:
            # Back off before retrying on rate-limit errors so we don't
            # immediately hammer the API again after the SDK's own retries
            # are exhausted.
            is_rate_limit = "rate" in str(e).lower() or "429" in str(e)
            backoff = 10 * (attempt + 1) if is_rate_limit else 0
            logger.warning(
                f"Runner attempt {attempt + 1} raised {type(e).__name__}: {e}"
                + (f" — backing off {backoff}s" if backoff else "")
            )
            if backoff:
                time.sleep(backoff)
            previous_output = ""
            validation_error = str(e)
            continue

        # Strip markdown fences if present
        clean = raw
        if clean.startswith("```"):
            lines = clean.splitlines()
            lines = lines[1:] if lines else lines
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            clean = "\n".join(lines).strip()

        try:
            llm_diff = json.loads(clean)
        except json.JSONDecodeError as e:
            logger.warning(f"Runner attempt {attempt + 1} produced invalid JSON: {e}")
            previous_output = raw
            validation_error = f"JSONDecodeError: {e}"
            continue

        known_outlier_columns = set(base_recommendations.get("outliers", {}).keys())
        if isinstance(llm_diff.get("outliers"), dict):
            llm_diff["outliers"] = {
                col: outlier_def
                for col, outlier_def in llm_diff["outliers"].items()
                if col in known_outlier_columns
            }
        valid, err = validate_llm_output(llm_diff, known_columns, known_outlier_columns)
        if not valid:
            logger.warning(f"Runner attempt {attempt + 1} failed validation: {err}")
            previous_output = raw
            validation_error = err
            continue

        llm_diff.setdefault("columns", {})

        # Strip keys that are identical to the baseline (LLM sometimes echoes unchanged keys)
        for col, col_diff in llm_diff["columns"].items():
            if col in base_recommendations["columns"]:
                base_col = base_recommendations["columns"][col]
                echoed = [k for k, v in col_diff.items() if k != "note" and base_col.get(k) == v]
                for k in echoed:
                    del col_diff[k]

        # Strip missing_values when baseline has null. Baseline is authoritative for zero-null columns.
        # The LLM sometimes adds an imputation strategy for zero-null columns.
        for col, col_diff in llm_diff["columns"].items():
            if "missing_values" in col_diff:
                base_col = base_recommendations["columns"].get(col, {})
                if base_col.get("missing_values") is None:
                    logger.debug("Stripped missing_values for '%s' — baseline has no nulls", col)
                    del col_diff["missing_values"]

        # Protect leave_null set by MAR detection. Code precedence is higher than LLM.
        # The LLM cannot override leave_null to a fill strategy; it can still set leave_null
        # itself (making a non-MAR column intentionally null), but cannot undo a MAR decision.
        for col, col_diff in llm_diff["columns"].items():
            if "missing_values" in col_diff:
                base_col = base_recommendations["columns"].get(col, {})
                base_strategy = (base_col.get("missing_values") or {}).get("strategy")
                if base_strategy == "leave_null":
                    logger.debug("Protected leave_null for '%s' — MAR-detected, LLM cannot override", col)
                    del col_diff["missing_values"]

        # Drop columns where only a note remains (note without a real change is noise)
        # and columns where nothing changed at all.
        # transform_hint counts as a substantive change. A column with only transform_hint + note is kept.
        _SUBSTANTIVE_KEYS = {"type", "nullable", "missing_values", "transform_hint"}
        llm_diff["columns"] = {
            col: col_diff
            for col, col_diff in llm_diff["columns"].items()
            if col_diff and (set(col_diff.keys()) & _SUBSTANTIVE_KEYS)
        }

        # --- Separate rename call (split approach) ---
        # Strip any stray rename_to the strategy call produced (model sometimes ignores the rule)
        for col_diff in llm_diff["columns"].values():
            col_diff.pop("rename_to", None)

        # Deep merge: baseline + LLM diff (only changed cols/keys)
        enriched = copy.deepcopy(base_recommendations)
        for col, col_diff in llm_diff["columns"].items():
            if col in enriched["columns"]:
                for key, val in col_diff.items():
                    if (
                        key == "missing_values"
                        and val is not None
                        and isinstance(enriched["columns"][col].get("missing_values"), dict)
                    ):
                        # Merge sub-keys so null_count (and other baseline fields) survive
                        enriched["columns"][col]["missing_values"].update(val)
                    else:
                        enriched["columns"][col][key] = val
                # Clear stale warnings when LLM changed the strategy. The old warnings
                # were generated for the baseline strategy and are now contradictory.
                if "missing_values" in col_diff:
                    enriched["columns"][col]["warnings"] = []

        # Merge outlier notes from LLM. Strategy, count, and bounds are read-only.
        for col, outlier_diff in llm_diff.get("outliers", {}).items():
            if col in enriched["outliers"]:
                note = outlier_diff.get("note")
                if note:
                    # Guard: strip self-referential "consider X instead of X" suffix produced
                    # when the heuristic strategy already matches what the LLM would suggest.
                    strategy = enriched["outliers"][col]["strategy"]
                    tautology = f"consider '{strategy}' instead of '{strategy}'"
                    if tautology in note:
                        note = note[:note.index(tautology)].rstrip(" —–-")
                    if note:
                        enriched["outliers"][col]["note"] = note

        # Apply renames from dedicated rename call
        renames = _rename_runner(client, df, profile)
        for col, new_name in renames.items():
            if col in enriched["columns"]:
                enriched["columns"][col]["rename_to"] = new_name

        n_changed = len(llm_diff["columns"])
        n_outlier_notes = sum(1 for od in llm_diff.get("outliers", {}).values() if od.get("note"))
        logger.info(
            "LLM enrichment succeeded on attempt %d. columns_changed=%d outlier_notes=%d renames=%d",
            attempt + 1, n_changed, n_outlier_notes, len(renames),
        )

        # Log each override so it's easy to see what the LLM actually changed.
        # Format: col | key: <before> → <after>
        # For missing_values we show the strategy string, not the full dict.
        for col, col_diff in llm_diff["columns"].items():
            base_col = base_recommendations["columns"].get(col, {})
            changes = []
            for key, after in col_diff.items():
                if key == "note":
                    continue
                before = base_col.get(key)
                if key == "missing_values":
                    before = (before or {}).get("strategy")
                    after  = (after  or {}).get("strategy") if isinstance(after, dict) else after
                changes.append(f"{key}: {before!r} → {after!r}")
            if changes:
                logger.info("LLM override | %s | %s", col, " | ".join(changes))

        return enriched

    logger.error("LLM enrichment: all 3 attempts exhausted — falling back to baseline recommendations")
    return base_recommendations


# ---------------------------------------------------------------------------
# Custom transform code generation
# ---------------------------------------------------------------------------

_TRANSFORM_SYSTEM = """\
You are a pandas data transformation expert. Given a column name, target type, \
sample values, and a plain-English transform description, output ONLY a Python \
lambda expression — nothing else.

Format: lambda col: <pandas Series expression>

Rules:
- Only use pandas Series methods and arithmetic operators
- No imports, no multi-line statements, no eval/exec/open
- No access to dunder attributes (__class__, __dict__, etc.)
- The input `col` is a pandas Series; return a pandas Series
- The result Series must have the same length as the input

Example:
  Column: price, type: float, hint: strip '%' suffix and divide by 100
  Output: lambda col: col.str.rstrip('%').astype(float) / 100

Output ONLY the lambda expression — no markdown, no commentary, no explanation.\
"""

_FORBIDDEN_NAMES = frozenset({
    "eval", "exec", "open", "__import__", "compile", "globals", "locals",
    "getattr", "setattr", "delattr", "vars", "dir", "type", "input",
    "print", "os", "sys", "subprocess",
})


def _validate_transform_ast(code: str) -> tuple[bool, str]:
    """
    Static safety check for a transform lambda string.

    Rejects:
    - Code that doesn't parse as a single expression (mode='eval')
    - Import statements (ast.Import / ast.ImportFrom nodes)
    - Calls to forbidden builtins (eval, exec, open, __import__, etc.)
    - Dunder attribute access (__class__, __dict__, etc.)

    Returns (True, "") on success or (False, reason) on failure.
    """
    try:
        tree = ast.parse(code, mode="eval")
    except SyntaxError as e:
        return False, f"SyntaxError: {e}"

    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            return False, "Import statements not allowed in transform lambda"
        if isinstance(node, ast.Name) and node.id in _FORBIDDEN_NAMES:
            return False, f"Forbidden name: '{node.id}'"
        if isinstance(node, ast.Attribute) and node.attr.startswith("__"):
            return False, f"Dunder attribute access not allowed: '{node.attr}'"

    return True, ""


_TRANSFORM_TEST_GLOBALS = {
    "__builtins__": {},
    "pd": pd, "re": re,
    "str": str, "int": int, "float": float,
    "len": len, "abs": abs, "round": round, "min": min, "max": max,
}


def _test_transform(code: str, series: pd.Series) -> tuple[bool, str]:
    """
    Execute a transform lambda against a real Series and check for damage.

    Stages (each logged at DEBUG):
      1. compile  — ast.parse + compile to bytecode
      2. execute  — call fn(series)
      3. type     — result must be pd.Series
      4. length   — result must have same row count as input
      5. damage   — new NaN rate ≤ 5% of original non-null values
      6. nulls    — original null positions must remain null (catches NaN → "None" string)
      7. ok       — all checks passed

    Returns (True, "") on success or (False, reason) on failure. Never raises.
    """
    n_rows = len(series)
    n_nonnull = int(series.notna().sum())
    logger.debug("_test_transform | compile  | rows=%d non-null=%d | %s", n_rows, n_nonnull, code)

    try:
        fn = eval(
            compile(ast.parse(code, mode="eval"), "<transform>", "eval"),
            _TRANSFORM_TEST_GLOBALS,
        )
    except Exception as e:
        reason = f"{type(e).__name__}: {e}"
        logger.debug("_test_transform | compile  | FAIL | %s", reason)
        return False, reason

    logger.debug("_test_transform | execute  | calling fn(series)")
    try:
        result = fn(series)
    except Exception as e:
        reason = f"{type(e).__name__}: {e}"
        logger.debug("_test_transform | execute  | FAIL | %s", reason)
        return False, reason

    logger.debug("_test_transform | type     | got %s", type(result).__name__)
    if not isinstance(result, pd.Series):
        reason = f"Result must be pd.Series, got {type(result).__name__}"
        logger.debug("_test_transform | type     | FAIL | %s", reason)
        return False, reason

    logger.debug("_test_transform | length   | expected=%d got=%d", n_rows, len(result))
    if len(result) != n_rows:
        reason = f"Length mismatch: expected {n_rows}, got {len(result)}"
        logger.debug("_test_transform | length   | FAIL | %s", reason)
        return False, reason

    original_nulls = series.isna().sum()
    new_null_increase = (result.isna().sum() - original_nulls) / max(n_nonnull, 1)
    logger.debug(
        "_test_transform | damage   | original_nulls=%d result_nulls=%d new_null_rate=%.2f%%",
        original_nulls, result.isna().sum(), new_null_increase * 100,
    )
    if new_null_increase > 0.05:
        reason = f"Excess new nulls: {new_null_increase:.2%} of non-null values became null"
        logger.debug("_test_transform | damage   | FAIL | %s", reason)
        return False, reason

    original_null_mask = series.isna()
    if original_null_mask.any():
        n_corrupted = int((~result[original_null_mask].isna()).sum())
        logger.debug(
            "_test_transform | nulls    | original null positions filled: %d", n_corrupted,
        )
        if n_corrupted > 0:
            reason = f"Transform filled {n_corrupted} original null(s) — null positions must be preserved (hint: avoid .astype(str) which converts NaN → 'None')"
            logger.debug("_test_transform | nulls    | FAIL | %s", reason)
            return False, reason

    logger.debug("_test_transform | ok       | result non-null=%d", int(result.notna().sum()))
    return True, ""


def generate_transform_code(
    recommendations: dict[str, Any],
    df: pd.DataFrame,
) -> dict[str, Any]:
    """
    For each column with a transform_hint, call the LLM to generate a validated
    pandas lambda, then store it as transform_code in the column dict.

    Soft-fail contract. Never raises. Returns recommendations unchanged if:
    - LLM_API_KEY is not set
    - groq package is not installed
    - All 3 attempts fail for a column (that column is skipped silently)

    transform_code is generated at transform time and never persisted to DB.
    The user reviews transform_hint; the generated lambda is an implementation detail.
    """
    api_key = os.getenv("LLM_API_KEY", "").strip()
    if not api_key:
        logger.warning("LLM_API_KEY not set — skipping custom transform generation")
        return recommendations

    try:
        from groq import Groq
    except ImportError:
        logger.warning("groq package not installed — skipping custom transform generation")
        return recommendations

    client = Groq(api_key=api_key)
    recs = copy.deepcopy(recommendations)

    for col, col_def in recs.get("columns", {}).items():
        transform_hint = col_def.get("transform_hint")
        if not transform_hint:
            continue
        if col not in df.columns:
            continue

        sample = df[col].dropna().head(10).tolist()
        target_type = col_def.get("type", "string")
        null_pct = df[col].isna().mean() * 100
        nullable_line = f"yes ({null_pct:.1f}% null — NaN cells arrive as float in .apply(); guard with: <expr> if pd.notna(x) else x)" if null_pct > 0 else "no"
        previous_code = ""
        previous_error = ""

        for attempt in range(3):
            if attempt == 0:
                user_content = (
                    f"Column: {col}\n"
                    f"Target type: {target_type}\n"
                    f"Nullable: {nullable_line}\n"
                    f"Sample values: {sample}\n"
                    f"Transform: {transform_hint}"
                )
            else:
                user_content = (
                    f"Column: {col}\n"
                    f"Target type: {target_type}\n"
                    f"Nullable: {nullable_line}\n"
                    f"Sample values: {sample}\n"
                    f"Transform: {transform_hint}\n\n"
                    f"Your previous attempt:\n{previous_code}\n\n"
                    f"Error:\n{previous_error}\n\n"
                    f"Fix the error and return only the corrected lambda."
                )

            logger.info(
                "Transform LLM request | col=%s attempt=%d/3 hint=%r",
                col, attempt + 1, transform_hint,
            )
            logger.debug("Transform LLM user prompt | col=%s:\n%s", col, user_content)
            try:
                t0 = time.time()
                response = client.chat.completions.create(
                    model=_MODEL_TRANSFORM,
                    max_tokens=128,
                    temperature=0,
                    messages=[
                        {"role": "system", "content": _TRANSFORM_SYSTEM},
                        {"role": "user", "content": user_content},
                    ],
                )
                elapsed = time.time() - t0
                raw = response.choices[0].message.content.strip()
                usage = response.usage
                logger.info(
                    "Transform LLM response | col=%s attempt=%d elapsed=%.2fs tokens_in=%d tokens_out=%d | %s",
                    col, attempt + 1, elapsed,
                    usage.prompt_tokens if usage else -1,
                    usage.completion_tokens if usage else -1,
                    raw,
                )
            except Exception as e:
                logger.warning(
                    "Transform code gen attempt %d/3 for '%s' raised: %s",
                    attempt + 1, col, e,
                )
                previous_code = ""
                previous_error = str(e)
                time.sleep(12)
                continue

            time.sleep(12)

            # Strip markdown fences if present
            code = raw
            if code.startswith("```"):
                lines = code.splitlines()
                lines = lines[1:] if lines else lines
                if lines and lines[-1].strip() == "```":
                    lines = lines[:-1]
                code = "\n".join(lines).strip()

            logger.info("Transform code | %s | attempt %d | %s", col, attempt + 1, code)

            valid, err = _validate_transform_ast(code)
            if not valid:
                logger.warning(
                    "Transform code for '%s' failed AST validation (attempt %d): %s",
                    col, attempt + 1, err,
                )
                previous_code = code
                previous_error = f"AST validation failed: {err}"
                continue

            ok, err = _test_transform(code, df[col].head(200))
            if not ok:
                logger.warning(
                    "Transform code for '%s' failed execution test (attempt %d): %s",
                    col, attempt + 1, err,
                )
                previous_code = code
                null_hint = f" (column is {null_pct:.1f}% null — check the Nullable line above and add a null guard)" if null_pct > 0 else ""
                previous_error = f"Execution test failed: {err}{null_hint}"
                continue

            col_def["transform_code"] = code
            break
        else:
            logger.warning(
                "Transform code gen for '%s': all 3 attempts failed — skipping", col,
            )

    return recs

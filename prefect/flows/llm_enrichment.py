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

_MODEL = "llama-3.3-70b-versatile"

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
- Abbreviated/cryptic column names (e.g. sal, dept_cd, emp_no) — add rename_to with clearer name
- High-null event-date or optional-attribute columns (adv_evt_dt, incident_dt, resolved_at, notes) — nulls are by design → prefer leave_null
- Sample rows: spot sentinel strings ("N/A", "unknown", "none") — these are validity issues tracked in invalid count, NOT nulls; always verify null_pct > 0 before recommending any missing_values strategy

Output a JSON object with a single "columns" key. Include only columns you \
are improving. Within each column include only the keys you are changing.

{{
  "columns": {{
    "<col>": {{
      "type": "int|float|string|date|bool",            (optional — only if changing)
      "missing_values": {{"strategy": "median|mean|mode|fill|drop_row|drop_column|leave_null", "value": null}} or null,  (optional — only if changing)
      "rename_to": "clearer_name",                      (optional — only for abbreviated/cryptic names)
      "note": "one sentence explaining the change"      (required for every column you include)
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
- rename_to: when the column name would be clearer based on the actual data values in sample rows (e.g. a column "name" containing "John Smith" could be "full_name"; "sal" → "salary"; "dept_cd" → "department_code"); do NOT change _id columns to _number — identifiers and numbers are different concepts
- missing_values: only recommend if null_pct > 0 — sentinel strings like "N/A" or "unknown" in sample rows are validity issues (tracked in invalid count), not nulls; never add missing_values for a column with null_pct = 0
- format inconsistency: if a column's warnings mention "mixed value formats" or "not numeric-castable", do NOT change its type — instead add an entry to custom_transforms describing the normalization needed (e.g. "strip % suffix and divide by 100"); type casts are for true type mismatches only, not format normalization
- note is required for every column you include, but do NOT include a column solely to add a note — a note is only valid when you are also changing type, missing_values, or rename_to
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


# ---------------------------------------------------------------------------
# Sampling
# ---------------------------------------------------------------------------

def _sample_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a representative sample of rows for LLM context.

    Size: min(10% of rows, 25) — proportional for small datasets, capped for large ones.

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
    # they go into the numeric table — not the string table which has no stats.
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
        lines.append("name,type,null_pct,mean,median,std,min,max,outliers")
        for col, cp in numeric.items():
            s = cp.get("stats", {})
            lines.append(
                f"{col},{cp['detected_type']},{_fmt(cp['null_pct'])},"
                f"{_fmt(s.get('mean'))},{_fmt(s.get('median'))},{_fmt(s.get('std'))},"
                f"{_fmt(s.get('min'))},{_fmt(s.get('max'))},{cp['outliers']['count']}"
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

    Only the "columns" top-level key is included — the LLM does not touch
    duplicates, outliers, custom_transforms, or _metadata.

    Per-column fields at their default value are omitted:
      nullable       → omitted when False
      missing_values → omitted when None; "value" key omitted when None
      normalize      → omitted when False
      warnings       → omitted when empty
      rename_to      → always omitted (LLM-set, always None in baseline)
      note           → always omitted (LLM-set, always None in baseline)

    Also switches to compact JSON (no indent) — caller passes the result
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

    Returns raw string — may or may not be valid JSON.
    Lets exceptions propagate — caller handles.
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
    response = client.chat.completions.create(
        model=_MODEL,
        max_tokens=max_tokens,
        temperature=0,
        messages=[
            {"role": "system", "content": _RUNNER_SYSTEM},
            {"role": "user", "content": user_content},
        ],
    )
    raw = response.choices[0].message.content.strip()
    logger.debug("LLM_RESPONSE attempt=%d\n%s", attempt + 1, raw)
    return raw


# ---------------------------------------------------------------------------
# Validator
# NOTE: _RUNNER_SYSTEM schema must stay in sync with validate_llm_output().
# ---------------------------------------------------------------------------

_VALID_TYPES = {"int", "float", "string", "date", "bool"}
_VALID_STRATEGIES = {"median", "mean", "mode", "fill", "drop_row", "drop_column", "leave_null"}


def validate_llm_output(data: dict[str, Any], known_columns: set[str]) -> tuple[bool, str]:
    """
    Structural validation of the LLM partial diff.

    Expects {"columns": {<col>: {<only changed keys>}}} — only changed columns,
    only changed keys within each column.

    Collects ALL errors before returning so the retry prompt gets the full
    picture in one shot rather than one error at a time.

    Returns (True, "") on success or (False, "<all errors joined>") on failure.
    The error string is fed verbatim into the next retry prompt.
    """
    errors: list[str] = []

    if "columns" not in data:
        # Nothing else to validate without columns
        return False, "Missing required key: 'columns'"

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

        if "rename_to" in col_def:
            rt = col_def["rename_to"]
            if not isinstance(rt, str):
                errors.append(f"columns['{col}'].rename_to must be a string")
            elif not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", rt):
                errors.append(
                    f"columns['{col}'].rename_to='{rt}' is not a valid identifier "
                    f"(use snake_case, no spaces or hyphens)"
                )

    if errors:
        return False, "\n".join(f"- {e}" for e in errors)
    return True, ""


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

    The LLM returns a partial diff — only changed columns and only changed keys.
    We deep-merge that diff onto the baseline so _metadata, duplicates, and
    custom_transforms always come from the baseline.

    Soft failure contract — never raises. Returns base_recommendations if:
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

        valid, err = validate_llm_output(llm_diff, known_columns)
        if not valid:
            logger.warning(f"Runner attempt {attempt + 1} failed validation: {err}")
            previous_output = raw
            validation_error = err
            continue

        # Strip keys that are identical to the baseline (LLM sometimes echoes unchanged keys)
        for col, col_diff in llm_diff["columns"].items():
            if col in base_recommendations["columns"]:
                base_col = base_recommendations["columns"][col]
                echoed = [k for k, v in col_diff.items() if k != "note" and base_col.get(k) == v]
                for k in echoed:
                    del col_diff[k]

        # Strip missing_values when baseline has null (column has no nulls — baseline is authoritative).
        # The LLM sometimes adds an imputation strategy for zero-null columns.
        for col, col_diff in llm_diff["columns"].items():
            if "missing_values" in col_diff:
                base_col = base_recommendations["columns"].get(col, {})
                if base_col.get("missing_values") is None:
                    logger.debug("Stripped missing_values for '%s' — baseline has no nulls", col)
                    del col_diff["missing_values"]

        # Protect leave_null set by MAR detection — code precedence is higher than LLM.
        # The LLM cannot override leave_null to a fill strategy; it can still set leave_null
        # itself (making a non-MAR column intentionally null), but cannot undo a MAR decision.
        for col, col_diff in llm_diff["columns"].items():
            if "missing_values" in col_diff:
                base_col = base_recommendations["columns"].get(col, {})
                base_strategy = (base_col.get("missing_values") or {}).get("strategy")
                if base_strategy == "leave_null":
                    logger.debug("Protected leave_null for '%s' — MAR-detected, LLM cannot override", col)
                    del col_diff["missing_values"]

        # Strip no-op renames (rename_to identical to the column name).
        for col, col_diff in llm_diff["columns"].items():
            if col_diff.get("rename_to") == col:
                logger.debug("Stripped no-op rename_to for '%s'", col)
                del col_diff["rename_to"]

        # Drop columns where only a note remains (note without a real change is noise)
        # and columns where nothing changed at all.
        _SUBSTANTIVE_KEYS = {"type", "nullable", "missing_values", "rename_to"}
        llm_diff["columns"] = {
            col: col_diff
            for col, col_diff in llm_diff["columns"].items()
            if col_diff and (set(col_diff.keys()) & _SUBSTANTIVE_KEYS)
        }

        # Deep merge: baseline + LLM diff (only changed cols/keys)
        enriched = copy.deepcopy(base_recommendations)
        for col, col_diff in llm_diff["columns"].items():
            if col in enriched["columns"]:
                enriched["columns"][col].update(col_diff)
                # Clear stale warnings when LLM changed the strategy — the old warnings
                # were generated for the baseline strategy and are now contradictory.
                if "missing_values" in col_diff:
                    enriched["columns"][col]["warnings"] = []

        n_changed = len(llm_diff["columns"])
        n_notes = sum(1 for cd in llm_diff["columns"].values() if cd.get("note"))
        logger.info(
            f"LLM enrichment succeeded on attempt {attempt + 1}. "
            f"columns_changed={n_changed} column_notes={n_notes}"
        )
        return enriched

    logger.error("LLM enrichment: all 3 attempts exhausted — falling back to baseline recommendations")
    return base_recommendations

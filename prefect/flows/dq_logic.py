"""
Pure business logic for DQ analysis — no Prefect imports.

All functions here are plain Python and can be tested without a Prefect context:

    from flows.dq_logic import profile_dataframe, score_profile
    profile = profile_dataframe(df, malformed_rows=0)

Architecture
------------
Code  → scrapes objective facts  (metadata / column_profiles)
LLM   → interprets those facts   (fill strategy, explanations)   [Phase 3]
Human → approves the result      (via PUT /recommendations)

Everything in this file is deterministic and LLM-free.
"""
import io
import re
from datetime import datetime
from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_csv(stream) -> tuple[pd.DataFrame, int]:
    """
    Parse a CSV from any file-like stream.

    Malformed rows (mismatched field count) are counted and skipped rather than
    raising an exception. Accepts any stream pd.read_csv() understands
    (MinIO urllib3 response, BytesIO, file handle).

    Returns:
        df:             parsed DataFrame
        malformed_rows: count of skipped rows
    """
    malformed_rows = 0

    def _on_bad_line(bad_line: list) -> None:
        nonlocal malformed_rows
        malformed_rows += 1

    # Buffer into BytesIO — raw urllib3 responses (MinIO) are not reliably
    # iterable by the Python CSV engine. BytesIO is seekable and works with
    # both engines. The DataFrame must be in memory anyway, so no memory cost.
    buf = io.BytesIO(stream.read())
    df = pd.read_csv(buf, on_bad_lines=_on_bad_line, engine="python")
    return df, malformed_rows


def profile_dataframe(df: pd.DataFrame, malformed_rows: int = 0) -> dict[str, Any]:
    """
    Profile a DataFrame to produce objective facts — no strategy decisions.

    Dataset-level stats (completeness, uniqueness, validity, consistency) feed
    the DQ score. Per-column profiles feed the recommendations and, later, the
    LLM enrichment step.

    Returns a dict with:
        total_rows, total_columns, total_cells
        missing_cells, duplicate_rows, malformed_rows
        completeness, uniqueness, validity, consistency   ← DQ score inputs
        column_profiles                                   ← per-column facts
    """
    df = df.copy()  # ensure retries start with an unmodified DataFrame

    total_cells = df.size
    total_rows = len(df)
    missing_cells = int(df.isna().sum().sum())
    duplicate_rows = int(df.duplicated().sum())

    completeness = (1 - missing_cells / total_cells) * 100 if total_cells > 0 else 100.0
    uniqueness = (1 - duplicate_rows / total_rows) * 100 if total_rows > 0 else 100.0
    validity,  invalid_cells = _calculate_validity(df, total_cells, missing_cells)
    consistency = _calculate_consistency(df, total_cells)

    column_profiles = {}
    for col in df.columns:
        detected_type = _detect_column_type(df[col])
        if detected_type in ("numeric", "date"):
            col_profile = _profile_numeric_column(df[col])
        else:
            col_profile = _profile_string_column(df[col])

        # Count values that will become NaN when the schema cast is applied.
        # Only untyped columns can have this — numeric/bool/datetime are already validated by pandas.
        # These are separate from null_count: they are non-null but wrong-type values
        # (e.g. "N/A" in a salary column) that coercion will silently turn into NaN.
        invalid_count = 0
        if "object" in str(df[col].dtype):
            non_null = df[col].dropna()
            n = len(non_null)
            if n > 0:
                if detected_type == "numeric":
                    coerced = pd.to_numeric(non_null, errors="coerce")
                    invalid_count = int(coerced.isna().sum())
                elif detected_type == "date":
                    try:
                        coerced = pd.to_datetime(non_null, errors="coerce")
                        invalid_count = int(coerced.isna().sum())
                    except Exception:
                        pass
        col_profile["invalid_count"] = invalid_count
        col_profile["detected_type"] = detected_type
        col_profile["pandas_dtype"] = str(df[col].dtype)
        column_profiles[col] = col_profile

    return {
        "total_rows":     total_rows,
        "total_columns":  len(df.columns),
        "total_cells":    total_cells,
        "missing_cells":  missing_cells,
        "duplicate_rows": duplicate_rows,
        "malformed_rows": malformed_rows,
        "invalid_cells":  invalid_cells,
        "completeness":   completeness,
        "uniqueness":     uniqueness,
        "validity":       validity,
        "consistency":    consistency,
        "column_profiles": column_profiles,
    }


def score_profile(profile: dict[str, Any]) -> float:
    """
    Calculate DQ score from weighted profile components.

    Weights:
        Completeness  35%
        Uniqueness    25%
        Validity      25%
        Consistency   15%
    """
    dq_score = (
        profile["completeness"] * 0.35
        + profile["uniqueness"] * 0.25
        + profile["validity"] * 0.25
        + profile["consistency"] * 0.15
    )
    return round(dq_score, 2)


def build_recommendations(profile: dict[str, Any], dq_score: float) -> dict[str, Any]:
    """
    Build a recommendations dict from objective profile facts.

    Decisions made here are deterministic code rules — no LLM.
    The LLM enrichment step (Phase 3) will receive column_profiles and may
    override or enhance these defaults with semantic reasoning.
    """
    column_profiles = profile["column_profiles"]

    recommendations = {
        "schema":            {},
        "missing_values":    {},
        "duplicates":        {},
        "normalization":     {"columns": []},
        "custom_transforms": [],
        "_metadata": {
            "generated_at": datetime.utcnow().isoformat(),
            "dq_score":     dq_score,
            "issues_found": {
                "missing":         profile["missing_cells"],
                "duplicates":      profile["duplicate_rows"],
                "type_mismatches": profile["invalid_cells"],
            },
        },
    }

    for col, cp in column_profiles.items():
        detected_type = cp["detected_type"]

        # --- Schema ---
        needs_fill = cp["null_count"] > 0 or cp.get("invalid_count", 0) > 0
        recommendations["schema"][col] = {
            "type":     _schema_type(detected_type, cp),
            "nullable": needs_fill,
        }

        # --- Missing values ---
        # Triggered by actual nulls OR by invalid cells that will become NaN on cast.
        if needs_fill:
            recommendations["missing_values"][col] = {
                "strategy": _fill_strategy(detected_type, cp),
                "value":    None,
            }

    # --- Duplicates ---
    if profile["duplicate_rows"] > 0:
        recommendations["duplicates"] = {
            "strategy": "drop",
            "subset":   [],
            "keep":     "first",
        }

    # --- Normalization (advisory) ---
    # Suggest for numeric columns where values span more than two orders of magnitude
    for col, cp in column_profiles.items():
        if cp["detected_type"] == "numeric" and cp.get("stats"):
            stats = cp["stats"]
            col_min = abs(stats.get("min", 0))
            col_max = abs(stats.get("max", 0))
            if col_min > 0 and col_max / col_min > 100:
                recommendations["normalization"]["columns"].append(col)

    return recommendations


# ---------------------------------------------------------------------------
# Step 1 — Column type detection
# ---------------------------------------------------------------------------

def _detect_column_type(series: pd.Series) -> str:
    """
    Detect the semantic type of a column.

    Pandas already knows the type for numeric/bool/datetime columns it parsed
    correctly on read. For object (string) columns we probe with coercion:
    if ≥80% of non-null values parse as numeric → "numeric"
    if ≥80% of non-null values parse as datetime → "date"
    otherwise → "string"

    Returns one of: "numeric", "date", "bool", "string"
    """
    dtype_str = str(series.dtype).lower()

    if "int" in dtype_str or "float" in dtype_str:
        return "numeric"
    if "bool" in dtype_str:
        return "bool"
    if "datetime" in dtype_str:
        return "date"

    # Object column — probe the values
    col_data = series.dropna()
    n = len(col_data)
    if n == 0:
        return "string"

    _BOOL_LIKE = {True, False, "true", "false", "True", "False", "TRUE", "FALSE"}
    if all(v in _BOOL_LIKE for v in col_data):
        return "bool"

    numeric = pd.to_numeric(col_data, errors="coerce")
    if numeric.notna().sum() / n >= 0.8:
        return "numeric"

    try:
        dates = pd.to_datetime(col_data, errors="coerce")
        if dates.notna().sum() / n >= 0.8:
            return "date"
    except Exception:
        pass

    return "string"


# ---------------------------------------------------------------------------
# Step 2 — Numeric column profiler
# ---------------------------------------------------------------------------

def _profile_numeric_column(series: pd.Series) -> dict[str, Any]:
    """
    Profile a numeric or date column.

    Stats: null_count, null_pct, min, max, mean, median, std
    Outliers: IQR method — only run when ≥30 non-null values (statistically reliable).
    Outliers are advisory; they do not affect the DQ score.
    """
    n_total  = len(series)
    n_null   = int(series.isna().sum())
    non_null = series.dropna()

    # Coerce to numeric — handles object columns detected as numeric (e.g. "75000")
    numeric_vals = pd.to_numeric(non_null, errors="coerce").dropna()

    stats = {}
    if len(numeric_vals) > 0:
        stats = {
            "min":    round(float(numeric_vals.min()), 4),
            "max":    round(float(numeric_vals.max()), 4),
            "mean":   round(float(numeric_vals.mean()), 4),
            "median": round(float(numeric_vals.median()), 4),
            "std":    round(float(numeric_vals.std()), 4) if len(numeric_vals) > 1 else 0.0,
        }

    # IQR outlier detection — min 30 rows threshold
    outlier_count = 0
    if len(numeric_vals) >= 30:
        q1    = numeric_vals.quantile(0.25)
        q3    = numeric_vals.quantile(0.75)
        iqr   = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        outlier_count = int(((numeric_vals < lower) | (numeric_vals > upper)).sum())

    return {
        "null_count": n_null,
        "null_pct":   round(n_null / n_total * 100, 1) if n_total > 0 else 0.0,
        "stats":      stats,
        "outliers":   {"count": outlier_count, "method": "IQR"},
    }


# ---------------------------------------------------------------------------
# Step 3 — String column profiler
# ---------------------------------------------------------------------------

def _profile_string_column(series: pd.Series) -> dict[str, Any]:
    """
    Profile a string column.

    Cardinality: unique_count / non_null_count — low = likely categorical,
    high = likely free text or ID.

    Pattern detection: reuses _PATTERNS. If >50% of non-null values match
    a known pattern, that pattern name is recorded. This feeds both the
    consistency DQ metric and the schema type recommendation.

    top_values: up to 5 most frequent values — useful context for LLM enrichment.
    """
    n_total  = len(series)
    n_null   = int(series.isna().sum())
    non_null = series.dropna().astype(str)
    n_non_null = len(non_null)

    unique_count    = int(non_null.nunique())
    cardinality_pct = round(unique_count / n_non_null * 100, 1) if n_non_null > 0 else 0.0

    top_values = {
        str(k): int(v)
        for k, v in non_null.value_counts().head(5).items()
    }

    # Detect dominant pattern
    detected_pattern = None
    if n_non_null > 0:
        for pattern_name, pattern in _PATTERNS.items():
            match_count = int(non_null.apply(lambda x: bool(pattern.fullmatch(x))).sum())
            if match_count / n_non_null > 0.5:
                detected_pattern = pattern_name
                break

    return {
        "null_count":      n_null,
        "null_pct":        round(n_null / n_total * 100, 1) if n_total > 0 else 0.0,
        "unique_count":    unique_count,
        "cardinality_pct": cardinality_pct,
        "top_values":      top_values,
        "pattern":         detected_pattern,
    }


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


#: Patterns used for both consistency scoring and column profiling.
_PATTERNS = {
    "email":       re.compile(r"^[\w.+-]+@[\w-]+\.[\w.]+$"),
    "date_iso":    re.compile(r"^\d{4}-\d{2}-\d{2}$"),       # before phone — ISO dates match phone pattern otherwise
    "phone":       re.compile(r"^\+?[\d\s\-().]{7,15}$"),
    "url":         re.compile(r"^https?://\S+$"),
    "postcode_uk": re.compile(r"^[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}$", re.I),
    "uuid":        re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I),
}


def _schema_type(detected_type: str, col_profile: dict) -> str:
    """Map a detected_type + profile to a schema type string."""
    if detected_type == "bool":
        return "bool"
    if detected_type == "date":
        return "date"
    if detected_type == "numeric":
        if "float" in str(col_profile.get("pandas_dtype", "")).lower():
            return "float"
        # Distinguish int vs float from the stats
        stats = col_profile.get("stats", {})
        col_min = stats.get("min", 0)
        col_max = stats.get("max", 0)
        # If min and max are both whole numbers, treat as int
        if col_min == int(col_min) and col_max == int(col_max):
            return "int"
        return "float"
    pattern = col_profile.get("pattern")
    if pattern == "date_iso":
        return "date"
    return "string"


def _fill_strategy(detected_type: str, col_profile: dict) -> str:
    """
    Pick a default fill strategy for a column with missing values.

    Rules (deterministic, no LLM):
        numeric              → median  (robust to outliers)
        bool                 → mode    (most common true/false)
        date                 → drop_row (dates are hard to impute)
        string with pattern  → drop_row (can't invent a valid email/phone)
        string low-cardinality (<10% unique) → mode (likely a category)
        string high-cardinality             → drop_row (likely an ID/name)
    """
    if detected_type == "numeric":
        return "median"
    if detected_type == "bool":
        return "mode"
    if detected_type == "date":
        return "drop_row"
    # String
    if col_profile.get("pattern"):
        return "drop_row"
    if col_profile.get("cardinality_pct", 100) < 10:
        return "mode"
    return "drop_row"


def _calculate_validity(
    df: pd.DataFrame,
    total_cells: int,
    missing_cells: int,
) -> tuple[float, int]:
    """
    Validity: (type_conforming_cells / total_cells) * 100

    Pandas already validates numeric/bool/datetime columns on read.
    Only object columns need checking — they may contain mixed types
    (e.g. a salary column where some rows have "N/A" instead of a number).
    Null cells are excluded from the invalid count (that's completeness).

    Returns: (validity_pct, invalid_cell_count)
    """
    if total_cells == 0:
        return 100.0, 0

    invalid_cells = 0

    for col in df.columns:
        col_data = df[col].dropna()
        n = len(col_data)
        if n == 0 or "object" not in str(df[col].dtype):
            continue  # non-object dtypes already validated by pandas

        numeric       = pd.to_numeric(col_data, errors="coerce")
        numeric_count = int(numeric.notna().sum())
        if numeric_count / n > 0.5:
            invalid_cells += n - numeric_count
            continue

        try:
            dates      = pd.to_datetime(col_data, errors="coerce")
            date_count = int(dates.notna().sum())
            if date_count / n > 0.5:
                invalid_cells += n - date_count
        except Exception:
            pass

    type_conforming_cells = total_cells - missing_cells - invalid_cells
    validity_pct          = round(type_conforming_cells / total_cells * 100, 2)
    return validity_pct, invalid_cells


def _calculate_consistency(df: pd.DataFrame, total_cells: int) -> float:
    """
    Consistency: (pattern_matching_cells / total_cells) * 100

    For each string column, if >50% of non-null values match a known
    pattern (email, phone, URL, etc.), non-matching values are inconsistent.
    Numeric/datetime columns are skipped — patterns don't apply to them.
    """
    if total_cells == 0:
        return 100.0

    inconsistent_cells = 0

    for col in df.columns:
        col_data = df[col].dropna().astype(str)
        n = len(col_data)
        if n == 0 or "object" not in str(df[col].dtype):
            continue

        for pattern in _PATTERNS.values():
            match_count = int(col_data.apply(lambda x: bool(pattern.fullmatch(x))).sum())
            if match_count / n > 0.5:
                inconsistent_cells += n - match_count
                break

    return round((1 - inconsistent_cells / total_cells) * 100, 2)

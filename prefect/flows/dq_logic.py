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
import logging
import re
from datetime import datetime
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


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
    validity, invalid_cells = _calculate_validity(df, total_cells, missing_cells)
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
                        coerced = pd.to_datetime(non_null, errors="coerce", format="mixed")
                        invalid_count = int(coerced.isna().sum())
                    except Exception:
                        pass
        col_profile["invalid_count"] = invalid_count
        col_profile["detected_type"] = detected_type
        col_profile["pandas_dtype"] = str(df[col].dtype)
        column_profiles[col] = col_profile

    return {
        "total_rows": total_rows,
        "total_columns": len(df.columns),
        "total_cells": total_cells,
        "missing_cells": missing_cells,
        "duplicate_rows": duplicate_rows,
        "malformed_rows": malformed_rows,
        "invalid_cells": invalid_cells,
        "completeness": completeness,
        "uniqueness": uniqueness,
        "validity": validity,
        "consistency": consistency,
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


def score_profile_detailed(profile: dict[str, Any]) -> dict[str, float]:
    """
    Return all five DQ score components plus the weighted composite.

    Shape:
        {
            "overall":      97.70,
            "completeness": 95.2,
            "uniqueness":   100.0,
            "validity":     98.1,
            "consistency":  99.0,
        }
    """
    return {
        "overall": score_profile(profile),
        "completeness": round(profile["completeness"], 2),
        "uniqueness": round(profile["uniqueness"], 2),
        "validity": round(profile["validity"], 2),
        "consistency": round(profile["consistency"], 2),
    }


def _would_cast_safely(series: pd.Series, target_type: str, threshold: float = 0.05) -> bool:
    """
    Return True if casting series to target_type would produce fewer than
    threshold fraction of new NaN values among currently non-null cells.

    Only relevant for int/float casts — other types are always considered safe.
    Threshold default 0.05 = skip the cast if >5% of non-null values would be lost.
    """
    if target_type not in ("float", "int"):
        return True
    non_null = series.dropna()
    if len(non_null) == 0:
        return True
    converted = pd.to_numeric(non_null, errors="coerce")
    new_null_rate = converted.isna().sum() / len(non_null)
    return bool(new_null_rate <= threshold)


def apply_recommendations(df: pd.DataFrame, recommendations: dict[str, Any]) -> pd.DataFrame:
    """
    Apply approved recommendations to a DataFrame.

    Order of operations:
        1. Schema cast        — coerces columns to declared types; invalid values become NaN
        2. Fill/drop          — fills NaN (including newly coerced ones) per-column strategy
        3. Int recast         — after fill, cast int columns that are now NaN-free to int64
        3b. Outlier treatment — winsorise/remove/cap outliers per recommendations["outliers"]
        4. Deduplicate        — drop duplicate rows on the cleaned data
        5. Normalize          — min-max scale requested numeric columns
        6. Rename columns     — apply rename_to mappings from column definitions

    Custom transforms are not applied here (LLM phase).
    """
    df = df.copy()

    columns = recommendations.get("columns", {})
    duplicates = recommendations.get("duplicates", {})

    # ------------------------------------------------------------------
    # 0. Replace numeric sentinel values with NaN
    # ------------------------------------------------------------------
    # Sentinel values (e.g. -999.0, 9999) are validity violations — they are
    # not real measurements, they are coded "no data" markers. Replace them
    # with proper NaN before any schema cast or imputation so downstream steps
    # operate on clean data. The fill strategy then handles them identically
    # to real missing values.
    for col, col_def in columns.items():
        sentinels = col_def.get("sentinel_values")
        if sentinels and col in df.columns:
            df[col] = df[col].replace(sentinels, float("nan"))

    # ------------------------------------------------------------------
    # 1. Schema cast
    # ------------------------------------------------------------------
    for col, col_def in columns.items():
        if col not in df.columns:
            continue
        col_type = col_def.get("type", "string")
        if col_type in ("int", "float"):
            if not _would_cast_safely(df[col], col_type):
                logger.warning(
                    "Skipping type cast for '%s': casting to %s would null >5%% of "
                    "non-null values. Normalize the column format first via a custom_transform.",
                    col, col_type,
                )
                continue
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif col_type == "date":
            df[col] = pd.to_datetime(df[col], errors="coerce", format="mixed")
            # Standardise to ISO 8601 date string on output so downstream tools
            # see a consistent format regardless of the original locale.
            # NOTE: dt.strftime() converts NaT to the literal string "NaT" — use
            # where() to preserve nulls as NaN instead of producing "NaT" strings.
            df[col] = df[col].where(df[col].isna(), df[col].dt.strftime("%Y-%m-%d"))
        elif col_type == "bool":
            # Normalise to lowercase before lookup so "TRUE", "Yes", "Y", "ON"
            # all map correctly. Values not in either set become NaN (same as
            # the old fixed-map behaviour, but now covers the full _BOOL_LIKE set).
            _true_strs = {"true", "1", "yes", "y", "t", "on"}
            _false_strs = {"false", "0", "no", "n", "f", "off"}
            _bool_literal = {True: True, False: False, 1: True, 0: False}
            df[col] = df[col].map(
                lambda x: True  if str(x).strip().lower() in _true_strs
                     else False if str(x).strip().lower() in _false_strs
                     else _bool_literal.get(x)
            )
        # "string": leave as-is

    # ------------------------------------------------------------------
    # 2. Fill / drop missing values
    # ------------------------------------------------------------------
    drop_cols = []
    for col, col_def in columns.items():
        if col not in df.columns:
            continue
        fill_def = col_def.get("missing_values")
        if not fill_def:
            continue
        strategy = fill_def.get("strategy", "drop_row")
        fill_value = fill_def.get("value")

        if strategy == "median":
            df[col] = df[col].fillna(df[col].median())
        elif strategy == "mean":
            df[col] = df[col].fillna(df[col].mean())
        elif strategy == "mode":
            mode = df[col].mode()
            if len(mode) > 0:
                df[col] = df[col].fillna(mode[0])
        elif strategy == "fill" and fill_value is not None:
            df[col] = df[col].fillna(fill_value)
        elif strategy == "drop_column":
            df = df.drop(columns=[col], errors="ignore")
        elif strategy == "leave_null":
            pass  # intentional — keep nulls as-is
        elif strategy == "drop_row":
            drop_cols.append(col)

    if drop_cols:
        df = df.dropna(subset=drop_cols)

    # ------------------------------------------------------------------
    # 3. Int recast — only when no NaN remains in the column
    # ------------------------------------------------------------------
    for col, col_def in columns.items():
        if col not in df.columns:
            continue
        if col_def.get("type") == "int" and df[col].notna().all():
            try:
                df[col] = df[col].astype(int)
            except (ValueError, TypeError):
                pass  # leave as float if cast fails

    # ------------------------------------------------------------------
    # 3b. Outlier treatment
    # ------------------------------------------------------------------
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
        if strategy in ("winsorise", "cap"):
            df[col] = df[col].clip(lower=lower, upper=upper)
        elif strategy == "remove":
            df = df[(df[col].isna()) | ((df[col] >= lower) & (df[col] <= upper))]

    # ------------------------------------------------------------------
    # 4. Deduplicate
    # ------------------------------------------------------------------
    if duplicates and duplicates.get("strategy") == "drop":
        subset = duplicates.get("subset") or None
        keep = duplicates.get("keep", "first")
        if subset == []:
            subset = None
        df = df.drop_duplicates(subset=subset, keep=keep)

    # ------------------------------------------------------------------
    # 5. Normalize (min-max or z-score)
    # ------------------------------------------------------------------
    for col, col_def in columns.items():
        if col not in df.columns:
            continue
        normalize = col_def.get("normalize", False)
        if normalize == "min_max":
            col_min = df[col].min()
            col_max = df[col].max()
            if col_max != col_min:
                df[col] = (df[col] - col_min) / (col_max - col_min)
        elif normalize == "z_score":
            mean = df[col].mean()
            std = df[col].std(ddof=0)
            if std > 0:
                df[col] = (df[col] - mean) / std

    # ------------------------------------------------------------------
    # 6. Rename columns
    # ------------------------------------------------------------------
    rename_map = {
        col: col_def["rename_to"]
        for col, col_def in columns.items()
        if col in df.columns and col_def.get("rename_to")
    }
    if rename_map:
        df = df.rename(columns=rename_map)

    return df


def build_recommendations(df: pd.DataFrame, profile: dict[str, Any], dq_score: float) -> dict[str, Any]:
    """
    Build a recommendations dict from objective profile facts.

    Decisions made here are deterministic code rules — no LLM.
    The LLM enrichment step (Phase 3) will receive column_profiles and may
    override or enhance these defaults with semantic reasoning.

    Decision precedence (highest to lowest):
        1. MAR detection (code) → leave_null
        2. 50% null threshold   → drop_column
        3. Type-based rules     → median/mean/mode/drop_row
        4. LLM MNAR override    (applied in llm_enrichment.py)
        5. User edit            (applied via PUT /recommendations)
    """
    column_profiles = profile["column_profiles"]

    # MAR detection — runs once, O(cols²) with scipy stats tests.
    # Any column whose nulls are statistically associated with another column
    # is flagged as MAR and gets leave_null (filling would introduce bias).
    mar_cols = {c["column"] for c in _detect_mar_columns(df)}

    total_sentinels = sum(cp.get("sentinel_count", 0) for cp in column_profiles.values())
    total_format_issues = sum(1 for cp in column_profiles.values() if cp.get("format_inconsistency"))

    recommendations = {
        "columns": {},
        "duplicates": {},
        "custom_transforms": [],
        "outliers": {},
        "_metadata": {
            "generated_at": datetime.utcnow().isoformat(),
            "dq_score": dq_score,
            "issues_found": {
                "missing": profile["missing_cells"],
                "duplicates": profile["duplicate_rows"],
                "type_mismatches": profile["invalid_cells"],
                "sentinel_values": total_sentinels,
                "format_inconsistencies": total_format_issues,
            },
        },
    }

    for col, cp in column_profiles.items():
        detected_type = cp["detected_type"]
        sentinel_vals = cp.get("sentinel_values", [])
        needs_fill = cp["null_count"] > 0 or cp.get("invalid_count", 0) > 0 or bool(sentinel_vals)

        # --- Missing values ---
        # Triggered by actual nulls OR by invalid cells that will become NaN on cast.
        # MAR override takes priority over _fill_strategy() rules.
        if needs_fill:
            if col in mar_cols:
                strategy = "leave_null"
            else:
                strategy = _fill_strategy(detected_type, cp)
            missing_values_def = {"strategy": strategy, "value": None}
        else:
            strategy = None
            missing_values_def = None

        # --- Normalize (advisory) ---
        # Suggest for numeric columns whose values are large-scale (|max| > 100)
        # and non-constant. Uses range to handle zero-min and negative columns.
        # Use z_score when outliers are present (they would squash min-max into a tiny range);
        # use min_max otherwise (produces [0, 1] output, easier to reason about).
        normalize: str | bool = False
        if detected_type == "numeric" and cp.get("stats"):
            stats = cp["stats"]
            col_range = stats.get("max", 0) - stats.get("min", 0)
            col_max_abs = max(abs(stats.get("max", 0)), abs(stats.get("min", 0)))
            if col_range > 0 and col_max_abs > 100:
                has_outliers = cp.get("outliers", {}).get("count", 0) > 0
                normalize = "z_score" if has_outliers else "min_max"

        # --- Warnings ---
        # All applicable warnings are collected — a column can trigger more than one.
        # e.g. drop_row strategy AND sentinel values present are both user-relevant.
        warnings = []
        if needs_fill and strategy in ("drop_row", "drop_column"):
            null_count = cp.get("null_count", 0) + cp.get("invalid_count", 0)
            null_pct = cp.get("null_pct", 0)
            if strategy == "drop_row":
                pct = round(null_count / profile["total_rows"] * 100, 1)
                warnings.append(f"drop_row will remove up to {null_count} rows ({pct}% of dataset) where '{col}' is null or invalid.")
            elif strategy == "drop_column":
                warnings.append(f"drop_column will remove the entire '{col}' column ({null_pct}% of values are missing).")
        if strategy == "leave_null" and cp.get("null_pct", 0) > 30:
            warnings.append(f"'{col}' has {cp['null_pct']}% nulls — kept as-is (MAR detected or intentional).")
        if cp.get("sentinel_count", 0) > 0:
            sentinel_c = cp["sentinel_count"]
            warnings.append(f"'{col}' contains {sentinel_c} sentinel-like value(s) — will be replaced with null before imputation.")
        if detected_type == "string" and cp.get("format_inconsistency"):
            pct = round(100 - cp.get("castable_pct", 0), 1)
            sample = cp.get("non_castable_sample", [])
            sample_str = ", ".join(f"'{v}'" for v in sample)
            warnings.append(
                f"'{col}' has mixed value formats — {pct}% of values are not numeric-castable"
                + (f" (e.g. {sample_str})" if sample_str else "")
                + ". Type cast would corrupt data; a custom transform is needed to normalize format first."
            )

        recommendations["columns"][col] = {
            "type": _schema_type(detected_type, cp),
            "nullable": needs_fill,
            "missing_values": missing_values_def,
            "normalize": normalize,
            "warnings": warnings,
            "note": None,
            "sentinel_values": sentinel_vals or None,
        }

    # --- Outliers ---
    # Only numeric columns with detected outliers get an entry.
    # Default strategy is "keep" — user can change to winsorise/remove/cap.
    for col, cp in column_profiles.items():
        if cp.get("detected_type") != "numeric":
            continue
        outlier_info = cp.get("outliers", {})
        if outlier_info.get("count", 0) > 0:
            recommendations["outliers"][col] = {
                "strategy": "keep",
                "method": "iqr",
                "lower": outlier_info.get("lower"),
                "upper": outlier_info.get("upper"),
            }

    # --- Duplicates ---
    if profile["duplicate_rows"] > 0:
        recommendations["duplicates"] = {
            "strategy": "drop",
            "subset": [],
            "keep": "first",
        }

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

    _BOOL_LIKE = {
        True, False,
        "true", "false", "True", "False", "TRUE", "FALSE",
        "yes", "no", "Yes", "No", "YES", "NO",
        "y", "n", "Y", "N",
        "1", "0",
        "on", "off", "On", "Off", "ON", "OFF",
    }
    if all(v in _BOOL_LIKE for v in col_data):
        return "bool"

    numeric = pd.to_numeric(col_data, errors="coerce")
    if numeric.notna().sum() / n >= 0.8:
        return "numeric"

    try:
        dates = pd.to_datetime(col_data, errors="coerce", format="mixed")
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
    n_total = len(series)
    n_null = int(series.isna().sum())
    non_null = series.dropna()

    # Coerce to numeric — handles object columns detected as numeric (e.g. "75000")
    numeric_vals = pd.to_numeric(non_null, errors="coerce").dropna()

    stats = {}
    if len(numeric_vals) > 0:
        stats = {
            "min": round(float(numeric_vals.min()), 4),
            "max": round(float(numeric_vals.max()), 4),
            "mean": round(float(numeric_vals.mean()), 4),
            "median": round(float(numeric_vals.median()), 4),
            "std": round(float(numeric_vals.std()), 4) if len(numeric_vals) > 1 else 0.0,
        }
    elif len(non_null) > 0:
        # Date column — numeric coercion failed, compute min/max as ISO strings
        date_vals = pd.to_datetime(non_null, errors="coerce", format="mixed").dropna()
        if len(date_vals) > 0:
            stats = {
                "min": date_vals.min().strftime("%Y-%m-%d"),
                "max": date_vals.max().strftime("%Y-%m-%d"),
            }

    # IQR outlier detection — min 30 rows threshold
    outlier_count = 0
    outlier_lower = None
    outlier_upper = None
    if len(numeric_vals) >= 30:
        q1 = numeric_vals.quantile(0.25)
        q3 = numeric_vals.quantile(0.75)
        iqr = q3 - q1
        outlier_lower = round(float(q1 - 1.5 * iqr), 4)
        outlier_upper = round(float(q3 + 1.5 * iqr), 4)
        outlier_count = int(((numeric_vals < outlier_lower) | (numeric_vals > outlier_upper)).sum())

    sentinel_count, sentinel_values = _detect_numeric_sentinels(series)

    return {
        "null_count": n_null,
        "null_pct": round(n_null / n_total * 100, 1) if n_total > 0 else 0.0,
        "stats": stats,
        "outliers": {"count": outlier_count, "method": "IQR", "lower": outlier_lower, "upper": outlier_upper},
        "sentinel_count": sentinel_count,
        "sentinel_values": sentinel_values,
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
    n_total = len(series)
    n_null = int(series.isna().sum())
    non_null = series.dropna().astype(str)
    n_non_null = len(non_null)

    unique_count = int(non_null.nunique())
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

    sentinel_count = _count_sentinels(series)

    # Partial castability — detect mixed-format columns.
    # Uses the same pd.to_numeric the apply step would use: ground truth, no regex.
    # Threshold: ≥5 castable values (absolute, consistent with sentinel detection)
    # AND castable_pct < 80% (above 80% the column is already typed as "numeric").
    # Primary use: feed custom transform generation so the LLM knows what format
    # normalization is needed. Secondary: warn user before a destructive type cast.
    format_inconsistency = False
    castable_pct = 0.0
    non_castable_sample: list[str] = []
    if n_non_null > 0:
        castable_mask = pd.to_numeric(non_null, errors="coerce").notna()
        castable_count = int(castable_mask.sum())
        castable_pct = round(float(castable_count / n_non_null * 100), 1)
        if castable_count >= 5 and castable_pct < 80.0:
            format_inconsistency = True
            non_castable_sample = non_null[~castable_mask].head(3).tolist()

    return {
        "null_count": n_null,
        "null_pct": round(n_null / n_total * 100, 1) if n_total > 0 else 0.0,
        "unique_count": unique_count,
        "cardinality_pct": cardinality_pct,
        "top_values": top_values,
        "pattern": detected_pattern,
        "sentinel_count": sentinel_count,
        "format_inconsistency": format_inconsistency,
        "castable_pct": castable_pct,
        "non_castable_sample": non_castable_sample,
    }


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

#: Sentinel strings that indicate a missing or unknown value stored as text.
#: Normalised to lowercase — comparison always lower-strips the column values.
_SENTINEL_STRINGS: frozenset = frozenset({
    "n/a", "na", "null", "none", "unknown", "missing", "undefined", "-", "?", "",
})


def _count_sentinels(series: pd.Series) -> int:
    """
    Count non-null string values that look like placeholder sentinels.

    Sentinel strings are human-typed substitutes for null (e.g. "N/A",
    "unknown", "-", "?"). They are not NaN so pandas counts them as valid
    values, but they should be treated as missing before imputation.

    Returns 0 for an empty or all-null series.
    """
    non_null = series.dropna()
    if len(non_null) == 0:
        return 0
    normalised = non_null.astype(str).str.strip().str.lower()
    return int(normalised.isin(_SENTINEL_STRINGS).sum())


def _detect_numeric_sentinels(series: pd.Series) -> tuple[int, list[float]]:
    """
    Detect numeric sentinel values — implausibly extreme values that appear
    with suspiciously high frequency (e.g. -999, 9999 as 'not recorded' codes).

    Algorithm:
    - Skips series with fewer than 10 non-null values (not enough data)
    - Identifies candidates outside Q1 − 3×IQR or Q3 + 3×IQR
      (3×IQR is a stricter fence than the standard 1.5×IQR outlier threshold)
    - Flags candidates appearing ≥5 times (absolute count).

    Percentage-based thresholds (e.g. ≥5%) silently fail on large datasets:
    34 occurrences of -999 in 1200 rows = 2.8% — below the threshold despite
    being a clear systematic coded null. The 3×IQR fence is already very strict;
    any extreme value appearing 5+ times is almost certainly a sentinel code.

    A non-zero IQR is required — constant columns cannot have sentinel codes.

    Returns:
        (total_instance_count, [unique_sentinel_values])
        e.g. (60, [-999.0]) when -999.0 appears 60 times in the series.
    """
    numeric = pd.to_numeric(series, errors="coerce").dropna()
    n = len(numeric)
    if n < 10:
        return 0, []
    q1 = float(numeric.quantile(0.25))
    q3 = float(numeric.quantile(0.75))
    iqr = q3 - q1
    if iqr == 0:
        return 0, []
    lower = q1 - 3 * iqr
    upper = q3 + 3 * iqr
    candidates = numeric[(numeric < lower) | (numeric > upper)]
    if len(candidates) == 0:
        return 0, []
    vc = candidates.value_counts()
    suspicious = vc[vc >= 5]
    return int(suspicious.sum()), sorted(float(v) for v in suspicious.index)


#: Patterns used for both consistency scoring and column profiling.
_PATTERNS = {
    "email": re.compile(r"^[\w.+-]+@[\w-]+\.[\w.]+$"),
    "date_iso": re.compile(r"^\d{4}-\d{2}-\d{2}$"),       # before phone — ISO dates match phone pattern otherwise
    "phone": re.compile(r"^\+?[\d\s\-().]{7,15}$"),
    "url": re.compile(r"^https?://\S+$"),
    "postcode_uk": re.compile(r"^[A-Z]{1,2}\d[A-Z\d]?\s?\d[A-Z]{2}$", re.I),
    "uuid": re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I),
}


def _detect_mar_columns(df: pd.DataFrame, alpha: float = 0.05) -> list[dict]:
    """
    Formal MAR detection using statistical association tests (van Buuren, 2018).

    For each column C with at least 5 nulls:
    - Creates a binary is_missing indicator (1 = null, 0 = present)
    - Tests its statistical association with every other column O:
        - Numeric O   → point-biserial correlation (scipy.stats.pointbiserialr)
          Tests whether the mean of O differs significantly between rows where
          C is missing vs rows where C is present.
        - Categorical O → chi-square test of independence (scipy.stats.chi2_contingency)
          Tests whether the distribution of O values differs between missing
          and non-missing rows of C.
    - If p < alpha (default 0.05): C is MAR with respect to O.

    Soft failure: returns [] if scipy is not installed, so the baseline
    recommendations fall back gracefully with no crash.

    Returns a list of dicts: {column, correlated_with, test, p_value}
    """
    try:
        from scipy import stats
    except ImportError:
        return []

    results = []
    n_rows = len(df)
    if n_rows < 10:
        return results

    for col in df.columns:
        null_mask = df[col].isna()
        n_nulls = int(null_mask.sum())
        if n_nulls < 5:
            continue

        is_missing = null_mask.astype(int)

        for other in df.columns:
            if other == col:
                continue

            other_col = df[other].dropna()
            if len(other_col) < 10:
                continue

            # Align to rows where `other` is not null
            aligned_missing = is_missing.loc[other_col.index]

            try:
                if pd.api.types.is_numeric_dtype(df[other]):
                    # Point-biserial: tests mean difference between missing/non-missing groups
                    _, p = stats.pointbiserialr(aligned_missing, other_col)
                else:
                    # Chi-square: tests distribution shift between missing/non-missing groups
                    ct = pd.crosstab(aligned_missing, df[other].loc[other_col.index])
                    if ct.shape[0] < 2 or ct.shape[1] < 2:
                        continue
                    _, p, _, _ = stats.chi2_contingency(ct)

                if p < alpha:
                    results.append({
                        "column": col,
                        "correlated_with": other,
                        "test": "point_biserial" if pd.api.types.is_numeric_dtype(df[other]) else "chi2",
                        "p_value": round(float(p), 4),
                    })
                    break  # first significant association per column is enough

            except Exception:
                continue

    return results


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
        ≥50% null                → drop_column (filling half-empty data fabricates too much)
        numeric — skewed         → median  (outliers present OR |mean−median|/std > 0.15)
        numeric — symmetric      → mean    (efficient estimator when distribution is symmetric)
        bool                     → mode    (most common true/false)
        date                     → drop_row (dates are hard to impute)
        string with pattern      → drop_row (can't invent a valid email/phone)
        string low-cardinality (<10% unique) → mode (likely a category)
        string high-cardinality             → drop_row (likely an ID/name)
    """
    if col_profile.get("null_pct", 0) >= 50:
        return "drop_column"
    if detected_type == "numeric":
        s = col_profile.get("stats", {})
        mean = s.get("mean", 0)
        median = s.get("median", 0)
        std = s.get("std", 1) or 1
        has_outliers = col_profile.get("outliers", {}).get("count", 0) > 0
        is_skewed = abs(mean - median) / std > 0.15
        return "median" if (has_outliers or is_skewed) else "mean"
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

        numeric = pd.to_numeric(col_data, errors="coerce")
        numeric_count = int(numeric.notna().sum())
        if numeric_count / n > 0.5:
            invalid_cells += n - numeric_count
            continue

        try:
            dates = pd.to_datetime(col_data, errors="coerce", format="mixed")
            date_count = int(dates.notna().sum())
            if date_count / n > 0.5:
                invalid_cells += n - date_count
        except Exception:
            pass

    type_conforming_cells = total_cells - missing_cells - invalid_cells
    validity_pct = round(type_conforming_cells / total_cells * 100, 2)
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

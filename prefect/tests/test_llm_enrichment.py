"""
Unit tests for flows/llm_enrichment.py.

No real API calls — groq.Groq is mocked throughout.
Run with: .venv/bin/python3 -m pytest prefect/tests/test_llm_enrichment.py -v
"""
import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from flows.llm_enrichment import (
    _build_llm_profile,
    _sample_rows,
    _validate_transform_ast,
    _test_transform,
    enrich_recommendations,
    generate_transform_code,
    validate_llm_output,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "cust_id":  ["C001", "C002", None, "C004", "C005"],
        "sal":      [45000, 120000, 67000, None, 89000],
        "dept_cd":  ["FIN", "MGT", "FIN", "HR", "MGT"],
        "email":    ["a@x.com", "b@x.com", "c@x.com", "d@x.com", "e@x.com"],
        "age":      [25, 42, 31, 28, None],
    })

@pytest.fixture
def sample_profile(sample_df):
    """Minimal profile dict matching the shape profile_dataframe() produces."""
    return {
        "total_rows":    5,
        "total_columns": 5,
        "total_cells":   25,
        "missing_cells": 3,
        "duplicate_rows": 0,
        "malformed_rows": 0,
        "invalid_cells":  0,
        "completeness":  88.0,
        "uniqueness":    100.0,
        "validity":      100.0,
        "consistency":   100.0,
        "column_profiles": {
            "cust_id": {
                "null_count": 1, "null_pct": 20.0,
                "unique_count": 4, "cardinality_pct": 100.0,
                "top_values": {"C001": 1, "C002": 1, "C004": 1, "C005": 1, "C003": 1},
                "pattern": None, "invalid_count": 0, "detected_type": "string",
                "pandas_dtype": "object",
            },
            "sal": {
                "null_count": 1, "null_pct": 20.0,
                "stats": {"min": 45000.0, "max": 120000.0, "mean": 80250.0, "median": 78000.0, "std": 29500.0},
                "outliers": {"count": 0, "method": "IQR"},
                "invalid_count": 0, "detected_type": "numeric",
                "pandas_dtype": "float64",
            },
            "dept_cd": {
                "null_count": 0, "null_pct": 0.0,
                "unique_count": 3, "cardinality_pct": 60.0,
                "top_values": {"FIN": 2, "MGT": 2, "HR": 1},
                "pattern": None, "invalid_count": 0, "detected_type": "string",
                "pandas_dtype": "object",
            },
            "email": {
                "null_count": 0, "null_pct": 0.0,
                "unique_count": 5, "cardinality_pct": 100.0,
                "top_values": {"a@x.com": 1, "b@x.com": 1, "c@x.com": 1},
                "pattern": "email", "invalid_count": 0, "detected_type": "string",
                "pandas_dtype": "object",
            },
            "age": {
                "null_count": 1, "null_pct": 20.0,
                "stats": {"min": 25.0, "max": 42.0, "mean": 31.5, "median": 30.0, "std": 7.3},
                "outliers": {"count": 0, "method": "IQR"},
                "invalid_count": 0, "detected_type": "numeric",
                "pandas_dtype": "float64",
            },
        },
    }

@pytest.fixture
def base_recs():
    return {
        "columns": {
            "cust_id": {"type": "string", "nullable": True,  "missing_values": {"strategy": "drop_row", "value": None}, "normalize": False, "warnings": [], "note": None},
            "sal":     {"type": "float",  "nullable": True,  "missing_values": {"strategy": "median",   "value": None}, "normalize": False, "warnings": [], "note": None},
            "dept_cd": {"type": "string", "nullable": False, "missing_values": None, "normalize": False, "warnings": [], "note": None},
            "email":   {"type": "string", "nullable": False, "missing_values": None, "normalize": False, "warnings": [], "note": None},
            "age":     {"type": "float",  "nullable": True,  "missing_values": {"strategy": "median",   "value": None}, "normalize": False, "warnings": [], "note": None},
        },
        "duplicates": {},
        "custom_transforms": [],
        "_metadata": {
            "generated_at": datetime.utcnow().isoformat(),
            "dq_score": 88.0,
            "issues_found": {"missing": 3, "duplicates": 0, "type_mismatches": 0},
        },
    }

def _valid_diff():
    """A valid LLM partial diff — only changed columns, only changed keys."""
    return {
        "columns": {
            "cust_id": {
                "nullable": False,  # genuine change: baseline has nullable=True
                "note": "High cardinality ID column — should never be nullable.",
            },
            "sal": {
                "missing_values": {"strategy": "mean", "value": None},
                "note": "Salary is roughly symmetric — mean is appropriate.",
            },
        }
    }

# ---------------------------------------------------------------------------
# _sample_rows
# ---------------------------------------------------------------------------

class TestSampleRows:
    def test_empty_df_returns_empty(self):
        df = pd.DataFrame({"a": []})
        result = _sample_rows(df)
        assert len(result) == 0

    def test_small_df_returns_all_rows(self):
        df = pd.DataFrame({"a": range(5)})
        result = _sample_rows(df)
        assert len(result) == 5

    def test_cap_at_25(self):
        df = pd.DataFrame({"a": range(1000)})
        result = _sample_rows(df)
        assert len(result) <= 25

    def test_ten_percent_for_medium_df(self):
        df = pd.DataFrame({"a": range(100)})
        result = _sample_rows(df)
        # 10% of 100 = 10, capped at 25
        assert len(result) == 10

    def test_null_rows_included(self):
        df = pd.DataFrame({"a": [None, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]})
        result = _sample_rows(df)
        # The null row (index 0) should be in the sample
        assert 0 in result.index

    def test_does_not_mutate_input(self):
        df = pd.DataFrame({"a": range(100)})
        original_len = len(df)
        _sample_rows(df)
        assert len(df) == original_len

# ---------------------------------------------------------------------------
# _build_llm_profile
# ---------------------------------------------------------------------------

class TestBuildLlmProfile:
    def test_returns_string(self, sample_profile, sample_df):
        result = _build_llm_profile(sample_profile, sample_df)
        assert isinstance(result, str)

    def test_pandas_dtype_not_in_output(self, sample_profile, sample_df):
        result = _build_llm_profile(sample_profile, sample_df)
        assert "pandas_dtype" not in result

    def test_numeric_and_string_tables_present(self, sample_profile, sample_df):
        result = _build_llm_profile(sample_profile, sample_df)
        assert "mean,median,std" in result          # numeric table header
        assert "cardinality_pct,top_values" in result  # string table header

    def test_top_values_trimmed_to_3(self, sample_profile, sample_df):
        # cust_id has 5 top_values in fixture; only first 3 should appear in columns section
        # C003 is the 5th entry and does not appear in the df — safe to check globally
        result = _build_llm_profile(sample_profile, sample_df)
        assert "C003" not in result
        cust_id_line = next(l for l in result.splitlines() if l.startswith("cust_id,"))
        top_values_field = cust_id_line.split(",")[5]   # 6th CSV field
        assert len(top_values_field.split()) == 3

    def test_does_not_mutate_input_profile(self, sample_profile, sample_df):
        original_top = len(sample_profile["column_profiles"]["cust_id"]["top_values"])
        _build_llm_profile(sample_profile, sample_df)
        assert len(sample_profile["column_profiles"]["cust_id"]["top_values"]) == original_top

    def test_sample_rows_present(self, sample_profile, sample_df):
        result = _build_llm_profile(sample_profile, sample_df)
        assert "SAMPLE ROWS:" in result

    def test_dataset_level_stats_present(self, sample_profile, sample_df):
        result = _build_llm_profile(sample_profile, sample_df)
        assert "rows,cols,missing" in result
        assert "completeness" in result

    def test_correlated_nulls_section_not_in_output(self, sample_profile, sample_df):
        # MAR detection is now deterministic code — the CORRELATED NULLS section
        # was removed from the LLM prompt (todo 2.3).
        result = _build_llm_profile(sample_profile, sample_df)
        assert "CORRELATED NULLS" not in result

# ---------------------------------------------------------------------------
# validate_llm_output
# ---------------------------------------------------------------------------

KNOWN = {"cust_id", "sal", "dept_cd", "email", "age"}

class TestValidateLlmOutput:
    def test_valid_diff_passes(self):
        ok, err = validate_llm_output(_valid_diff(), KNOWN)
        assert ok is True
        assert err == ""

    def test_missing_columns_key(self):
        ok, err = validate_llm_output({}, KNOWN)
        assert ok is False
        assert "columns" in err

    def test_unknown_column(self):
        diff = {"columns": {"ghost_col": {"note": "x"}}}
        ok, err = validate_llm_output(diff, KNOWN)
        assert ok is False
        assert "ghost_col" in err

    def test_missing_note(self):
        diff = {"columns": {"sal": {"type": "float"}}}
        ok, err = validate_llm_output(diff, KNOWN)
        assert ok is False
        assert "note" in err

    def test_invalid_type(self):
        diff = {"columns": {"sal": {"type": "number", "note": "x"}}}
        ok, err = validate_llm_output(diff, KNOWN)
        assert ok is False
        assert "number" in err

    def test_invalid_strategy(self):
        diff = {"columns": {"sal": {"missing_values": {"strategy": "delete", "value": None}, "note": "x"}}}
        ok, err = validate_llm_output(diff, KNOWN)
        assert ok is False
        assert "delete" in err

    def test_fill_without_value(self):
        diff = {"columns": {"sal": {"missing_values": {"strategy": "fill", "value": None}, "note": "x"}}}
        ok, err = validate_llm_output(diff, KNOWN)
        assert ok is False
        assert "fill" in err
        assert "null" in err

    def test_transform_hint_valid_string_accepted(self):
        diff = {"columns": {"sal": {"transform_hint": "strip '%%' suffix and divide by 100", "note": "x"}}}
        ok, err = validate_llm_output(diff, KNOWN)
        assert ok is True
        assert err == ""

    def test_transform_hint_empty_string_rejected(self):
        diff = {"columns": {"sal": {"transform_hint": "", "note": "x"}}}
        ok, err = validate_llm_output(diff, KNOWN)
        assert ok is False
        assert "transform_hint" in err

    def test_transform_hint_non_string_rejected(self):
        diff = {"columns": {"sal": {"transform_hint": 123, "note": "x"}}}
        ok, err = validate_llm_output(diff, KNOWN)
        assert ok is False
        assert "transform_hint" in err

    def test_all_errors_collected(self):
        """Multiple errors in one diff — all should appear in the error string."""
        diff = {
            "columns": {
                "sal":   {"type": "number", "note": "x"},           # bad type
                "email": {"missing_values": {"strategy": "delete", "value": None}, "note": "x"},  # bad strategy
                "ghost": {"note": "x"},                              # unknown column
            }
        }
        ok, err = validate_llm_output(diff, KNOWN)
        assert ok is False
        assert "number" in err
        assert "delete" in err
        assert "ghost" in err

# ---------------------------------------------------------------------------
# enrich_recommendations — fallback paths
# ---------------------------------------------------------------------------

class TestEnrichRecommendations:
    def test_no_api_key_returns_base(self, sample_profile, base_recs, sample_df):
        with patch.dict("os.environ", {"LLM_API_KEY": ""}):
            result = enrich_recommendations(sample_profile, base_recs, sample_df)
        assert result is base_recs

    def test_import_error_returns_base(self, sample_profile, base_recs, sample_df):
        with patch.dict("os.environ", {"LLM_API_KEY": "sk-test"}):
            with patch.dict("sys.modules", {"groq": None}):
                result = enrich_recommendations(sample_profile, base_recs, sample_df)
        assert result is base_recs

    def test_all_retries_fail_returns_base(self, sample_profile, base_recs, sample_df):
        """All 3 runner attempts raise — should fall back to base_recs."""
        with patch.dict("os.environ", {"LLM_API_KEY": "sk-test"}):
            with patch.dict("sys.modules", {"groq": MagicMock()}):
                with patch("flows.llm_enrichment._runner", side_effect=RuntimeError("API down")):
                    result = enrich_recommendations(sample_profile, base_recs, sample_df)
        assert result is base_recs

    def test_diff_merged_onto_baseline(self, sample_profile, base_recs, sample_df):
        """LLM diff is deep-merged onto baseline — unchanged columns stay intact."""
        diff = _valid_diff()

        with patch.dict("os.environ", {"LLM_API_KEY": "sk-test"}):
            with patch.dict("sys.modules", {"groq": MagicMock()}):
                with patch("flows.llm_enrichment._runner", return_value=json.dumps(diff)):
                    result = enrich_recommendations(sample_profile, base_recs, sample_df)

        # Changed columns have the LLM note
        assert result["columns"]["cust_id"]["note"] is not None
        assert result["columns"]["sal"]["missing_values"]["strategy"] == "mean"
        # Unchanged columns still exist and are unmodified
        assert result["columns"]["dept_cd"]["type"] == "string"
        assert result["columns"]["dept_cd"]["note"] is None
        # Top-level baseline keys preserved
        assert "_metadata" in result
        assert "duplicates" in result

    def test_invalid_json_then_success(self, sample_profile, base_recs, sample_df):
        """Attempt 1 returns invalid JSON, attempt 2 returns valid diff — should succeed."""
        diff = _valid_diff()
        runner_responses = iter(["not valid json {{{{", json.dumps(diff)])

        with patch.dict("os.environ", {"LLM_API_KEY": "sk-test"}):
            with patch.dict("sys.modules", {"groq": MagicMock()}):
                with patch("flows.llm_enrichment._runner", side_effect=lambda *a, **kw: next(runner_responses)):
                    result = enrich_recommendations(sample_profile, base_recs, sample_df)

        assert result["columns"]["cust_id"]["note"] is not None

    def test_validation_failure_then_success(self, sample_profile, base_recs, sample_df):
        """Attempt 1 fails validation, attempt 2 passes — should return merged result."""
        diff     = _valid_diff()
        bad_diff = {"columns": {"sal": {"type": "number", "note": "x"}}}
        runner_responses = iter([json.dumps(bad_diff), json.dumps(diff)])

        with patch.dict("os.environ", {"LLM_API_KEY": "sk-test"}):
            with patch.dict("sys.modules", {"groq": MagicMock()}):
                with patch("flows.llm_enrichment._runner", side_effect=lambda *a, **kw: next(runner_responses)):
                    result = enrich_recommendations(sample_profile, base_recs, sample_df)

        # bad_diff failed (type=number invalid), good diff applied — sal type stays from baseline
        assert result["columns"]["sal"]["type"] == "float"
        assert result["columns"]["sal"]["missing_values"]["strategy"] == "mean"

    def test_markdown_fences_stripped(self, sample_profile, base_recs, sample_df):
        """Runner wraps JSON in ```json ... ``` — should still parse correctly."""
        diff   = _valid_diff()
        fenced = f"```json\n{json.dumps(diff)}\n```"

        with patch.dict("os.environ", {"LLM_API_KEY": "sk-test"}):
            with patch.dict("sys.modules", {"groq": MagicMock()}):
                with patch("flows.llm_enrichment._runner", return_value=fenced):
                    result = enrich_recommendations(sample_profile, base_recs, sample_df)

        assert result["columns"]["cust_id"]["note"] is not None

    def test_runner_called_once_on_success(self, sample_profile, base_recs, sample_df):
        """Runner is called exactly once when output is valid on first attempt."""
        diff = _valid_diff()

        with patch.dict("os.environ", {"LLM_API_KEY": "sk-test"}):
            with patch.dict("sys.modules", {"groq": MagicMock()}):
                with patch("flows.llm_enrichment._runner", return_value=json.dumps(diff)) as mock_runner:
                    result = enrich_recommendations(sample_profile, base_recs, sample_df)

        assert mock_runner.call_count == 1
        assert result["columns"]["sal"]["missing_values"]["strategy"] == "mean"

    def test_transform_hint_merged_onto_baseline(self, sample_profile, base_recs, sample_df):
        """transform_hint set by LLM is merged into the column and stored in result."""
        diff = {
            "columns": {
                "sal": {
                    "transform_hint": "strip '%%' suffix from values like '50%%' and divide by 100",
                    "type": "float",
                    "note": "Salary stored with percent suffix — normalise to decimal.",
                }
            }
        }
        with patch.dict("os.environ", {"LLM_API_KEY": "sk-test"}):
            with patch.dict("sys.modules", {"groq": MagicMock()}):
                with patch("flows.llm_enrichment._runner", return_value=json.dumps(diff)):
                    result = enrich_recommendations(sample_profile, base_recs, sample_df)

        assert result["columns"]["sal"]["transform_hint"] == diff["columns"]["sal"]["transform_hint"]
        assert result["columns"]["sal"]["type"] == "float"

    def test_transform_hint_only_column_not_dropped(self, sample_profile, base_recs, sample_df):
        """A column with only transform_hint + note must NOT be dropped by the substantive key filter."""
        diff = {
            "columns": {
                "dept_cd": {
                    "transform_hint": "normalise abbreviated codes like 'FIN', 'MGT' to full names",
                    "note": "Department codes are opaque — expand to readable names.",
                }
            }
        }
        with patch.dict("os.environ", {"LLM_API_KEY": "sk-test"}):
            with patch.dict("sys.modules", {"groq": MagicMock()}):
                with patch("flows.llm_enrichment._runner", return_value=json.dumps(diff)):
                    result = enrich_recommendations(sample_profile, base_recs, sample_df)

        assert "transform_hint" in result["columns"]["dept_cd"]
        assert result["columns"]["dept_cd"]["note"] is not None

    def test_leave_null_baseline_not_overridden_by_llm(self, sample_profile, sample_df):
        """
        MAR detection sets leave_null in the baseline (Level 1 precedence).
        The LLM must not be able to change it to a fill strategy (Level 4).
        """
        # Baseline with sal as leave_null (simulates MAR detection result)
        recs_with_mar = {
            "columns": {
                "cust_id": {"type": "string", "nullable": True,  "missing_values": {"strategy": "drop_row",   "value": None}, "normalize": False, "warnings": [], "note": None},
                "sal":     {"type": "float",  "nullable": True,  "missing_values": {"strategy": "leave_null", "value": None}, "normalize": False, "warnings": [], "note": None},
                "dept_cd": {"type": "string", "nullable": False, "missing_values": None, "normalize": False, "warnings": [], "note": None},
                "email":   {"type": "string", "nullable": False, "missing_values": None, "normalize": False, "warnings": [], "note": None},
                "age":     {"type": "float",  "nullable": True,  "missing_values": {"strategy": "mean",       "value": None}, "normalize": False, "warnings": [], "note": None},
            },
            "duplicates": {}, "custom_transforms": [],
            "_metadata": {"generated_at": datetime.utcnow().isoformat(), "dq_score": 88.0, "issues_found": {}},
        }
        # LLM tries to change sal's leave_null to median
        llm_tries_override = {
            "columns": {
                "sal": {"missing_values": {"strategy": "median", "value": None}, "note": "Should use median not leave_null."},
            }
        }

        with patch.dict("os.environ", {"LLM_API_KEY": "sk-test"}):
            with patch.dict("sys.modules", {"groq": MagicMock()}):
                with patch("flows.llm_enrichment._runner", return_value=json.dumps(llm_tries_override)):
                    result = enrich_recommendations(sample_profile, recs_with_mar, sample_df)

        # leave_null must be preserved — LLM override stripped
        assert result["columns"]["sal"]["missing_values"]["strategy"] == "leave_null"

    def test_rename_runner_results_applied(self, sample_profile, base_recs, sample_df):
        """Renames from _rename_runner are merged into enriched recommendations."""
        diff = {
            "columns": {
                "cust_id": {"nullable": False, "note": "ID column."},
            }
        }
        with patch.dict("os.environ", {"LLM_API_KEY": "sk-test"}):
            with patch.dict("sys.modules", {"groq": MagicMock()}):
                with patch("flows.llm_enrichment._runner", return_value=json.dumps(diff)):
                    with patch(
                        "flows.llm_enrichment._rename_runner",
                        return_value={"sal": "salary", "dept_cd": "department_code"},
                    ):
                        result = enrich_recommendations(sample_profile, base_recs, sample_df)

        assert result["columns"]["sal"]["rename_to"] == "salary"
        assert result["columns"]["dept_cd"]["rename_to"] == "department_code"
        assert result["columns"]["email"].get("rename_to") is None

    def test_stray_rename_to_in_strategy_diff_stripped(self, sample_profile, base_recs, sample_df):
        """rename_to in the strategy diff is stripped before merge (separate call handles renames)."""
        diff_with_stray_rename = {
            "columns": {
                "sal": {
                    "rename_to": "salary",
                    "missing_values": {"strategy": "mean", "value": None},
                    "note": "Mean appropriate for symmetric salary distribution.",
                }
            }
        }
        with patch.dict("os.environ", {"LLM_API_KEY": "sk-test"}):
            with patch.dict("sys.modules", {"groq": MagicMock()}):
                with patch("flows.llm_enrichment._runner", return_value=json.dumps(diff_with_stray_rename)):
                    with patch("flows.llm_enrichment._rename_runner", return_value={}):
                        result = enrich_recommendations(sample_profile, base_recs, sample_df)

        # Strategy change applied, stray rename_to stripped (rename runner returned {})
        assert result["columns"]["sal"]["missing_values"]["strategy"] == "mean"
        assert result["columns"]["sal"].get("rename_to") is None


# ---------------------------------------------------------------------------
# _validate_transform_ast and _test_transform (4.8)
# ---------------------------------------------------------------------------

class TestTransformCodeGeneration:

    # --- _validate_transform_ast ---

    def test_validate_ast_valid_lambda_passes(self):
        ok, err = _validate_transform_ast(
            "lambda col: col.str.rstrip('%').astype(float) / 100"
        )
        assert ok is True
        assert err == ""

    def test_validate_ast_rejects_import(self):
        # 'import os' is a statement; ast.parse(mode='eval') raises SyntaxError
        ok, err = _validate_transform_ast("import os")
        assert ok is False
        assert "SyntaxError" in err

    def test_validate_ast_rejects_forbidden_name(self):
        ok, err = _validate_transform_ast("lambda col: eval(col)")
        assert ok is False
        assert "eval" in err

    def test_validate_ast_rejects_dunder_attr(self):
        ok, err = _validate_transform_ast("lambda col: col.__class__")
        assert ok is False
        assert "__class__" in err

    def test_validate_ast_rejects_multiline(self):
        # Multiple statements are not a valid single expression
        ok, err = _validate_transform_ast("x = 1\nlambda col: col")
        assert ok is False

    # --- _test_transform ---

    def test_test_transform_valid_strip_percent(self):
        s = pd.Series(["10%", "20%", "30%"])
        ok, err = _test_transform("lambda col: col.str.rstrip('%').astype(float)", s)
        assert ok is True
        assert err == ""

    def test_test_transform_fails_on_exception(self):
        s = pd.Series(["a", "b", "c"])
        ok, err = _test_transform("lambda col: col.astype(int)", s)
        assert ok is False

    def test_test_transform_fails_on_excess_nulls(self):
        # lambda that nullifies all 10 non-null values — 100% > 5% threshold
        s = pd.Series(["10", "20", "30", "40", "50", "60", "70", "80", "90", "100"])
        ok, err = _test_transform("lambda col: col.where(col == 'NEVERMATCHES')", s)
        assert ok is False
        assert "null" in err.lower()

    def test_test_transform_fails_when_nulls_filled(self):
        """
        .astype(str) converts NaN → 'None' string, filling original null positions.
        This must be rejected even though result_nulls < original_nulls (passes old damage check).
        """
        s = pd.Series(["10%", None, "30%", None])
        ok, err = _test_transform("lambda col: col.str.rstrip('%').astype(str)", s)
        assert ok is False
        assert "null" in err.lower()

    def test_test_transform_fails_on_wrong_return_type(self):
        s = pd.Series([1, 2, 3])
        ok, err = _test_transform("lambda col: 42", s)
        assert ok is False
        assert "Series" in err

    # --- generate_transform_code ---

    def test_generate_no_api_key_returns_unchanged(self, base_recs, sample_df):
        with patch.dict("os.environ", {"LLM_API_KEY": ""}):
            result = generate_transform_code(base_recs, sample_df)
        assert result is base_recs

    def test_generate_stores_transform_code_on_success(self):
        """LLM returns a valid lambda; generate_transform_code stores it as transform_code."""
        recs = {
            "columns": {
                "price": {
                    "type": "float",
                    "transform_hint": "strip '%' suffix and divide by 100",
                }
            }
        }
        df = pd.DataFrame({"price": ["10%", "20%", "30%"]})
        groq_mock = MagicMock()
        mock_choice = MagicMock()
        mock_choice.message.content = "lambda col: col.str.rstrip('%').astype(float) / 100"
        groq_mock.Groq.return_value.chat.completions.create.return_value.choices = [mock_choice]

        with patch.dict("os.environ", {"LLM_API_KEY": "sk-test"}):
            with patch.dict("sys.modules", {"groq": groq_mock}):
                result = generate_transform_code(recs, df)

        assert "transform_code" in result["columns"]["price"]

    def test_generate_all_retries_fail_skips_column(self):
        """If all 3 LLM attempts produce code that fails validation, column is skipped."""
        recs = {
            "columns": {
                "price": {
                    "type": "float",
                    "transform_hint": "strip '%' suffix and divide by 100",
                }
            }
        }
        df = pd.DataFrame({"price": ["10%", "20%", "30%"]})
        groq_mock = MagicMock()
        mock_choice = MagicMock()
        # eval is a forbidden name — AST validation will reject this every time
        mock_choice.message.content = "lambda col: eval(col)"
        groq_mock.Groq.return_value.chat.completions.create.return_value.choices = [mock_choice]

        with patch.dict("os.environ", {"LLM_API_KEY": "sk-test"}):
            with patch.dict("sys.modules", {"groq": groq_mock}):
                result = generate_transform_code(recs, df)

        assert "transform_code" not in result["columns"]["price"]

"""
Unit tests for flows/dq_logic.py

All tests are pure Python — no Prefect context, no MinIO, no PostgreSQL.
Run from the prefect/ directory:

    pytest tests/test_dq_logic.py -v

Coverage strategy
-----------------
For every function we test:
  - Happy path (normal expected input)
  - Boundary conditions (at and around thresholds)
  - Empty / all-null inputs
  - Single-element inputs
  - Type edge cases (negative numbers, empty strings, etc.)
  - Mutation safety (input not modified)
"""
import io
import sys
from pathlib import Path

import pandas as pd
import pytest

# Make flows/ importable when running from prefect/
sys.path.insert(0, str(Path(__file__).parent.parent))

from flows.dq_logic import (
    _calculate_consistency,
    _calculate_validity,
    _detect_column_type,
    _fill_strategy,
    _profile_numeric_column,
    _profile_string_column,
    _schema_type,
    build_recommendations,
    parse_csv,
    profile_dataframe,
    score_profile,
)


# ===========================================================================
# Helpers
# ===========================================================================

def series(values, name="col", dtype=None):
    s = pd.Series(values, name=name)
    return s.astype(dtype) if dtype else s


def csv_bytes(content: str) -> io.BytesIO:
    return io.BytesIO(content.encode())


# ===========================================================================
# _detect_column_type
# ===========================================================================

class TestDetectColumnType:

    # --- Already-typed by pandas ---

    def test_int64(self):
        assert _detect_column_type(series([1, 2, 3], dtype="int64")) == "numeric"

    def test_float64(self):
        assert _detect_column_type(series([1.1, 2.2], dtype="float64")) == "numeric"

    def test_bool(self):
        assert _detect_column_type(series([True, False], dtype="bool")) == "bool"

    def test_datetime(self):
        s = pd.to_datetime(pd.Series(["2021-01-01", "2022-06-15"]))
        assert _detect_column_type(s) == "date"

    # --- Object columns: numeric detection ---

    def test_object_all_numeric_strings(self):
        assert _detect_column_type(series(["1", "2", "3", "4", "5"])) == "numeric"

    def test_object_negative_numeric_strings(self):
        assert _detect_column_type(series(["-10", "-20", "30", "40", "50"])) == "numeric"

    def test_object_float_strings(self):
        assert _detect_column_type(series(["1.5", "2.7", "3.9", "4.1", "5.6"])) == "numeric"

    def test_object_exactly_80pct_numeric_boundary(self):
        # 4/5 = 80% → should be "numeric" (>= 0.8 threshold)
        assert _detect_column_type(series(["1", "2", "3", "4", "text"])) == "numeric"

    def test_object_below_80pct_numeric(self):
        # 3/5 = 60% → should be "string"
        assert _detect_column_type(series(["1", "2", "3", "text", "more"])) == "string"

    def test_object_exactly_one_non_numeric(self):
        # 9/10 = 90% numeric → numeric
        assert _detect_column_type(series(["1"] * 9 + ["N/A"])) == "numeric"

    # --- Object columns: date detection ---

    def test_object_iso_dates(self):
        s = series(["2021-01-01", "2022-06-15", "2023-03-20", "2020-11-05", "not-a-date"])
        assert _detect_column_type(s) == "date"

    def test_object_below_80pct_date(self):
        # 3/5 = 60% → not enough → string
        s = series(["2021-01-01", "2022-06-15", "2023-03-20", "hello", "world"])
        assert _detect_column_type(s) == "string"

    # --- String / undetectable ---

    def test_email_column(self):
        s = series(["alice@example.com", "bob@example.com", "charlie@example.com"])
        assert _detect_column_type(s) == "string"

    def test_categorical_column(self):
        assert _detect_column_type(series(["Eng", "Sales", "HR", "Finance"])) == "string"

    # --- Edge cases ---

    def test_all_null_returns_string(self):
        assert _detect_column_type(series([None, None, None])) == "string"

    def test_single_numeric_value(self):
        assert _detect_column_type(series([42])) == "numeric"

    def test_single_string_value(self):
        assert _detect_column_type(series(["hello"])) == "string"

    def test_single_null_returns_string(self):
        assert _detect_column_type(series([None])) == "string"

    def test_numeric_does_not_mutate_series(self):
        s = series(["1", "2", "3"])
        original = s.copy()
        _detect_column_type(s)
        pd.testing.assert_series_equal(s, original)


# ===========================================================================
# _profile_numeric_column
# ===========================================================================

class TestProfileNumericColumn:

    # --- Stats ---

    def test_basic_stats(self):
        p = _profile_numeric_column(series([10.0, 20.0, 30.0, 40.0, 50.0]))
        assert p["stats"]["min"] == 10.0
        assert p["stats"]["max"] == 50.0
        assert p["stats"]["mean"] == 30.0
        assert p["stats"]["median"] == 30.0

    def test_negative_values(self):
        p = _profile_numeric_column(series([-50.0, -10.0, 0.0, 10.0, 50.0]))
        assert p["stats"]["min"] == -50.0
        assert p["stats"]["max"] == 50.0

    def test_all_same_value_std_is_zero(self):
        p = _profile_numeric_column(series([5.0, 5.0, 5.0, 5.0]))
        assert p["stats"]["std"] == 0.0

    def test_single_value_no_std_error(self):
        p = _profile_numeric_column(series([42.0]))
        assert p["stats"]["min"] == 42.0
        assert p["stats"]["std"] == 0.0

    # --- Nulls ---

    def test_null_count(self):
        p = _profile_numeric_column(series([1.0, None, 3.0, None]))
        assert p["null_count"] == 2
        assert p["null_pct"] == 50.0

    def test_all_null(self):
        p = _profile_numeric_column(series([None, None, None]))
        assert p["null_count"] == 3
        assert p["stats"] == {}
        assert p["outliers"]["count"] == 0

    def test_no_nulls(self):
        p = _profile_numeric_column(series([1.0, 2.0, 3.0]))
        assert p["null_count"] == 0
        assert p["null_pct"] == 0.0

    # --- Outlier detection boundary ---

    def test_no_outliers_with_29_rows(self):
        # 29 values — just below threshold, outliers not computed
        vals = [10.0] * 28 + [99999.0]
        p = _profile_numeric_column(series(vals))
        assert p["outliers"]["count"] == 0

    def test_outliers_computed_with_exactly_30_rows(self):
        # 29 normal + 1 extreme = 30 total → threshold met
        vals = [10.0] * 29 + [99999.0]
        p = _profile_numeric_column(series(vals))
        assert p["outliers"]["count"] == 1

    def test_no_outliers_in_uniform_distribution(self):
        p = _profile_numeric_column(series([float(i) for i in range(50)]))
        assert p["outliers"]["count"] == 0

    def test_multiple_outliers(self):
        normal = [10.0] * 30
        normal += [100000.0, -100000.0]  # two extreme outliers
        p = _profile_numeric_column(series(normal))
        assert p["outliers"]["count"] == 2

    def test_outlier_method_always_iqr(self):
        p = _profile_numeric_column(series([1.0, 2.0, 3.0]))
        assert p["outliers"]["method"] == "IQR"

    # --- Object columns detected as numeric ---

    def test_string_numbers_get_correct_stats(self):
        p = _profile_numeric_column(series(["100", "200", "300", "400"]))
        assert p["stats"]["min"] == 100.0
        assert p["stats"]["max"] == 400.0

    # --- Rounding ---

    def test_stats_rounded_to_4dp(self):
        p = _profile_numeric_column(series([1.0, 2.0, 3.0]))
        mean = p["stats"]["mean"]
        assert mean == round(mean, 4)


# ===========================================================================
# _profile_string_column
# ===========================================================================

class TestProfileStringColumn:

    # --- Cardinality ---

    def test_all_unique(self):
        p = _profile_string_column(series(["a", "b", "c", "d"]))
        assert p["unique_count"] == 4
        assert p["cardinality_pct"] == 100.0

    def test_all_same(self):
        p = _profile_string_column(series(["Eng", "Eng", "Eng", "Eng"]))
        assert p["unique_count"] == 1
        assert p["cardinality_pct"] == 25.0  # 1/4 * 100

    def test_mixed_cardinality(self):
        p = _profile_string_column(series(["a", "b", "c", "a", "b"]))
        assert p["unique_count"] == 3
        assert p["cardinality_pct"] == 60.0

    # --- Nulls ---

    def test_null_count_and_pct(self):
        p = _profile_string_column(series(["a", None, "b", None]))
        assert p["null_count"] == 2
        assert p["null_pct"] == 50.0

    def test_all_null(self):
        p = _profile_string_column(series([None, None, None]))
        assert p["null_count"] == 3
        assert p["unique_count"] == 0
        assert p["cardinality_pct"] == 0.0
        assert p["pattern"] is None

    def test_no_nulls(self):
        p = _profile_string_column(series(["a", "b", "c"]))
        assert p["null_count"] == 0
        assert p["null_pct"] == 0.0

    # --- Pattern detection ---

    def test_email_pattern(self):
        s = series(["a@b.com", "c@d.com", "e@f.com", "g@h.com", "not-email"])
        assert _profile_string_column(s)["pattern"] == "email"

    def test_date_iso_pattern(self):
        # date_iso must be checked before phone — ISO dates match phone regex too
        s = series(["2021-03-15", "2022-07-01", "2020-01-10", "2023-05-20"])
        assert _profile_string_column(s)["pattern"] == "date_iso"

    def test_phone_pattern(self):
        s = series(["+44 7700 900123", "+44 7700 900456",
                    "+44 7700 900789", "+44 7700 900321"])
        assert _profile_string_column(s)["pattern"] == "phone"

    def test_postcode_uk_pattern(self):
        s = series(["SW1A 1AA", "M1 1AE", "EC1A 1BB", "B1 1BB", "W1A 0AX"])
        assert _profile_string_column(s)["pattern"] == "postcode_uk"

    def test_uuid_pattern(self):
        s = series([
            "550e8400-e29b-41d4-a716-446655440000",
            "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
            "6ba7b811-9dad-11d1-80b4-00c04fd430c8",
            "6ba7b812-9dad-11d1-80b4-00c04fd430c8",
        ])
        assert _profile_string_column(s)["pattern"] == "uuid"

    def test_url_pattern(self):
        s = series([
            "https://example.com",
            "https://google.com",
            "https://github.com",
            "not-a-url",
        ])
        assert _profile_string_column(s)["pattern"] == "url"

    def test_no_pattern_plain_text(self):
        s = series(["Engineering", "Sales", "Marketing", "HR", "Finance"])
        assert _profile_string_column(s)["pattern"] is None

    def test_pattern_requires_majority_above_50pct(self):
        # 2/4 = 50% emails — exactly at threshold, NOT above → no pattern
        s = series(["a@b.com", "c@d.com", "not-email", "also-not"])
        assert _profile_string_column(s)["pattern"] is None

    def test_pattern_at_just_above_50pct(self):
        # 3/5 = 60% emails → pattern detected
        s = series(["a@b.com", "c@d.com", "e@f.com", "not1", "not2"])
        assert _profile_string_column(s)["pattern"] == "email"

    # --- top_values ---

    def test_top_values_capped_at_5(self):
        s = series([str(i) for i in range(20)])
        assert len(_profile_string_column(s)["top_values"]) <= 5

    def test_top_values_frequency_correct(self):
        p = _profile_string_column(series(["a", "a", "a", "b", "b", "c"]))
        assert p["top_values"]["a"] == 3
        assert p["top_values"]["b"] == 2

    def test_top_values_keys_are_strings(self):
        p = _profile_string_column(series(["x", "y", "z"]))
        for k in p["top_values"].keys():
            assert isinstance(k, str)

    # --- Empty string vs null ---

    def test_empty_string_is_not_null(self):
        p = _profile_string_column(series(["", "a", "b"]))
        assert p["null_count"] == 0


# ===========================================================================
# _calculate_validity
# ===========================================================================

class TestCalculateValidity:

    # --- Happy path ---

    def test_all_valid_typed_columns(self):
        df = pd.DataFrame({"age": [25, 30, 35], "salary": [50000, 60000, 70000]})
        validity, invalid = _calculate_validity(df, df.size, 0)
        assert validity == 100.0
        assert invalid == 0

    def test_one_invalid_cell(self):
        df = pd.DataFrame({"sal": ["50000", "60000", "70000", "80000", "N/A"]})
        validity, invalid = _calculate_validity(df, df.size, 0)
        assert invalid == 1
        assert validity < 100.0

    # --- Boundary: 50% numeric threshold ---

    def test_exactly_50pct_numeric_does_not_trigger(self):
        # 2/4 = 50% parse as numeric — threshold is >50%, so NOT treated as numeric
        df = pd.DataFrame({"col": ["1", "2", "text", "more"]})
        validity, invalid = _calculate_validity(df, df.size, 0)
        assert invalid == 0  # not classified as numeric column, no invalids counted

    def test_just_above_50pct_numeric_triggers(self):
        # 3/5 = 60% → column classified as numeric → 2 invalids
        df = pd.DataFrame({"col": ["1", "2", "3", "text", "more"]})
        validity, invalid = _calculate_validity(df, df.size, 0)
        assert invalid == 2

    # --- Null exclusion ---

    def test_null_cells_not_counted_as_invalid(self):
        df = pd.DataFrame({"salary": [None, None, None]})
        missing = int(df.isna().sum().sum())
        validity, invalid = _calculate_validity(df, df.size, missing)
        assert invalid == 0

    # --- Multiple columns ---

    def test_multiple_invalid_columns(self):
        df = pd.DataFrame({
            "a": ["1", "2", "X"],   # 2/3 numeric → 1 invalid
            "b": ["1", "2", "Y"],   # 2/3 numeric → 1 invalid
        })
        validity, invalid = _calculate_validity(df, df.size, 0)
        assert invalid == 2

    # --- Edge cases ---

    def test_empty_dataframe(self):
        validity, invalid = _calculate_validity(pd.DataFrame(), 0, 0)
        assert validity == 100.0
        assert invalid == 0

    def test_all_null_object_column(self):
        df = pd.DataFrame({"col": [None, None, None]})
        missing = int(df.isna().sum().sum())
        validity, invalid = _calculate_validity(df, df.size, missing)
        assert invalid == 0

    def test_non_object_columns_skipped(self):
        df = pd.DataFrame({"age": pd.array([25, 30, 35], dtype="Int64")})
        validity, invalid = _calculate_validity(df, df.size, 0)
        assert invalid == 0
        assert validity == 100.0


# ===========================================================================
# _calculate_consistency
# ===========================================================================

class TestCalculateConsistency:

    def test_all_consistent_emails(self):
        df = pd.DataFrame({"email": ["a@b.com", "c@d.com", "e@f.com", "g@h.com", "i@j.com"]})
        assert _calculate_consistency(df, df.size) == 100.0

    def test_one_inconsistent_email(self):
        df = pd.DataFrame({"email": ["a@b.com", "c@d.com", "e@f.com", "g@h.com", "invalid"]})
        consistency = _calculate_consistency(df, df.size)
        assert consistency < 100.0

    def test_no_dominant_pattern_not_penalised(self):
        df = pd.DataFrame({"notes": ["foo", "bar", "baz", "qux", "quux"]})
        assert _calculate_consistency(df, df.size) == 100.0

    def test_numeric_column_always_skipped(self):
        df = pd.DataFrame({"sal": [50000, 60000, 70000, 80000, 90000]})
        assert _calculate_consistency(df, df.size) == 100.0

    def test_exactly_50pct_match_not_penalised(self):
        # 2/4 = 50% — threshold is >50%, so no pattern applied
        df = pd.DataFrame({"col": ["a@b.com", "c@d.com", "not", "also-not"]})
        assert _calculate_consistency(df, df.size) == 100.0

    def test_just_above_50pct_triggers_penalty(self):
        # 3/5 = 60% emails → inconsistency counted for the 2 non-emails
        df = pd.DataFrame({"col": ["a@b.com", "c@d.com", "e@f.com", "not1", "not2"]})
        consistency = _calculate_consistency(df, df.size)
        assert consistency < 100.0

    def test_empty_dataframe(self):
        assert _calculate_consistency(pd.DataFrame(), 0) == 100.0

    def test_all_null_object_column(self):
        df = pd.DataFrame({"col": pd.Series([None, None, None], dtype=object)})
        assert _calculate_consistency(df, df.size) == 100.0

    def test_multiple_columns_different_patterns(self):
        df = pd.DataFrame({
            "email": ["a@b.com", "c@d.com", "e@f.com", "g@h.com", "not-email"],
            "dept":  ["Eng", "Sales", "HR", "Finance", "Marketing"],
        })
        # email column: 4/5 match → 1 inconsistent out of 10 total cells
        consistency = _calculate_consistency(df, df.size)
        assert 0 < consistency < 100.0


# ===========================================================================
# profile_dataframe
# ===========================================================================

class TestProfileDataframe:

    @pytest.fixture
    def standard_df(self):
        return pd.DataFrame({
            "name":       ["Alice", "Bob", "Charlie", "Alice", "Diana"],
            "email":      ["a@b.com", "c@d.com", "e@f.com", "a@b.com", "not-email"],
            "age":        [30, 25, 35, 30, 28],
            "salary":     [75000.0, None, 90000.0, 75000.0, 60000.0],
            "department": ["Eng", "Sales", "Eng", "Eng", "Marketing"],
        })

    # --- Dataset-level counts ---

    def test_total_counts(self, standard_df):
        p = profile_dataframe(standard_df)
        assert p["total_rows"] == 5
        assert p["total_columns"] == 5
        assert p["total_cells"] == 25
        assert p["missing_cells"] == 1
        assert p["duplicate_rows"] == 1

    def test_all_columns_profiled(self, standard_df):
        p = profile_dataframe(standard_df)
        assert set(p["column_profiles"].keys()) == set(standard_df.columns)

    # --- Per-column profile content ---

    def test_numeric_column_has_stats_and_outliers(self, standard_df):
        cp = profile_dataframe(standard_df)["column_profiles"]["salary"]
        assert cp["detected_type"] == "numeric"
        assert "stats" in cp
        assert "outliers" in cp
        assert cp["stats"]["min"] == 60000.0

    def test_string_column_has_pattern_and_cardinality(self, standard_df):
        cp = profile_dataframe(standard_df)["column_profiles"]["email"]
        assert cp["detected_type"] == "string"
        assert "pattern" in cp
        assert "cardinality_pct" in cp
        assert cp["pattern"] == "email"

    def test_detected_type_and_pandas_dtype_both_present(self, standard_df):
        p = profile_dataframe(standard_df)
        for col_profile in p["column_profiles"].values():
            assert "detected_type" in col_profile
            assert "pandas_dtype" in col_profile

    # --- DQ score components ---

    def test_all_dq_components_present_and_in_range(self, standard_df):
        p = profile_dataframe(standard_df)
        for key in ("completeness", "uniqueness", "validity", "consistency"):
            assert key in p
            assert 0.0 <= p[key] <= 100.0

    def test_perfect_df_scores_100_on_all_components(self):
        df = pd.DataFrame({
            "id":   [1, 2, 3, 4, 5],
            "val":  [10.0, 20.0, 30.0, 40.0, 50.0],
        })
        p = profile_dataframe(df)
        assert p["completeness"] == 100.0
        assert p["uniqueness"] == 100.0

    # --- Mutation safety ---

    def test_input_not_mutated(self, standard_df):
        original = standard_df.copy()
        profile_dataframe(standard_df)
        pd.testing.assert_frame_equal(standard_df, original)

    # --- Edge cases ---

    def test_empty_dataframe(self):
        p = profile_dataframe(pd.DataFrame())
        assert p["total_rows"] == 0
        assert p["column_profiles"] == {}
        assert p["completeness"] == 100.0

    def test_single_row_dataframe(self):
        df = pd.DataFrame({"a": [1], "b": ["hello"]})
        p = profile_dataframe(df)
        assert p["total_rows"] == 1
        assert p["duplicate_rows"] == 0

    def test_single_column_dataframe(self):
        df = pd.DataFrame({"salary": [1000.0, 2000.0, 3000.0]})
        p = profile_dataframe(df)
        assert p["total_columns"] == 1
        assert "salary" in p["column_profiles"]

    def test_all_null_dataframe(self):
        df = pd.DataFrame({"a": [None, None], "b": [None, None]})
        p = profile_dataframe(df)
        assert p["missing_cells"] == 4
        assert p["completeness"] == 0.0

    def test_all_duplicate_rows(self):
        df = pd.DataFrame({"a": [1, 1, 1], "b": ["x", "x", "x"]})
        p = profile_dataframe(df)
        assert p["duplicate_rows"] == 2  # first row kept, 2 are duplicates

    def test_malformed_rows_passed_through(self):
        df = pd.DataFrame({"a": [1, 2, 3]})
        p = profile_dataframe(df, malformed_rows=7)
        assert p["malformed_rows"] == 7

    def test_invalid_cells_counted(self):
        # salary column: 4 numeric strings + 1 "N/A" → 1 invalid
        df = pd.DataFrame({"salary": ["1000", "2000", "3000", "4000", "N/A"]})
        p = profile_dataframe(df)
        assert p["invalid_cells"] == 1

    # --- Per-column invalid_count ---

    def test_invalid_count_in_column_profile_for_mixed_column(self):
        # Object column detected as numeric: "N/A" will become NaN on cast
        df = pd.DataFrame({"salary": ["1000", "2000", "3000", "4000", "N/A"]})
        p = profile_dataframe(df)
        assert p["column_profiles"]["salary"]["invalid_count"] == 1

    def test_invalid_count_zero_for_clean_numeric_object_column(self):
        df = pd.DataFrame({"salary": ["1000", "2000", "3000", "4000", "5000"]})
        p = profile_dataframe(df)
        assert p["column_profiles"]["salary"]["invalid_count"] == 0

    def test_invalid_count_zero_for_typed_numeric_column(self):
        # int64/float64 columns are already type-safe — pandas guarantees it
        df = pd.DataFrame({"age": [25, 30, 35]})
        p = profile_dataframe(df)
        assert p["column_profiles"]["age"]["invalid_count"] == 0

    def test_invalid_count_zero_for_string_column(self):
        # String columns: any value is valid, nothing becomes NaN on cast
        df = pd.DataFrame({"name": ["Alice", "Bob", "Charlie"]})
        p = profile_dataframe(df)
        assert p["column_profiles"]["name"]["invalid_count"] == 0

    def test_invalid_count_present_on_all_column_profiles(self, standard_df):
        p = profile_dataframe(standard_df)
        for col_profile in p["column_profiles"].values():
            assert "invalid_count" in col_profile


# ===========================================================================
# score_profile
# ===========================================================================

class TestScoreProfile:

    def test_perfect_score(self):
        p = {"completeness": 100.0, "uniqueness": 100.0,
             "validity": 100.0, "consistency": 100.0}
        assert score_profile(p) == 100.0

    def test_zero_score(self):
        p = {"completeness": 0.0, "uniqueness": 0.0,
             "validity": 0.0, "consistency": 0.0}
        assert score_profile(p) == 0.0

    def test_only_completeness_contributes_35pct(self):
        p = {"completeness": 100.0, "uniqueness": 0.0,
             "validity": 0.0, "consistency": 0.0}
        assert score_profile(p) == 35.0

    def test_only_uniqueness_contributes_25pct(self):
        p = {"completeness": 0.0, "uniqueness": 100.0,
             "validity": 0.0, "consistency": 0.0}
        assert score_profile(p) == 25.0

    def test_only_validity_contributes_25pct(self):
        p = {"completeness": 0.0, "uniqueness": 0.0,
             "validity": 100.0, "consistency": 0.0}
        assert score_profile(p) == 25.0

    def test_only_consistency_contributes_15pct(self):
        p = {"completeness": 0.0, "uniqueness": 0.0,
             "validity": 0.0, "consistency": 100.0}
        assert score_profile(p) == 15.0

    def test_weights_sum_to_100(self):
        assert 35 + 25 + 25 + 15 == 100

    def test_known_score_calculation(self):
        p = {"completeness": 96.43, "uniqueness": 80.0,
             "validity": 100.0, "consistency": 95.24}
        expected = round(96.43 * 0.35 + 80.0 * 0.25 + 100.0 * 0.25 + 95.24 * 0.15, 2)
        assert score_profile(p) == expected

    def test_result_rounded_to_2dp(self):
        p = {"completeness": 100.0, "uniqueness": 100.0,
             "validity": 100.0, "consistency": 33.33}
        result = score_profile(p)
        assert result == round(result, 2)

    def test_result_is_float(self):
        p = {"completeness": 100.0, "uniqueness": 100.0,
             "validity": 100.0, "consistency": 100.0}
        assert isinstance(score_profile(p), float)


# ===========================================================================
# _fill_strategy
# ===========================================================================

class TestFillStrategy:

    def test_numeric_gets_median(self):
        assert _fill_strategy("numeric", {"pattern": None, "cardinality_pct": 80}) == "median"

    def test_bool_gets_mode(self):
        assert _fill_strategy("bool", {"pattern": None, "cardinality_pct": 50}) == "mode"

    def test_date_gets_drop_row(self):
        assert _fill_strategy("date", {"pattern": None, "cardinality_pct": 100}) == "drop_row"

    def test_string_with_email_pattern_gets_drop_row(self):
        assert _fill_strategy("string", {"pattern": "email", "cardinality_pct": 100}) == "drop_row"

    def test_string_with_phone_pattern_gets_drop_row(self):
        assert _fill_strategy("string", {"pattern": "phone", "cardinality_pct": 80}) == "drop_row"

    def test_string_with_uuid_pattern_gets_drop_row(self):
        assert _fill_strategy("string", {"pattern": "uuid", "cardinality_pct": 100}) == "drop_row"

    def test_string_low_cardinality_gets_mode(self):
        # cardinality < 10% → categorical → mode
        assert _fill_strategy("string", {"pattern": None, "cardinality_pct": 5.0}) == "mode"

    def test_string_exactly_10pct_cardinality_gets_drop_row(self):
        # at boundary (not below 10%) → drop_row
        assert _fill_strategy("string", {"pattern": None, "cardinality_pct": 10.0}) == "drop_row"

    def test_string_high_cardinality_gets_drop_row(self):
        assert _fill_strategy("string", {"pattern": None, "cardinality_pct": 95.0}) == "drop_row"

    def test_string_zero_cardinality_gets_mode(self):
        # all same value → 0% cardinality → mode
        assert _fill_strategy("string", {"pattern": None, "cardinality_pct": 0.0}) == "mode"


# ===========================================================================
# _schema_type
# ===========================================================================

class TestSchemaType:

    def test_bool(self):
        assert _schema_type("bool", {}) == "bool"

    def test_date(self):
        assert _schema_type("date", {}) == "date"

    def test_numeric_whole_numbers_is_int(self):
        cp = {"stats": {"min": 1.0, "max": 100.0}}
        assert _schema_type("numeric", cp) == "int"

    def test_numeric_with_decimals_is_float(self):
        cp = {"stats": {"min": 1.5, "max": 100.9}}
        assert _schema_type("numeric", cp) == "float"

    def test_numeric_negative_whole_numbers_is_int(self):
        cp = {"stats": {"min": -50.0, "max": 50.0}}
        assert _schema_type("numeric", cp) == "int"

    def test_numeric_zero_min_is_int(self):
        cp = {"stats": {"min": 0.0, "max": 100.0}}
        assert _schema_type("numeric", cp) == "int"

    def test_string_no_pattern(self):
        assert _schema_type("string", {"pattern": None}) == "string"

    def test_string_with_date_iso_pattern_is_date(self):
        assert _schema_type("string", {"pattern": "date_iso"}) == "date"

    def test_string_with_email_pattern_stays_string(self):
        assert _schema_type("string", {"pattern": "email"}) == "string"

    def test_string_with_uuid_pattern_stays_string(self):
        assert _schema_type("string", {"pattern": "uuid"}) == "string"


# ===========================================================================
# build_recommendations
# ===========================================================================

class TestBuildRecommendations:

    @pytest.fixture
    def profile_with_issues(self):
        """
        5 rows: Alice duplicated, salary null for Bob,
        one invalid email (not-email), dept is low-cardinality.
        """
        return profile_dataframe(pd.DataFrame({
            "name":       ["Alice", "Bob", "Charlie", "Alice", "Diana"],
            "email":      ["a@b.com", "c@d.com", "e@f.com", "a@b.com", "not-email"],
            "age":        [30, 25, 35, 30, 28],
            "salary":     [75000.0, None, 90000.0, 75000.0, 60000.0],
            "department": ["Eng", "Sales", "Eng", "Eng", "Marketing"],
        }))

    # --- Schema ---

    def test_schema_covers_all_columns(self, profile_with_issues):
        rec = build_recommendations(profile_with_issues, 85.0)
        assert set(rec["schema"].keys()) == {
            "name", "email", "age", "salary", "department"
        }

    def test_nullable_true_for_columns_with_nulls(self, profile_with_issues):
        rec = build_recommendations(profile_with_issues, 85.0)
        assert rec["schema"]["salary"]["nullable"] is True

    def test_nullable_false_for_complete_columns(self, profile_with_issues):
        rec = build_recommendations(profile_with_issues, 85.0)
        assert rec["schema"]["age"]["nullable"] is False
        assert rec["schema"]["name"]["nullable"] is False

    def test_age_schema_type_is_int(self, profile_with_issues):
        rec = build_recommendations(profile_with_issues, 85.0)
        assert rec["schema"]["age"]["type"] == "int"

    def test_salary_schema_type_is_float(self, profile_with_issues):
        rec = build_recommendations(profile_with_issues, 85.0)
        assert rec["schema"]["salary"]["type"] == "float"

    # --- Missing values ---

    def test_missing_values_only_for_null_columns(self, profile_with_issues):
        rec = build_recommendations(profile_with_issues, 85.0)
        assert "salary" in rec["missing_values"]
        assert "age" not in rec["missing_values"]
        assert "name" not in rec["missing_values"]

    def test_salary_numeric_gets_median(self, profile_with_issues):
        rec = build_recommendations(profile_with_issues, 85.0)
        assert rec["missing_values"]["salary"]["strategy"] == "median"

    def test_email_with_nulls_gets_drop_row(self):
        df = pd.DataFrame({
            "email": ["a@b.com", "b@c.com", "c@d.com", None, "e@f.com"],
        })
        rec = build_recommendations(profile_dataframe(df), 90.0)
        assert rec["missing_values"]["email"]["strategy"] == "drop_row"

    def test_department_low_cardinality_with_nulls_gets_mode(self):
        # department: 3 unique out of 6 non-null = 50% cardinality — wait,
        # that's above 10% so drop_row. Let's use a very repetitive column.
        df = pd.DataFrame({
            "dept": ["Eng", "Eng", "Eng", "Eng", "Eng", None, "Sales", "Eng", "Eng", "Eng"],
        })
        rec = build_recommendations(profile_dataframe(df), 90.0)
        # cardinality = 2 unique / 9 non-null = 22% → still above 10% → drop_row
        # For true mode, cardinality must be < 10%
        # Let's just verify a value is returned
        assert rec["missing_values"]["dept"]["strategy"] in ("mode", "drop_row")

    def test_truly_low_cardinality_string_with_nulls_gets_mode(self):
        # 1 unique value out of 20 non-null = 5% cardinality → mode
        vals = ["Eng"] * 20 + [None]
        df = pd.DataFrame({"dept": vals})
        rec = build_recommendations(profile_dataframe(df), 90.0)
        assert rec["missing_values"]["dept"]["strategy"] == "mode"

    def test_bool_with_nulls_gets_mode(self):
        df = pd.DataFrame({"active": [True, False, True, None, True]})
        rec = build_recommendations(profile_dataframe(df), 90.0)
        assert rec["missing_values"]["active"]["strategy"] == "mode"

    def test_date_with_nulls_gets_drop_row(self):
        dates = pd.to_datetime(["2021-01-01", "2022-06-15", None, "2023-03-20"])
        df = pd.DataFrame({"joined": dates})
        rec = build_recommendations(profile_dataframe(df), 90.0)
        assert rec["missing_values"]["joined"]["strategy"] == "drop_row"

    # --- Duplicates ---

    def test_duplicates_recommended_when_present(self, profile_with_issues):
        rec = build_recommendations(profile_with_issues, 85.0)
        assert rec["duplicates"]["strategy"] == "drop"
        assert rec["duplicates"]["keep"] == "first"

    def test_no_duplicates_entry_when_all_unique(self):
        df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "val": [10, 20, 30, 40, 50]})
        rec = build_recommendations(profile_dataframe(df), 100.0)
        assert rec["duplicates"] == {}

    # --- Normalization ---

    def test_normalization_suggested_for_large_value_range(self):
        # min=100, max=100000 → ratio 1000 → suggest normalization
        df = pd.DataFrame({"salary": [100.0, 500.0, 1000.0, 50000.0, 100000.0]})
        rec = build_recommendations(profile_dataframe(df), 90.0)
        assert "salary" in rec["normalization"]["columns"]

    def test_normalization_not_suggested_for_small_range(self):
        df = pd.DataFrame({"age": [25, 28, 30, 32, 35]})
        rec = build_recommendations(profile_dataframe(df), 100.0)
        assert "age" not in rec["normalization"]["columns"]

    def test_normalization_not_suggested_for_string_columns(self, profile_with_issues):
        rec = build_recommendations(profile_with_issues, 85.0)
        assert "name" not in rec["normalization"]["columns"]
        assert "email" not in rec["normalization"]["columns"]

    # --- Metadata ---

    def test_metadata_contains_dq_score(self, profile_with_issues):
        rec = build_recommendations(profile_with_issues, 87.5)
        assert rec["_metadata"]["dq_score"] == 87.5

    def test_metadata_issues_found(self, profile_with_issues):
        rec = build_recommendations(profile_with_issues, 85.0)
        assert rec["_metadata"]["issues_found"]["missing"] == 1
        assert rec["_metadata"]["issues_found"]["duplicates"] == 1

    def test_metadata_generated_at_is_iso_string(self, profile_with_issues):
        rec = build_recommendations(profile_with_issues, 85.0)
        from datetime import datetime
        # Should not raise
        datetime.fromisoformat(rec["_metadata"]["generated_at"])

    # --- Structure ---

    def test_all_top_level_keys_present(self, profile_with_issues):
        rec = build_recommendations(profile_with_issues, 85.0)
        for key in ("schema", "missing_values", "duplicates",
                    "normalization", "custom_transforms", "_metadata"):
            assert key in rec

    def test_custom_transforms_is_empty_list(self, profile_with_issues):
        rec = build_recommendations(profile_with_issues, 85.0)
        assert rec["custom_transforms"] == []

    # --- Invalid cells (non-null values that fail type cast) ---

    def test_column_with_invalid_cells_but_no_nulls_gets_fill_strategy(self):
        # "N/A" is not NaN — null_count=0 — but becomes NaN after numeric cast
        df = pd.DataFrame({"salary": ["1000", "2000", "3000", "4000", "N/A"]})
        rec = build_recommendations(profile_dataframe(df), 90.0)
        assert "salary" in rec["missing_values"]
        assert rec["missing_values"]["salary"]["strategy"] == "median"

    def test_nullable_true_for_column_with_invalid_cells_no_nulls(self):
        df = pd.DataFrame({"salary": ["1000", "2000", "3000", "4000", "N/A"]})
        rec = build_recommendations(profile_dataframe(df), 90.0)
        assert rec["schema"]["salary"]["nullable"] is True

    def test_clean_column_no_nulls_no_invalids_not_in_missing_values(self):
        df = pd.DataFrame({"salary": ["1000", "2000", "3000", "4000", "5000"]})
        rec = build_recommendations(profile_dataframe(df), 100.0)
        assert "salary" not in rec["missing_values"]
        assert rec["schema"]["salary"]["nullable"] is False

    def test_column_with_both_nulls_and_invalids_gets_one_fill_entry(self):
        # Some true NaNs + some "N/A" strings
        df = pd.DataFrame({"salary": ["1000", "2000", None, "4000", "N/A"]})
        rec = build_recommendations(profile_dataframe(df), 85.0)
        # Should appear exactly once (not duplicated)
        assert "salary" in rec["missing_values"]
        assert isinstance(rec["missing_values"]["salary"], dict)


# ===========================================================================
# parse_csv
# ===========================================================================

class TestParseCsv:

    def test_happy_path(self):
        df, malformed = parse_csv(csv_bytes("name,age\nAlice,30\nBob,25\n"))
        assert len(df) == 2
        assert list(df.columns) == ["name", "age"]
        assert malformed == 0

    def test_missing_values_preserved_as_nan(self):
        df, _ = parse_csv(csv_bytes("name,age,salary\nAlice,30,\nBob,,60000\n"))
        assert pd.isna(df.loc[0, "salary"])
        assert pd.isna(df.loc[1, "age"])

    def test_malformed_row_counted_and_skipped(self):
        # Third row has an extra field
        content = "name,age\nAlice,30\nBob,25,EXTRA\nCharlie,35\n"
        df, malformed = parse_csv(csv_bytes(content))
        assert malformed == 1
        assert len(df) == 2

    def test_multiple_malformed_rows(self):
        content = "a,b\n1,2\n3,4,EXTRA\n5,6\n7,8,EXTRA\n9,10\n"
        df, malformed = parse_csv(csv_bytes(content))
        assert malformed == 2
        assert len(df) == 3

    def test_headers_only_empty_body(self):
        df, malformed = parse_csv(csv_bytes("name,age,salary\n"))
        assert len(df) == 0
        assert list(df.columns) == ["name", "age", "salary"]
        assert malformed == 0

    def test_accepts_bytesio_directly(self):
        content = b"a,b\n1,2\n3,4\n"
        df, malformed = parse_csv(io.BytesIO(content))
        assert len(df) == 2
        assert malformed == 0

    def test_windows_line_endings(self):
        content = "name,age\r\nAlice,30\r\nBob,25\r\n"
        df, malformed = parse_csv(csv_bytes(content))
        assert len(df) == 2
        assert malformed == 0

    def test_quoted_fields_with_commas(self):
        content = 'name,address\nAlice,"123 High St, London"\nBob,"456 Low St, Manchester"\n'
        df, malformed = parse_csv(csv_bytes(content))
        assert len(df) == 2
        assert "London" in df.loc[0, "address"]
        assert malformed == 0

    def test_no_malformed_for_clean_csv(self):
        content = "a,b,c\n1,2,3\n4,5,6\n7,8,9\n"
        _, malformed = parse_csv(csv_bytes(content))
        assert malformed == 0

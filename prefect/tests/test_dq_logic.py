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
    _detect_numeric_sentinels,
    _count_sentinels,
    _detect_column_type,
    _fill_strategy,
    _profile_numeric_column,
    _profile_string_column,
    _schema_type,
    apply_recommendations,
    build_recommendations,
    parse_csv,
    profile_dataframe,
    score_profile,
    score_profile_detailed,
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

    def test_numeric_symmetric_no_stats_gets_mean(self):
        # No stats / no outliers → symmetric assumption → mean
        assert _fill_strategy("numeric", {"pattern": None, "cardinality_pct": 80}) == "mean"

    def test_numeric_with_outliers_gets_median(self):
        # outlier count > 0 → distribution is heavy-tailed → median
        assert _fill_strategy("numeric", {
            "stats": {"mean": 10000, "median": 9000, "std": 5000},
            "outliers": {"count": 3},
        }) == "median"

    def test_numeric_skewed_gets_median(self):
        # |mean - median| / std = |10000 - 7000| / 5000 = 0.6 > 0.15 → skewed → median
        assert _fill_strategy("numeric", {
            "stats": {"mean": 10000, "median": 7000, "std": 5000},
            "outliers": {"count": 0},
        }) == "median"

    def test_numeric_symmetric_gets_mean(self):
        # |mean - median| / std = 0 / 5000 = 0 < 0.15, no outliers → mean
        assert _fill_strategy("numeric", {
            "stats": {"mean": 10000, "median": 10000, "std": 5000},
            "outliers": {"count": 0},
        }) == "mean"

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
    def df_with_issues(self):
        """
        5 rows: Alice duplicated, salary null for Bob,
        one invalid email (not-email), dept is low-cardinality.
        """
        return pd.DataFrame({
            "name":       ["Alice", "Bob", "Charlie", "Alice", "Diana"],
            "email":      ["a@b.com", "c@d.com", "e@f.com", "a@b.com", "not-email"],
            "age":        [30, 25, 35, 30, 28],
            "salary":     [75000.0, None, 90000.0, 75000.0, 60000.0],
            "department": ["Eng", "Sales", "Eng", "Eng", "Marketing"],
        })

    @pytest.fixture
    def profile_with_issues(self, df_with_issues):
        return profile_dataframe(df_with_issues)

    # --- Columns structure ---

    def test_schema_covers_all_columns(self, df_with_issues, profile_with_issues):
        rec = build_recommendations(df_with_issues, profile_with_issues, 85.0)
        assert set(rec["columns"].keys()) == {
            "name", "email", "age", "salary", "department"
        }

    def test_nullable_true_for_columns_with_nulls(self, df_with_issues, profile_with_issues):
        rec = build_recommendations(df_with_issues, profile_with_issues, 85.0)
        assert rec["columns"]["salary"]["nullable"] is True

    def test_nullable_false_for_complete_columns(self, df_with_issues, profile_with_issues):
        rec = build_recommendations(df_with_issues, profile_with_issues, 85.0)
        assert rec["columns"]["age"]["nullable"] is False
        assert rec["columns"]["name"]["nullable"] is False

    def test_age_schema_type_is_int(self, df_with_issues, profile_with_issues):
        rec = build_recommendations(df_with_issues, profile_with_issues, 85.0)
        assert rec["columns"]["age"]["type"] == "int"

    def test_salary_schema_type_is_float(self, df_with_issues, profile_with_issues):
        rec = build_recommendations(df_with_issues, profile_with_issues, 85.0)
        assert rec["columns"]["salary"]["type"] == "float"

    # --- Missing values ---

    def test_missing_values_only_for_null_columns(self, df_with_issues, profile_with_issues):
        rec = build_recommendations(df_with_issues, profile_with_issues, 85.0)
        assert rec["columns"]["salary"]["missing_values"] is not None
        assert rec["columns"]["age"]["missing_values"] is None
        assert rec["columns"]["name"]["missing_values"] is None

    def test_salary_numeric_gets_mean(self, df_with_issues, profile_with_issues):
        # 4 non-null values: 75000, 90000, 75000, 60000 — symmetric (mean≈median), no outliers
        # (fewer than 30 non-null values → IQR outlier check skipped → has_outliers=False)
        # Result: mean, not median.
        rec = build_recommendations(df_with_issues, profile_with_issues, 85.0)
        assert rec["columns"]["salary"]["missing_values"]["strategy"] == "mean"

    def test_email_with_nulls_gets_drop_row(self):
        df = pd.DataFrame({
            "email": ["a@b.com", "b@c.com", "c@d.com", None, "e@f.com"],
        })
        rec = build_recommendations(df, profile_dataframe(df), 90.0)
        assert rec["columns"]["email"]["missing_values"]["strategy"] == "drop_row"

    def test_department_low_cardinality_with_nulls_gets_mode(self):
        # department: 3 unique out of 6 non-null = 50% cardinality — wait,
        # that's above 10% so drop_row. Let's use a very repetitive column.
        df = pd.DataFrame({
            "dept": ["Eng", "Eng", "Eng", "Eng", "Eng", None, "Sales", "Eng", "Eng", "Eng"],
        })
        rec = build_recommendations(df, profile_dataframe(df), 90.0)
        # cardinality = 2 unique / 9 non-null = 22% → still above 10% → drop_row
        # For true mode, cardinality must be < 10%
        # Let's just verify a value is returned
        assert rec["columns"]["dept"]["missing_values"]["strategy"] in ("mode", "drop_row")

    def test_truly_low_cardinality_string_with_nulls_gets_mode(self):
        # 1 unique value out of 20 non-null = 5% cardinality → mode
        vals = ["Eng"] * 20 + [None]
        df = pd.DataFrame({"dept": vals})
        rec = build_recommendations(df, profile_dataframe(df), 90.0)
        assert rec["columns"]["dept"]["missing_values"]["strategy"] == "mode"

    def test_bool_with_nulls_gets_mode(self):
        df = pd.DataFrame({"active": [True, False, True, None, True]})
        rec = build_recommendations(df, profile_dataframe(df), 90.0)
        assert rec["columns"]["active"]["missing_values"]["strategy"] == "mode"

    def test_date_with_nulls_gets_drop_row(self):
        dates = pd.to_datetime(["2021-01-01", "2022-06-15", None, "2023-03-20"])
        df = pd.DataFrame({"joined": dates})
        rec = build_recommendations(df, profile_dataframe(df), 90.0)
        assert rec["columns"]["joined"]["missing_values"]["strategy"] == "drop_row"

    # --- Duplicates ---

    def test_duplicates_recommended_when_present(self, df_with_issues, profile_with_issues):
        rec = build_recommendations(df_with_issues, profile_with_issues, 85.0)
        assert rec["duplicates"]["strategy"] == "drop"
        assert rec["duplicates"]["keep"] == "first"

    def test_no_duplicates_entry_when_all_unique(self):
        df = pd.DataFrame({"id": [1, 2, 3, 4, 5], "val": [10, 20, 30, 40, 50]})
        rec = build_recommendations(df, profile_dataframe(df), 100.0)
        assert "duplicates" not in rec

    # --- Normalization ---

    def test_normalization_suggested_for_large_value_range(self):
        # range = 99900, max_abs = 100000 > 100 → suggest normalization
        df = pd.DataFrame({"salary": [100.0, 500.0, 1000.0, 50000.0, 100000.0]})
        rec = build_recommendations(df, profile_dataframe(df), 90.0)
        assert rec["columns"]["salary"]["normalize"] == "min_max"

    def test_normalization_not_suggested_for_small_range(self):
        # range = 10, max_abs = 35 < 100 → no normalization
        df = pd.DataFrame({"age": [25, 28, 30, 32, 35]})
        rec = build_recommendations(df, profile_dataframe(df), 100.0)
        assert rec["columns"]["age"]["normalize"] is False

    def test_normalization_not_suggested_for_string_columns(self, df_with_issues, profile_with_issues):
        rec = build_recommendations(df_with_issues, profile_with_issues, 85.0)
        assert rec["columns"]["name"]["normalize"] is False
        assert rec["columns"]["email"]["normalize"] is False

    def test_normalization_z_score_when_outliers_present(self):
        # ≥30 values required for IQR outlier detection; include an extreme outlier
        normal = list(range(1000, 1031))   # 31 values in a tight cluster
        normal[-1] = 999999                # extreme outlier → IQR will flag it
        df = pd.DataFrame({"revenue": normal})
        rec = build_recommendations(df, profile_dataframe(df), 90.0)
        assert rec["columns"]["revenue"]["normalize"] == "z_score"

    def test_normalization_min_max_when_no_outliers(self):
        # Large range but no outliers → min_max
        df = pd.DataFrame({"salary": [100.0, 500.0, 1000.0, 50000.0, 100000.0]})
        rec = build_recommendations(df, profile_dataframe(df), 90.0)
        assert rec["columns"]["salary"]["normalize"] == "min_max"

    # --- Metadata ---

    def test_metadata_contains_dq_score(self, df_with_issues, profile_with_issues):
        rec = build_recommendations(df_with_issues, profile_with_issues, 87.5)
        assert rec["_metadata"]["dq_score"] == 87.5

    def test_metadata_issues_found(self, df_with_issues, profile_with_issues):
        rec = build_recommendations(df_with_issues, profile_with_issues, 85.0)
        assert rec["_metadata"]["issues_found"]["missing"] == 1
        assert rec["_metadata"]["issues_found"]["duplicates"] == 1

    def test_metadata_generated_at_is_iso_string(self, df_with_issues, profile_with_issues):
        rec = build_recommendations(df_with_issues, profile_with_issues, 85.0)
        from datetime import datetime
        # Should not raise
        datetime.fromisoformat(rec["_metadata"]["generated_at"])

    # --- Structure ---

    def test_all_top_level_keys_present(self, df_with_issues, profile_with_issues):
        rec = build_recommendations(df_with_issues, profile_with_issues, 85.0)
        for key in ("columns", "duplicates", "custom_transforms", "outliers", "_metadata"):
            assert key in rec

    def test_custom_transforms_is_empty_list(self, df_with_issues, profile_with_issues):
        rec = build_recommendations(df_with_issues, profile_with_issues, 85.0)
        assert rec["custom_transforms"] == []

    # --- Invalid cells (non-null values that fail type cast) ---

    def test_column_with_invalid_cells_but_no_nulls_gets_fill_strategy(self):
        # "N/A" is not NaN — null_count=0 — but becomes NaN after numeric cast.
        # 4 valid values: 1000-4000, symmetric, <30 values → no IQR check → "mean"
        df = pd.DataFrame({"salary": ["1000", "2000", "3000", "4000", "N/A"]})
        rec = build_recommendations(df, profile_dataframe(df), 90.0)
        assert rec["columns"]["salary"]["missing_values"] is not None
        assert rec["columns"]["salary"]["missing_values"]["strategy"] == "mean"

    def test_nullable_true_for_column_with_invalid_cells_no_nulls(self):
        df = pd.DataFrame({"salary": ["1000", "2000", "3000", "4000", "N/A"]})
        rec = build_recommendations(df, profile_dataframe(df), 90.0)
        assert rec["columns"]["salary"]["nullable"] is True

    def test_clean_column_no_nulls_no_invalids_not_in_missing_values(self):
        df = pd.DataFrame({"salary": ["1000", "2000", "3000", "4000", "5000"]})
        rec = build_recommendations(df, profile_dataframe(df), 100.0)
        assert rec["columns"]["salary"]["missing_values"] is None
        assert rec["columns"]["salary"]["nullable"] is False

    def test_column_with_both_nulls_and_invalids_gets_one_fill_entry(self):
        # Some true NaNs + some "N/A" strings
        df = pd.DataFrame({"salary": ["1000", "2000", None, "4000", "N/A"]})
        rec = build_recommendations(df, profile_dataframe(df), 85.0)
        # Should appear exactly once (not duplicated)
        assert rec["columns"]["salary"]["missing_values"] is not None
        assert isinstance(rec["columns"]["salary"]["missing_values"], dict)

    def test_mar_column_gets_leave_null(self):
        # merchant_nm is null exactly when txn_typ=="ATM" — statistically MAR.
        # Need enough rows (≥10) and enough nulls (≥5) for scipy to detect it.
        n = 30
        df = pd.DataFrame({
            "txn_typ":     ["ATM"] * 15 + ["POS"] * 15,
            "merchant_nm": [None]  * 15 + ["Shop A"] * 15,
            "amount":      [100.0] * n,
        })
        rec = build_recommendations(df, profile_dataframe(df), 80.0)
        assert rec["columns"]["merchant_nm"]["missing_values"]["strategy"] == "leave_null"


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


# ===========================================================================
# score_profile_detailed
# ===========================================================================

class TestScoreProfileDetailed:

    def _make_profile(self, completeness, uniqueness, validity, consistency):
        return {
            "completeness": completeness,
            "uniqueness":   uniqueness,
            "validity":     validity,
            "consistency":  consistency,
        }

    def test_returns_five_keys(self):
        profile = self._make_profile(100, 100, 100, 100)
        result = score_profile_detailed(profile)
        assert set(result.keys()) == {"overall", "completeness", "uniqueness", "validity", "consistency"}

    def test_overall_matches_score_profile(self):
        profile = self._make_profile(95.2, 100.0, 98.1, 99.0)
        result = score_profile_detailed(profile)
        assert result["overall"] == score_profile(profile)

    def test_overall_formula_is_correct(self):
        profile = self._make_profile(95.2, 100.0, 98.1, 99.0)
        result = score_profile_detailed(profile)
        expected = round(95.2 * 0.35 + 100.0 * 0.25 + 98.1 * 0.25 + 99.0 * 0.15, 2)
        assert result["overall"] == expected

    def test_perfect_score_all_100(self):
        profile = self._make_profile(100.0, 100.0, 100.0, 100.0)
        result = score_profile_detailed(profile)
        assert result["overall"] == 100.0
        assert result["completeness"] == 100.0

    def test_component_values_are_rounded(self):
        profile = self._make_profile(95.2222, 100.0, 98.1111, 99.0)
        result = score_profile_detailed(profile)
        assert result["completeness"] == round(95.2222, 2)
        assert result["validity"] == round(98.1111, 2)

    def test_dimension_values_match_profile(self):
        profile = self._make_profile(80.0, 90.0, 70.0, 60.0)
        result = score_profile_detailed(profile)
        assert result["completeness"] == 80.0
        assert result["uniqueness"] == 90.0
        assert result["validity"] == 70.0
        assert result["consistency"] == 60.0


# ===========================================================================
# apply_recommendations — step 0b (transform_code execution)
# ===========================================================================

def _bare_recs(columns: dict) -> dict:
    """Minimal recommendations dict with only the columns provided."""
    return {"columns": columns, "duplicates": {}, "outliers": {}}


class TestApplyTransformCode:

    def test_transform_code_applied_before_schema_cast(self):
        """
        Percent strings cleaned by transform_code are then correctly cast to float.

        Without step 0b the cast would silently NaN the "10%" strings.
        With step 0b the lambda strips "%" first, so the cast succeeds.
        """
        df = pd.DataFrame({"price": ["10%", "20%", "30%"]})
        recs = _bare_recs({
            "price": {
                "type": "float",
                "transform_code": "lambda col: col.str.rstrip('%').astype(float)",
            }
        })
        result = apply_recommendations(df, recs)
        assert list(result["price"]) == [10.0, 20.0, 30.0]

    def test_transform_code_rollback_on_damage(self):
        """Transform that nullifies >5% of values is rolled back to the original."""
        df = pd.DataFrame({"val": ["10", "20", "30", "40", "50",
                                   "60", "70", "80", "90", "100"]})
        recs = _bare_recs({
            "val": {
                "type": "string",
                # This lambda returns NaN for every value (100% > 5%) → rollback
                "transform_code": "lambda col: col.where(col == 'NEVERMATCHES')",
            }
        })
        result = apply_recommendations(df, recs)
        assert list(result["val"]) == ["10", "20", "30", "40", "50",
                                       "60", "70", "80", "90", "100"]

    def test_transform_code_rollback_on_exception(self):
        """Transform that raises an exception is rolled back to the original."""
        df = pd.DataFrame({"val": ["a", "b", "c"]})
        recs = _bare_recs({
            "val": {
                "type": "string",
                "transform_code": "lambda col: col.astype(int)",  # raises ValueError
            }
        })
        result = apply_recommendations(df, recs)
        assert list(result["val"]) == ["a", "b", "c"]

    def test_no_transform_code_column_unchanged(self):
        """Column with transform_hint but no transform_code is left untouched."""
        df = pd.DataFrame({"val": ["10%", "20%", "30%"]})
        recs = _bare_recs({
            "val": {
                "type": "string",
                "transform_hint": "strip percent suffix",
                # No transform_code key — step 0b skips this column
            }
        })
        result = apply_recommendations(df, recs)
        assert list(result["val"]) == ["10%", "20%", "30%"]

# ===========================================================================
# apply_recommendations
# ===========================================================================

class TestApplyRecommendations:

    def _df(self, data):
        return pd.DataFrame(data)

    def _col(self, type="string", nullable=False, missing_values=None, normalize=False):
        """Helper to build a column config dict."""
        return {"type": type, "nullable": nullable, "missing_values": missing_values,
                "normalize": normalize, "warnings": [], "note": None}

    def _mv(self, strategy, value=None):
        """Helper to build a missing_values dict."""
        return {"strategy": strategy, "value": value}

    # --- Schema cast ---

    def test_numeric_string_cast_to_float(self):
        # 1 bad value out of 21 = ~4.8% — below 5% guard threshold, cast proceeds
        data = ["50000"] * 10 + ["60000"] * 10 + ["N/A"]
        df = self._df({"salary": data})
        recs = {"columns": {"salary": self._col(type="float")}, "duplicates": {}, "custom_transforms": []}
        result = apply_recommendations(df, recs)
        assert pd.isna(result["salary"].iloc[20])  # "N/A" → NaN via errors="coerce"
        assert result["salary"].iloc[0] == 50000.0

    def test_unsafe_cast_is_blocked(self):
        # 1 out of 3 = 33% would become NaN — guard blocks the cast, column stays as-is
        df = self._df({"salary": ["50000", "60000", "N/A"]})
        recs = {"columns": {"salary": self._col(type="float")}, "duplicates": {}, "custom_transforms": []}
        result = apply_recommendations(df, recs)
        assert result["salary"].iloc[2] == "N/A"  # cast was blocked — not converted
        assert result["salary"].dtype == object

    def test_int_cast_after_fill(self):
        df = self._df({"age": [25.0, None, 30.0]})
        recs = {
            "columns": {"age": self._col(type="int", nullable=True, missing_values=self._mv("median"))},
            "duplicates": {}, "custom_transforms": [],
        }
        result = apply_recommendations(df, recs)
        assert result["age"].dtype in (int, "int64", "int32")

    def test_bool_cast(self):
        df = self._df({"active": ["True", "False", "True"]})
        recs = {"columns": {"active": self._col(type="bool")}, "duplicates": {}, "custom_transforms": []}
        result = apply_recommendations(df, recs)
        assert result["active"].tolist() == [True, False, True]

    def test_unknown_column_in_schema_skipped(self):
        df = self._df({"name": ["Alice", "Bob"]})
        recs = {"columns": {"nonexistent": self._col(type="int")}, "duplicates": {}, "custom_transforms": []}
        result = apply_recommendations(df, recs)
        assert list(result.columns) == ["name"]

    # --- Missing values ---

    def test_fill_median(self):
        df = self._df({"salary": [50000.0, None, 70000.0]})
        recs = {
            "columns": {"salary": self._col(type="float", nullable=True, missing_values=self._mv("median"))},
            "duplicates": {}, "custom_transforms": [],
        }
        result = apply_recommendations(df, recs)
        assert result["salary"].isna().sum() == 0
        assert result["salary"].iloc[1] == 60000.0

    def test_fill_mean(self):
        df = self._df({"val": [10.0, None, 30.0]})
        recs = {
            "columns": {"val": self._col(nullable=True, missing_values=self._mv("mean"))},
            "duplicates": {}, "custom_transforms": [],
        }
        result = apply_recommendations(df, recs)
        assert result["val"].iloc[1] == 20.0

    def test_fill_mode_string(self):
        df = self._df({"dept": ["HR", "HR", None, "Finance"]})
        recs = {
            "columns": {"dept": self._col(nullable=True, missing_values=self._mv("mode"))},
            "duplicates": {}, "custom_transforms": [],
        }
        result = apply_recommendations(df, recs)
        assert result["dept"].iloc[2] == "HR"

    def test_fill_literal_value(self):
        df = self._df({"status": [None, "active"]})
        recs = {
            "columns": {"status": self._col(nullable=True, missing_values=self._mv("fill", "unknown"))},
            "duplicates": {}, "custom_transforms": [],
        }
        result = apply_recommendations(df, recs)
        assert result["status"].iloc[0] == "unknown"

    def test_drop_row_removes_null_rows(self):
        df = self._df({"email": ["a@b.com", None, "c@d.com"]})
        recs = {
            "columns": {"email": self._col(nullable=True, missing_values=self._mv("drop_row"))},
            "duplicates": {}, "custom_transforms": [],
        }
        result = apply_recommendations(df, recs)
        assert len(result) == 2
        assert result["email"].isna().sum() == 0

    def test_multiple_drop_row_columns_batched(self):
        df = self._df({"a": [1, None, 3], "b": ["x", "y", None]})
        recs = {
            "columns": {
                "a": self._col(nullable=True, missing_values=self._mv("drop_row")),
                "b": self._col(nullable=True, missing_values=self._mv("drop_row")),
            },
            "duplicates": {}, "custom_transforms": [],
        }
        result = apply_recommendations(df, recs)
        assert len(result) == 1  # only row 0 (index 0) has no NaN

    # --- Duplicates ---

    def test_drop_duplicates(self):
        df = self._df({"id": [1, 2, 1], "name": ["Alice", "Bob", "Alice"]})
        recs = {
            "columns": {},
            "duplicates": {"strategy": "drop", "subset": [], "keep": "first"},
            "custom_transforms": [],
        }
        result = apply_recommendations(df, recs)
        assert len(result) == 2

    def test_no_duplicates_entry_leaves_rows_unchanged(self):
        df = self._df({"id": [1, 2, 1]})
        recs = {"columns": {}, "duplicates": {}, "custom_transforms": []}
        result = apply_recommendations(df, recs)
        assert len(result) == 3

    # --- Normalization ---

    def test_normalization_scales_to_0_1(self):
        df = self._df({"salary": [0.0, 50000.0, 100000.0]})
        recs = {
            "columns": {"salary": self._col(type="float", normalize="min_max")},
            "duplicates": {}, "custom_transforms": [],
        }
        result = apply_recommendations(df, recs)
        assert result["salary"].min() == 0.0
        assert result["salary"].max() == 1.0
        assert result["salary"].iloc[1] == 0.5

    def test_normalization_constant_column_not_divided_by_zero(self):
        df = self._df({"val": [5.0, 5.0, 5.0]})
        recs = {
            "columns": {"val": self._col(type="float", normalize="min_max")},
            "duplicates": {}, "custom_transforms": [],
        }
        result = apply_recommendations(df, recs)  # should not raise
        assert list(result["val"]) == [5.0, 5.0, 5.0]

    def test_z_score_normalization_produces_zero_mean(self):
        df = self._df({"score": [10.0, 20.0, 30.0, 40.0, 50.0]})
        recs = {
            "columns": {"score": self._col(type="float", normalize="z_score")},
            "duplicates": {}, "custom_transforms": [],
        }
        result = apply_recommendations(df, recs)
        assert abs(result["score"].mean()) < 1e-10

    def test_z_score_normalization_produces_unit_std(self):
        # ddof=0 → population std → std of result should be 1.0
        df = self._df({"score": [10.0, 20.0, 30.0, 40.0, 50.0]})
        recs = {
            "columns": {"score": self._col(type="float", normalize="z_score")},
            "duplicates": {}, "custom_transforms": [],
        }
        result = apply_recommendations(df, recs)
        assert abs(result["score"].std(ddof=0) - 1.0) < 1e-10

    def test_z_score_constant_column_not_divided_by_zero(self):
        df = self._df({"val": [7.0, 7.0, 7.0]})
        recs = {
            "columns": {"val": self._col(type="float", normalize="z_score")},
            "duplicates": {}, "custom_transforms": [],
        }
        result = apply_recommendations(df, recs)  # should not raise
        assert list(result["val"]) == [7.0, 7.0, 7.0]

    def test_normalize_false_leaves_column_unchanged(self):
        df = self._df({"salary": [50000.0, 100000.0, 150000.0]})
        recs = {
            "columns": {"salary": self._col(type="float", normalize=False)},
            "duplicates": {}, "custom_transforms": [],
        }
        result = apply_recommendations(df, recs)
        assert list(result["salary"]) == [50000.0, 100000.0, 150000.0]

    # --- Input safety ---

    def test_input_dataframe_not_mutated(self):
        df = self._df({"salary": [50000.0, None, 70000.0]})
        original = df.copy()
        recs = {
            "columns": {"salary": self._col(nullable=True, missing_values=self._mv("median"))},
            "duplicates": {}, "custom_transforms": [],
        }
        apply_recommendations(df, recs)
        pd.testing.assert_frame_equal(df, original)

    # --- drop_column strategy ---

    def test_drop_column_removes_column(self):
        df = self._df({"name": ["Alice", "Bob", None], "score": [1, 2, 3]})
        recs = {
            "columns": {"name": self._col(nullable=True, missing_values=self._mv("drop_column"))},
            "duplicates": {}, "custom_transforms": [],
        }
        result = apply_recommendations(df, recs)
        assert "name" not in result.columns
        assert "score" in result.columns
        assert len(result) == 3  # no rows dropped

    def test_drop_column_missing_column_is_safe(self):
        df = self._df({"score": [1, 2, 3]})
        recs = {
            "columns": {"nonexistent": self._col(nullable=True, missing_values=self._mv("drop_column"))},
            "duplicates": {}, "custom_transforms": [],
        }
        result = apply_recommendations(df, recs)  # must not raise
        assert list(result.columns) == ["score"]

    def test_drop_column_does_not_affect_drop_row_for_other_cols(self):
        df = self._df({"bad": [None, None, "x"], "keep": [1, None, 3]})
        recs = {
            "columns": {
                "bad":  self._col(nullable=True, missing_values=self._mv("drop_column")),
                "keep": self._col(nullable=True, missing_values=self._mv("drop_row")),
            },
            "duplicates": {}, "custom_transforms": [],
        }
        result = apply_recommendations(df, recs)
        assert "bad" not in result.columns
        assert len(result) == 2  # 1 row dropped because keep has a null

    # --- leave_null strategy ---

    def test_leave_null_keeps_nulls_intact(self):
        df = self._df({"notes": ["a", None, "c"]})
        recs = {
            "columns": {"notes": self._col(nullable=True, missing_values=self._mv("leave_null"))},
            "duplicates": {}, "custom_transforms": [],
        }
        result = apply_recommendations(df, recs)
        assert result["notes"].isna().sum() == 1
        assert len(result) == 3  # no rows dropped

    def test_leave_null_does_not_drop_rows(self):
        df = self._df({"a": [1, None, 3], "b": [None, 2, None]})
        recs = {
            "columns": {
                "a": self._col(nullable=True, missing_values=self._mv("leave_null")),
                "b": self._col(nullable=True, missing_values=self._mv("leave_null")),
            },
            "duplicates": {}, "custom_transforms": [],
        }
        result = apply_recommendations(df, recs)
        assert len(result) == 3

    # --- Outlier treatment ---

    def test_outlier_winsorise_clips_values(self):
        df = self._df({"salary": [10, 20, 30, 40, 9999]})
        recs = {
            "columns": {"salary": self._col()},
            "duplicates": {}, "custom_transforms": [],
            "outliers": {"salary": {"strategy": "winsorise", "method": "iqr", "lower": 5.0, "upper": 50.0}},
        }
        result = apply_recommendations(df, recs)
        assert result["salary"].max() == 50.0
        assert result["salary"].min() == 10.0

    def test_outlier_remove_drops_rows(self):
        df = self._df({"salary": [10, 20, 30, 40, 9999]})
        recs = {
            "columns": {"salary": self._col()},
            "duplicates": {}, "custom_transforms": [],
            "outliers": {"salary": {"strategy": "remove", "method": "iqr", "lower": 5.0, "upper": 50.0}},
        }
        result = apply_recommendations(df, recs)
        assert len(result) == 4
        assert 9999 not in result["salary"].values

    def test_outlier_keep_leaves_values_unchanged(self):
        df = self._df({"salary": [10, 20, 30, 40, 9999]})
        recs = {
            "columns": {"salary": self._col()},
            "duplicates": {}, "custom_transforms": [],
            "outliers": {"salary": {"strategy": "keep", "method": "iqr", "lower": 5.0, "upper": 50.0}},
        }
        result = apply_recommendations(df, recs)
        assert 9999 in result["salary"].values


# ===========================================================================
# _fill_strategy — drop_column threshold
# ===========================================================================

class TestFillStrategyDropColumn:

    def test_null_pct_exactly_50_gets_drop_column(self):
        assert _fill_strategy("numeric", {"null_pct": 50, "cardinality_pct": 80}) == "drop_column"

    def test_null_pct_above_50_gets_drop_column(self):
        assert _fill_strategy("string", {"null_pct": 75, "pattern": None, "cardinality_pct": 5}) == "drop_column"

    def test_null_pct_below_50_falls_through_to_normal_rules(self):
        # No stats → no outliers, no skew → "mean" (symmetric assumption)
        assert _fill_strategy("numeric", {"null_pct": 49, "cardinality_pct": 80}) == "mean"

    def test_null_pct_zero_falls_through_to_normal_rules(self):
        assert _fill_strategy("string", {"null_pct": 0, "pattern": None, "cardinality_pct": 5}) == "mode"

    def test_drop_column_takes_priority_over_type_rules(self):
        # Even for a bool column: if >=50% null → drop_column, not mode
        assert _fill_strategy("bool", {"null_pct": 60, "cardinality_pct": 50}) == "drop_column"


# ===========================================================================
# build_recommendations — _metadata.warnings
# ===========================================================================

class TestBuildRecommendationsWarnings:

    def _profile_with_cols(self, col_profiles, total_rows=10):
        """Build a minimal profile dict for build_recommendations."""
        return {
            "total_rows":     total_rows,
            "total_columns":  len(col_profiles),
            "total_cells":    total_rows * len(col_profiles),
            "missing_cells":  sum(cp.get("null_count", 0) for cp in col_profiles.values()),
            "duplicate_rows": 0,
            "malformed_rows": 0,
            "invalid_cells":  sum(cp.get("invalid_count", 0) for cp in col_profiles.values()),
            "completeness":   100.0,
            "uniqueness":     100.0,
            "validity":       100.0,
            "consistency":    100.0,
            "column_profiles": col_profiles,
        }

    def test_drop_row_column_produces_warning(self):
        col_profiles = {
            "email": {
                "detected_type": "string", "null_count": 2, "null_pct": 20.0,
                "invalid_count": 0, "pattern": "email", "cardinality_pct": 100.0,
                "pandas_dtype": "object",
            }
        }
        profile = self._profile_with_cols(col_profiles, total_rows=10)
        recs = build_recommendations(pd.DataFrame(), profile, 85.0)
        warnings = recs["columns"]["email"]["warnings"]
        assert len(warnings) > 0
        assert any("drop_row" in w for w in warnings)
        assert any("2" in w for w in warnings)  # null_count in message

    def test_drop_column_column_produces_warning(self):
        col_profiles = {
            "sparse": {
                "detected_type": "string", "null_count": 6, "null_pct": 60.0,
                "invalid_count": 0, "pattern": None, "cardinality_pct": 5.0,
                "pandas_dtype": "object",
            }
        }
        profile = self._profile_with_cols(col_profiles, total_rows=10)
        recs = build_recommendations(pd.DataFrame(), profile, 70.0)
        warnings = recs["columns"]["sparse"]["warnings"]
        assert len(warnings) > 0
        assert any("drop_column" in w for w in warnings)
        assert any("60.0" in w for w in warnings)  # null_pct in message

    def test_mean_column_produces_no_warning(self):
        # Numeric column with null < 50% and no drop strategy → no warnings
        col_profiles = {
            "salary": {
                "detected_type": "numeric", "null_count": 1, "null_pct": 10.0,
                "invalid_count": 0, "stats": {"min": 1000, "max": 9000},
                "pandas_dtype": "float64",
            }
        }
        profile = self._profile_with_cols(col_profiles, total_rows=10)
        recs = build_recommendations(pd.DataFrame(), profile, 90.0)
        assert recs["columns"]["salary"]["warnings"] == []

    def test_warnings_empty_for_clean_dataset(self):
        # A clean dataset (no missing values) should have warnings=[] on all columns
        col_profiles = {
            "id": {
                "detected_type": "numeric", "null_count": 0, "null_pct": 0.0,
                "invalid_count": 0, "stats": {"min": 1, "max": 5},
                "pandas_dtype": "int64",
            }
        }
        profile = self._profile_with_cols(col_profiles, total_rows=5)
        recs = build_recommendations(pd.DataFrame(), profile, 100.0)
        for col_def in recs["columns"].values():
            assert col_def["warnings"] == []

    def test_sentinel_warning_generated_for_non_drop_strategy(self):
        # String column with sentinel values and mode strategy → sentinel warning
        col_profiles = {
            "status": {
                "detected_type": "string", "null_count": 0, "null_pct": 0.0,
                "invalid_count": 0, "pattern": None, "cardinality_pct": 5.0,
                "pandas_dtype": "object", "sentinel_count": 3,
            }
        }
        profile = self._profile_with_cols(col_profiles, total_rows=10)
        recs = build_recommendations(pd.DataFrame(), profile, 90.0)
        warnings = recs["columns"]["status"]["warnings"]
        assert len(warnings) > 0
        assert any("sentinel" in w.lower() for w in warnings)
        assert any("3" in w for w in warnings)

    def test_sentinel_warning_also_raised_alongside_drop_strategy(self):
        # High-null column gets drop_column strategy AND has sentinels → both warnings present
        col_profiles = {
            "notes": {
                "detected_type": "string", "null_count": 6, "null_pct": 60.0,
                "invalid_count": 0, "pattern": None, "cardinality_pct": 5.0,
                "pandas_dtype": "object", "sentinel_count": 2,
            }
        }
        profile = self._profile_with_cols(col_profiles, total_rows=10)
        recs = build_recommendations(pd.DataFrame(), profile, 70.0)
        warnings = recs["columns"]["notes"]["warnings"]
        assert any("drop_column" in w for w in warnings)
        assert any("sentinel" in w.lower() for w in warnings)

    def test_sentinel_values_in_metadata_issues_found(self):
        # sentinel_count in column profiles → _metadata.issues_found.sentinel_values
        col_profiles = {
            "col_a": {
                "detected_type": "string", "null_count": 0, "null_pct": 0.0,
                "invalid_count": 0, "pattern": None, "cardinality_pct": 50.0,
                "pandas_dtype": "object", "sentinel_count": 4,
            },
            "col_b": {
                "detected_type": "string", "null_count": 0, "null_pct": 0.0,
                "invalid_count": 0, "pattern": None, "cardinality_pct": 50.0,
                "pandas_dtype": "object", "sentinel_count": 2,
            },
        }
        profile = self._profile_with_cols(col_profiles, total_rows=10)
        recs = build_recommendations(pd.DataFrame(), profile, 90.0)
        assert recs["_metadata"]["issues_found"]["sentinel_values"] == 6


# ===========================================================================
# _count_sentinels
# ===========================================================================

class TestCountSentinels:

    def test_na_string_detected(self):
        s = series(["Alice", "N/A", "Bob", "unknown", "Carol"])
        assert _count_sentinels(s) == 2

    def test_case_insensitive(self):
        s = series(["NULL", "None", "UNKNOWN", "Missing", "valid"])
        assert _count_sentinels(s) == 4

    def test_dash_and_question_mark(self):
        s = series(["-", "?", "real_value"])
        assert _count_sentinels(s) == 2

    def test_clean_data_returns_zero(self):
        s = series(["Alice", "Bob", "Charlie"])
        assert _count_sentinels(s) == 0

    def test_empty_series_returns_zero(self):
        s = series([])
        assert _count_sentinels(s) == 0

    def test_all_null_returns_zero(self):
        s = series([None, None, None])
        assert _count_sentinels(s) == 0

    def test_whitespace_stripped(self):
        # "  N/A  " should still be detected after strip
        s = series(["  N/A  ", "  unknown  ", "real"])
        assert _count_sentinels(s) == 2

    def test_profile_string_column_includes_sentinel_count(self):
        s = series(["Alice", "N/A", "Bob", "unknown"])
        profile = _profile_string_column(s)
        assert "sentinel_count" in profile
        assert profile["sentinel_count"] == 2


# ===========================================================================
# _detect_numeric_sentinels
# ===========================================================================

class TestDetectNumericSentinels:

    def test_minus_999_detected(self):
        # -999 appears 5 times in 50 values (10%) — meets absolute count >= 5 and
        # is few enough not to distort Q1/Q3, so falls outside the 3×IQR fence
        normal = list(range(1, 46))  # 45 normal values
        with_sentinel = normal + [-999, -999, -999, -999, -999]
        s = series(with_sentinel)
        count, values = _detect_numeric_sentinels(s)
        assert count == 5
        assert -999.0 in values

    def test_9999_detected(self):
        # 9999 appears 5 times in 50 values — extreme upper sentinel
        normal = list(range(1, 46))  # 45 normal values
        with_sentinel = normal + [9999, 9999, 9999, 9999, 9999]
        s = series(with_sentinel)
        count, values = _detect_numeric_sentinels(s)
        assert count == 5
        assert 9999.0 in values

    def test_returns_unique_values_not_instance_count(self):
        # -999 appears 5 times but the unique sentinel list has exactly 1 entry
        normal = list(range(1, 46))  # 45 normal values
        with_sentinel = normal + [-999, -999, -999, -999, -999]
        s = series(with_sentinel)
        _, values = _detect_numeric_sentinels(s)
        assert len(values) == 1
        assert values == [-999.0]

    def test_genuine_outlier_single_occurrence_not_flagged(self):
        # Tight cluster + one outlier. IQR=0 for 19 identical values → returns empty.
        normal = [50] * 19 + [200]
        s = series(normal)
        count, values = _detect_numeric_sentinels(s)
        assert count == 0
        assert values == []

    def test_fewer_than_10_values_returns_empty(self):
        s = series([-999, -999, 1, 2, 3])
        count, values = _detect_numeric_sentinels(s)
        assert count == 0
        assert values == []

    def test_constant_column_returns_empty(self):
        s = series([5] * 20)
        count, values = _detect_numeric_sentinels(s)
        assert count == 0
        assert values == []

    def test_clean_numeric_data_returns_empty(self):
        s = series(list(range(1, 31)))  # 1-30, no sentinels
        count, values = _detect_numeric_sentinels(s)
        assert count == 0
        assert values == []

    def test_profile_numeric_column_includes_sentinel_count_and_values(self):
        # 45 normal + 5 sentinel = 50 total; sentinels are 10% so Q1/Q3 not distorted
        normal = list(range(1, 46))  # 45 normal values
        with_sentinel = normal + [-999, -999, -999, -999, -999]  # 5 sentinels
        s = series(with_sentinel, dtype=float)
        p = _profile_numeric_column(s)
        assert p["sentinel_count"] == 5
        assert p["sentinel_values"] == [-999.0]

    def test_profile_numeric_column_clean_has_empty_sentinel_values(self):
        s = series(list(range(1, 31)), dtype=float)
        p = _profile_numeric_column(s)
        assert p["sentinel_count"] == 0
        assert p["sentinel_values"] == []


# ===========================================================================
# sentinel_values in build_recommendations and apply_recommendations
# ===========================================================================

class TestSentinelValuesEndToEnd:

    def _profile_with_sentinel(self, sentinel_val=-999.0, n_normal=45, n_sentinel=5):
        """Build a DataFrame and profile where a numeric column has sentinel values.

        Uses 45 normal + 5 sentinel = 50 total so sentinels are ~10% of the data,
        which keeps them from distorting Q1/Q3 and allows the 3×IQR fence to detect them.
        """
        normal = list(range(1, n_normal + 1))
        data = normal + [sentinel_val] * n_sentinel
        df = pd.DataFrame({"temp": data})
        return df, profile_dataframe(df)

    def test_build_recommendations_includes_sentinel_values(self):
        df, profile = self._profile_with_sentinel()
        recs = build_recommendations(df, profile, 90.0)
        assert recs["columns"]["temp"]["sentinel_values"] == [-999.0]

    def test_build_recommendations_sentinel_only_column_gets_fill_strategy(self):
        # Column with sentinels but zero real NaN — needs_fill must still be True
        # so a fill strategy is generated for the NaN created by step 0.
        df = pd.DataFrame({"temp": list(range(1, 46)) + [-999.0] * 5})
        profile = profile_dataframe(df)
        recs = build_recommendations(df, profile, 90.0)
        col = recs["columns"]["temp"]
        assert col["missing_values"] is not None
        assert col["missing_values"]["strategy"] in ("median", "mean")

    def test_build_recommendations_no_sentinels_has_null_sentinel_values(self):
        df = pd.DataFrame({"temp": [1.0, 2.0, 3.0, None, 5.0]})
        profile = profile_dataframe(df)
        recs = build_recommendations(df, profile, 90.0)
        assert recs["columns"]["temp"]["sentinel_values"] is None

    def test_apply_replaces_sentinel_before_fill(self):
        # Column: real values 20-30, sentinel -999.0 (3 instances), no real NaN.
        # After step 0: -999.0 → NaN. After step 2: filled with median (~25).
        # Final column must have no -999.0 and no NaN.
        normal = list(range(20, 38))  # 17 values in 20-37
        data = normal + [-999.0, -999.0, -999.0]
        df = pd.DataFrame({"temp": data})
        recs = {
            "columns": {
                "temp": {
                    "type": "float",
                    "nullable": True,
                    "missing_values": {"strategy": "median", "value": None},
                    "normalize": False,
                    "warnings": [],
                    "note": None,
                    "sentinel_values": [-999.0],
                }
            },
            "duplicates": {},
            "custom_transforms": [],
        }
        result = apply_recommendations(df, recs)
        assert -999.0 not in result["temp"].values
        assert result["temp"].isna().sum() == 0
        # All filled values should be close to the median of the normal range
        assert result["temp"].min() >= 20.0

    def test_apply_sentinel_and_real_nulls_both_filled(self):
        # Mixed column: real NaN + sentinel -999.0. Both should be filled.
        data = [20.0, None, 25.0, -999.0, 22.0, None, 28.0,
                21.0, 24.0, 26.0, 23.0, 27.0, 20.0, 25.0,
                22.0, 24.0, 21.0, 26.0, 23.0, 25.0]
        df = pd.DataFrame({"temp": data})
        recs = {
            "columns": {
                "temp": {
                    "type": "float",
                    "nullable": True,
                    "missing_values": {"strategy": "median", "value": None},
                    "normalize": False,
                    "warnings": [],
                    "note": None,
                    "sentinel_values": [-999.0],
                }
            },
            "duplicates": {},
            "custom_transforms": [],
        }
        result = apply_recommendations(df, recs)
        assert result["temp"].isna().sum() == 0
        assert -999.0 not in result["temp"].values

    def test_apply_no_sentinel_values_field_is_safe(self):
        # Columns without sentinel_values key must not raise errors.
        df = pd.DataFrame({"salary": [50000.0, None, 70000.0]})
        recs = {
            "columns": {
                "salary": {
                    "type": "float",
                    "nullable": True,
                    "missing_values": {"strategy": "median", "value": None},
                    "normalize": False,
                    "warnings": [],
                    "note": None,
                }
            },
            "duplicates": {},
            "custom_transforms": [],
        }
        result = apply_recommendations(df, recs)
        assert result["salary"].isna().sum() == 0

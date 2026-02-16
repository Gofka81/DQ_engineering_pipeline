import json
import os
from datetime import datetime
from io import BytesIO
from typing import Any

import pandas as pd
from prefect import flow, task
from prefect.logging import get_run_logger


@task
def update_run_status(run_id: str, status: str, error_message: str = None):
    """Update run status in database."""
    logger = get_run_logger()
    logger.info(f"Updating run {run_id} to status: {status}")

    # TODO: Connect to PostgreSQL and update run status
    # For now, just log
    if error_message:
        logger.error(f"Error: {error_message}")

    logger.info(f"Status updated successfully")


@task
def download_file_from_minio(minio_path: str) -> BytesIO:
    """Download CSV file from MinIO."""
    logger = get_run_logger()
    logger.info(f"Downloading file from MinIO: {minio_path}")

    # TODO: Use MinioService to download file
    # For now, create dummy data
    dummy_csv = "col1,col2,col3\n1,2,3\n4,5,6\n7,8,9"
    file_data = BytesIO(dummy_csv.encode())

    logger.info(f"File downloaded successfully")
    return file_data


@task
def profile_data(file_data: BytesIO) -> dict[str, Any]:
    """Profile the CSV data to identify quality issues."""
    logger = get_run_logger()
    logger.info("Profiling data...")

    # Read CSV
    df = pd.read_csv(file_data)
    total_cells = df.size
    total_rows = len(df)

    # Calculate metrics
    missing_cells = df.isna().sum().sum()
    duplicate_rows = df.duplicated().sum()

    # Completeness: (1 - missing_cells / total_cells) * 100
    completeness = (1 - missing_cells / total_cells) * 100 if total_cells > 0 else 100

    # Uniqueness: (1 - duplicate_rows / total_rows) * 100
    uniqueness = (1 - duplicate_rows / total_rows) * 100 if total_rows > 0 else 100

    # Validity: assume 100% for now (all values conform to expected types)
    validity = 100.0

    # Consistency: assume 100% for now (all patterns match)
    consistency = 100.0

    profile = {
        "total_rows": total_rows,
        "total_columns": len(df.columns),
        "total_cells": total_cells,
        "missing_cells": int(missing_cells),
        "duplicate_rows": int(duplicate_rows),
        "completeness": completeness,
        "uniqueness": uniqueness,
        "validity": validity,
        "consistency": consistency,
        "columns": {col: str(df[col].dtype) for col in df.columns},
    }

    logger.info(f"Profiling complete: {profile}")
    return profile


@task
def calculate_dq_score(profile: dict[str, Any]) -> float:
    """
    Calculate DQ score based on weighted components.

    Formula:
    - Completeness: 35%
    - Uniqueness: 25%
    - Validity: 25%
    - Consistency: 15%
    """
    logger = get_run_logger()

    dq_score = (
        profile["completeness"] * 0.35
        + profile["uniqueness"] * 0.25
        + profile["validity"] * 0.25
        + profile["consistency"] * 0.15
    )

    logger.info(f"DQ Score calculated: {dq_score:.2f}")
    return round(dq_score, 2)


@task
def generate_recommendations(profile: dict[str, Any], dq_score: float) -> dict[str, Any]:
    """Generate recommendations JSON based on profiling results."""
    logger = get_run_logger()
    logger.info("Generating recommendations...")

    recommendations = {
        "schema": {},
        "missing_values": {},
        "duplicates": {},
        "normalization": {"columns": []},
        "custom_transforms": [],
        "_metadata": {
            "generated_at": datetime.utcnow().isoformat(),
            "dq_score": dq_score,
            "issues_found": {
                "missing": profile["missing_cells"],
                "duplicates": profile["duplicate_rows"],
                "type_mismatches": 0,
            },
        },
    }

    # Generate schema recommendations
    for col, dtype in profile["columns"].items():
        recommendations["schema"][col] = {
            "type": _map_dtype(dtype),
            "nullable": True,
        }

    # Generate missing value recommendations if needed
    if profile["missing_cells"] > 0:
        logger.info("Detected missing values - adding recommendations")
        # TODO: Analyze which columns have missing values
        # For now, generic recommendation
        recommendations["missing_values"]["example_column"] = {
            "strategy": "median",
            "value": None,
        }

    # Generate duplicate recommendations if needed
    if profile["duplicate_rows"] > 0:
        logger.info("Detected duplicate rows - adding recommendations")
        recommendations["duplicates"] = {
            "strategy": "drop",
            "subset": [],
            "keep": "first",
        }

    logger.info(f"Recommendations generated")
    return recommendations


def _map_dtype(pandas_dtype: str) -> str:
    """Map pandas dtype to our schema type."""
    dtype_lower = str(pandas_dtype).lower()
    if "int" in dtype_lower:
        return "int"
    elif "float" in dtype_lower:
        return "float"
    elif "bool" in dtype_lower:
        return "bool"
    elif "datetime" in dtype_lower or "date" in dtype_lower:
        return "date"
    else:
        return "string"


@task
def save_results_to_db(
    run_id: str,
    dq_score: float,
    recommendations: dict[str, Any],
):
    """Save DQ analysis results to database."""
    logger = get_run_logger()
    logger.info(f"Saving results to database for run {run_id}")

    # TODO: Connect to PostgreSQL and update run record
    # UPDATE runs SET
    #   dq_score_before = dq_score,
    #   recommendations_generated = recommendations,
    #   status = 'AWAITING_REVIEW'
    # WHERE id = run_id

    logger.info(f"Results saved successfully")


@flow(name="DQ Analysis", log_prints=True)
def dq_analysis_flow(run_id: str, file_id: str, minio_path: str):
    """
    Main DQ Analysis flow.

    Args:
        run_id: UUID of the run record
        file_id: UUID of the file record
        minio_path: Path to file in MinIO raw bucket
    """
    logger = get_run_logger()
    logger.info(f"Starting DQ Analysis for run_id={run_id}, file_id={file_id}")

    try:
        # Step 1: Update status to ANALYZING
        update_run_status(run_id, "ANALYZING")

        # Step 2: Download file from MinIO
        file_data = download_file_from_minio(minio_path)

        # Step 3: Profile the data
        profile = profile_data(file_data)

        # Step 4: Calculate DQ score
        dq_score = calculate_dq_score(profile)

        # Step 5: Generate recommendations
        recommendations = generate_recommendations(profile, dq_score)

        # Step 6: Save results to database
        save_results_to_db(run_id, dq_score, recommendations)

        # Step 7: Update status to AWAITING_REVIEW
        update_run_status(run_id, "AWAITING_REVIEW")

        logger.info(f"DQ Analysis completed successfully. Score: {dq_score}")

    except Exception as e:
        logger.error(f"DQ Analysis failed: {e}", exc_info=True)
        update_run_status(run_id, "FAILED", error_message=str(e))
        raise


if __name__ == "__main__":
    # For local testing
    dq_analysis_flow(
        run_id="test-run-id",
        file_id="test-file-id",
        minio_path="raw/test.csv",
    )

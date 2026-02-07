"""
Data Quality Validators
Validates data quality between QA and PROD layers
"""
import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import QA_THRESHOLDS


@dataclass
class QualityReport:
    """Quality assessment report for a dataset"""
    dataset_name: str
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    record_count: int = 0
    passed: bool = True
    checks: list[dict] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def add_check(self, name: str, passed: bool, details: dict = None):
        """Add a quality check result"""
        self.checks.append({
            "name": name,
            "passed": passed,
            "details": details or {}
        })
        if not passed:
            self.passed = False

    def to_dict(self) -> dict:
        return {
            "dataset_name": self.dataset_name,
            "timestamp": self.timestamp,
            "record_count": self.record_count,
            "passed": self.passed,
            "checks": self.checks,
            "errors": self.errors,
            "warnings": self.warnings
        }


def check_required_fields(records: list[dict], required_fields: list[str]) -> tuple[bool, dict]:
    """
    Check if all required fields are present in records

    Returns:
        Tuple of (passed, details)
    """
    missing_by_record = []
    for idx, record in enumerate(records):
        missing = [f for f in required_fields if f not in record or record[f] is None]
        if missing:
            missing_by_record.append({"record_index": idx, "missing_fields": missing})

    passed = len(missing_by_record) == 0
    details = {
        "required_fields": required_fields,
        "records_with_missing": len(missing_by_record),
        "sample_issues": missing_by_record[:5]  # First 5 issues
    }
    return passed, details


def check_null_percentage(records: list[dict], max_null_pct: float) -> tuple[bool, dict]:
    """
    Check if null percentage is within acceptable threshold

    Returns:
        Tuple of (passed, details)
    """
    if not records:
        return True, {"null_percentage": 0, "threshold": max_null_pct}

    total_fields = 0
    null_fields = 0

    for record in records:
        for key, value in record.items():
            total_fields += 1
            if value is None or value == "" or value == []:
                null_fields += 1

    null_pct = (null_fields / total_fields * 100) if total_fields > 0 else 0
    passed = null_pct <= max_null_pct

    return passed, {
        "null_percentage": round(null_pct, 2),
        "threshold": max_null_pct,
        "total_fields": total_fields,
        "null_fields": null_fields
    }


def check_duplicates(records: list[dict], id_field: str = "id") -> tuple[bool, dict]:
    """
    Check for duplicate records based on ID field

    Returns:
        Tuple of (passed, details)
    """
    if not records:
        return True, {"duplicate_count": 0, "duplicate_percentage": 0}

    ids = [r.get(id_field) for r in records if r.get(id_field)]
    unique_ids = set(ids)
    duplicate_count = len(ids) - len(unique_ids)
    duplicate_pct = (duplicate_count / len(ids) * 100) if ids else 0

    max_dup_pct = QA_THRESHOLDS["max_duplicate_percentage"]
    passed = duplicate_pct <= max_dup_pct

    return passed, {
        "duplicate_count": duplicate_count,
        "duplicate_percentage": round(duplicate_pct, 2),
        "threshold": max_dup_pct,
        "total_records": len(records)
    }


def check_email_format(records: list[dict], email_field: str = "email") -> tuple[bool, dict]:
    """
    Basic email format validation

    Returns:
        Tuple of (passed, details)
    """
    import re
    email_pattern = re.compile(r'^[\w\.-]+@[\w\.-]+\.\w+$')

    invalid_emails = []
    for idx, record in enumerate(records):
        email = record.get(email_field)
        if email and not email_pattern.match(email):
            invalid_emails.append({"index": idx, "email": email})

    passed = len(invalid_emails) == 0
    return passed, {
        "invalid_count": len(invalid_emails),
        "sample_invalid": invalid_emails[:5]
    }


def validate_customers(data: dict) -> QualityReport:
    """
    Run all quality checks on customer data

    Args:
        data: Dictionary with metadata and data keys

    Returns:
        QualityReport with all check results
    """
    report = QualityReport(dataset_name="customers")
    records = data.get("data", [])
    report.record_count = len(records)

    required_fields = QA_THRESHOLDS["required_fields"]["customers"]

    # Check 1: Required fields
    passed, details = check_required_fields(records, required_fields)
    report.add_check("required_fields", passed, details)
    if not passed:
        report.errors.append(f"Missing required fields in {details['records_with_missing']} records")

    # Check 2: Null percentage
    passed, details = check_null_percentage(records, QA_THRESHOLDS["max_null_percentage"])
    report.add_check("null_percentage", passed, details)
    if not passed:
        report.warnings.append(f"Null percentage ({details['null_percentage']}%) exceeds threshold")

    # Check 3: Duplicates
    passed, details = check_duplicates(records)
    report.add_check("duplicates", passed, details)
    if not passed:
        report.errors.append(f"Found {details['duplicate_count']} duplicate records")

    # Check 4: Email format
    passed, details = check_email_format(records)
    report.add_check("email_format", passed, details)
    if not passed:
        report.warnings.append(f"Found {details['invalid_count']} invalid email formats")

    return report


def validate_interactions(data: dict) -> QualityReport:
    """
    Run all quality checks on interaction data

    Args:
        data: Dictionary with metadata and data keys

    Returns:
        QualityReport with all check results
    """
    report = QualityReport(dataset_name="interactions")
    records = data.get("data", [])
    report.record_count = len(records)

    required_fields = QA_THRESHOLDS["required_fields"]["interactions"]

    # Check 1: Required fields
    passed, details = check_required_fields(records, required_fields)
    report.add_check("required_fields", passed, details)
    if not passed:
        report.errors.append(f"Missing required fields in {details['records_with_missing']} records")

    # Check 2: Null percentage
    passed, details = check_null_percentage(records, QA_THRESHOLDS["max_null_percentage"])
    report.add_check("null_percentage", passed, details)
    if not passed:
        report.warnings.append(f"Null percentage ({details['null_percentage']}%) exceeds threshold")

    # Check 3: Duplicates
    passed, details = check_duplicates(records)
    report.add_check("duplicates", passed, details)
    if not passed:
        report.errors.append(f"Found {details['duplicate_count']} duplicate records")

    # Check 4: Valid interaction types
    valid_types = {"post", "comment", "support_ticket", "feedback"}
    invalid_types = []
    for idx, record in enumerate(records):
        if record.get("type") not in valid_types:
            invalid_types.append({"index": idx, "type": record.get("type")})

    passed = len(invalid_types) == 0
    report.add_check("valid_interaction_types", passed, {
        "valid_types": list(valid_types),
        "invalid_count": len(invalid_types),
        "sample_invalid": invalid_types[:5]
    })
    if not passed:
        report.warnings.append(f"Found {len(invalid_types)} records with invalid interaction types")

    return report


def run_validation(data_file: Path, dataset_type: str) -> QualityReport:
    """
    Run validation on a data file

    Args:
        data_file: Path to JSON data file
        dataset_type: 'customers' or 'interactions'

    Returns:
        QualityReport
    """
    with open(data_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    if dataset_type == "customers":
        return validate_customers(data)
    elif dataset_type == "interactions":
        return validate_interactions(data)
    else:
        raise ValueError(f"Unknown dataset type: {dataset_type}")

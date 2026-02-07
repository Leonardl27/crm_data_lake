"""
Data Quality Validators for Life Insurance Data Lake
Validates data quality between QA and PROD layers
"""
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import QA_THRESHOLDS, LIFE_INSURANCE_CONFIG


@dataclass
class QualityReport:
    """Quality assessment report for a dataset"""

    dataset_name: str
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    record_count: int = 0
    passed: bool = True
    checks: list[dict] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    def add_check(self, name: str, passed: bool, details: dict = None, critical: bool = True):
        """Add a quality check result

        Args:
            name: Check name
            passed: Whether check passed
            details: Check details
            critical: If True (default), failing this check fails the report
        """
        self.checks.append({"name": name, "passed": passed, "details": details or {}})
        if not passed and critical:
            self.passed = False

    def to_dict(self) -> dict:
        return {
            "dataset_name": self.dataset_name,
            "timestamp": self.timestamp,
            "record_count": self.record_count,
            "passed": self.passed,
            "checks": self.checks,
            "errors": self.errors,
            "warnings": self.warnings,
        }


def check_required_fields(
    records: list[dict], required_fields: list[str]
) -> tuple[bool, dict]:
    """Check if all required fields are present in records"""
    missing_by_record = []
    for idx, record in enumerate(records):
        missing = [f for f in required_fields if f not in record or record[f] is None]
        if missing:
            missing_by_record.append({"record_index": idx, "missing_fields": missing})

    passed = len(missing_by_record) == 0
    details = {
        "required_fields": required_fields,
        "records_with_missing": len(missing_by_record),
        "sample_issues": missing_by_record[:5],
    }
    return passed, details


def check_null_percentage(
    records: list[dict], max_null_pct: float
) -> tuple[bool, dict]:
    """Check if null percentage is within acceptable threshold"""
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
        "null_fields": null_fields,
    }


def check_duplicates(
    records: list[dict], id_field: str = "id"
) -> tuple[bool, dict]:
    """Check for duplicate records based on ID field"""
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
        "total_records": len(records),
    }


def check_foreign_key_integrity(
    records: list[dict],
    fk_field: str,
    parent_records: list[dict],
    parent_id_field: str,
) -> tuple[bool, dict]:
    """Check foreign key integrity between child and parent records"""
    parent_ids = {r.get(parent_id_field) for r in parent_records}
    orphaned = []

    for idx, record in enumerate(records):
        fk_value = record.get(fk_field)
        if fk_value and fk_value not in parent_ids:
            orphaned.append({"index": idx, "fk_value": fk_value})

    passed = len(orphaned) == 0
    return passed, {
        "orphaned_count": len(orphaned),
        "sample_orphaned": orphaned[:5],
        "fk_field": fk_field,
        "parent_id_field": parent_id_field,
    }


def check_date_sequence(
    records: list[dict], date_fields: list[str]
) -> tuple[bool, dict]:
    """Check that dates are in proper chronological order"""
    invalid_sequences = []

    for idx, record in enumerate(records):
        dates = []
        for field_name in date_fields:
            date_val = record.get(field_name)
            if date_val:
                try:
                    parsed = datetime.fromisoformat(
                        date_val.replace("Z", "+00:00")
                    )
                    dates.append((field_name, parsed))
                except (ValueError, AttributeError):
                    pass

        for i in range(len(dates) - 1):
            if dates[i][1] > dates[i + 1][1]:
                invalid_sequences.append(
                    {"index": idx, "field1": dates[i][0], "field2": dates[i + 1][0]}
                )
                break

    passed = len(invalid_sequences) == 0
    return passed, {
        "invalid_count": len(invalid_sequences),
        "sample_invalid": invalid_sequences[:5],
        "date_fields_checked": date_fields,
    }


def check_numeric_range(
    records: list[dict], field_name: str, min_val: float, max_val: float
) -> tuple[bool, dict]:
    """Check that numeric values are within expected range"""
    out_of_range = []

    for idx, record in enumerate(records):
        value = record.get(field_name)
        if value is not None:
            try:
                num_val = float(value)
                if num_val < min_val or num_val > max_val:
                    out_of_range.append({"index": idx, "value": num_val})
            except (ValueError, TypeError):
                out_of_range.append({"index": idx, "value": value, "error": "not_numeric"})

    passed = len(out_of_range) == 0
    return passed, {
        "out_of_range_count": len(out_of_range),
        "sample_invalid": out_of_range[:5],
        "field": field_name,
        "min": min_val,
        "max": max_val,
    }


def check_enum_values(
    records: list[dict], field_name: str, valid_values: list[str]
) -> tuple[bool, dict]:
    """Check that field values are from allowed enumeration"""
    invalid_values = []

    for idx, record in enumerate(records):
        value = record.get(field_name)
        if value is not None and value not in valid_values:
            invalid_values.append({"index": idx, "value": value})

    passed = len(invalid_values) == 0
    return passed, {
        "invalid_count": len(invalid_values),
        "sample_invalid": invalid_values[:5],
        "field": field_name,
        "valid_values": valid_values,
    }


def check_email_format(
    records: list[dict], email_field: str = "Email__c"
) -> tuple[bool, dict]:
    """Basic email format validation"""
    import re

    email_pattern = re.compile(r"^[\w\.-]+@[\w\.-]+\.\w+$")

    invalid_emails = []
    for idx, record in enumerate(records):
        email = record.get(email_field)
        if email and not email_pattern.match(email):
            invalid_emails.append({"index": idx, "email": email})

    passed = len(invalid_emails) == 0
    return passed, {"invalid_count": len(invalid_emails), "sample_invalid": invalid_emails[:5]}


def validate_customers(data: dict) -> QualityReport:
    """Run quality checks on customer data"""
    report = QualityReport(dataset_name="customers")
    records = data.get("data", [])
    report.record_count = len(records)

    required_fields = QA_THRESHOLDS["required_fields"]["customers"]

    # Check 1: Required fields
    passed, details = check_required_fields(records, required_fields)
    report.add_check("required_fields", passed, details)
    if not passed:
        report.errors.append(
            f"Missing required fields in {details['records_with_missing']} records"
        )

    # Check 2: Null percentage (warning only)
    passed, details = check_null_percentage(
        records, QA_THRESHOLDS["max_null_percentage"]
    )
    report.add_check("null_percentage", passed, details, critical=False)
    if not passed:
        report.warnings.append(
            f"Null percentage ({details['null_percentage']}%) exceeds threshold"
        )

    # Check 3: Duplicates
    passed, details = check_duplicates(records, id_field="Customer_ID__c")
    report.add_check("duplicates", passed, details)
    if not passed:
        report.errors.append(f"Found {details['duplicate_count']} duplicate records")

    # Check 4: Email format (warning only)
    passed, details = check_email_format(records, "Email__c")
    report.add_check("email_format", passed, details, critical=False)
    if not passed:
        report.warnings.append(
            f"Found {details['invalid_count']} invalid email formats"
        )

    return report


def validate_agents(data: dict) -> QualityReport:
    """Run quality checks on agent data"""
    report = QualityReport(dataset_name="agents")
    records = data.get("data", [])
    report.record_count = len(records)

    required_fields = QA_THRESHOLDS["required_fields"]["agents"]

    # Check 1: Required fields
    passed, details = check_required_fields(records, required_fields)
    report.add_check("required_fields", passed, details)
    if not passed:
        report.errors.append(
            f"Missing required fields in {details['records_with_missing']} records"
        )

    # Check 2: Duplicates
    passed, details = check_duplicates(records, id_field="Agent_ID__c")
    report.add_check("duplicates", passed, details)
    if not passed:
        report.errors.append(f"Found {details['duplicate_count']} duplicate records")

    return report


def validate_quotes(data: dict) -> QualityReport:
    """Run quality checks on quote data"""
    report = QualityReport(dataset_name="quotes")
    records = data.get("data", [])
    report.record_count = len(records)

    required_fields = QA_THRESHOLDS["required_fields"]["quotes"]

    # Check 1: Required fields
    passed, details = check_required_fields(records, required_fields)
    report.add_check("required_fields", passed, details)
    if not passed:
        report.errors.append(
            f"Missing required fields in {details['records_with_missing']} records"
        )

    # Check 2: Null percentage (warning only)
    passed, details = check_null_percentage(
        records, QA_THRESHOLDS["max_null_percentage"]
    )
    report.add_check("null_percentage", passed, details, critical=False)
    if not passed:
        report.warnings.append(
            f"Null percentage ({details['null_percentage']}%) exceeds threshold"
        )

    # Check 3: Duplicates
    passed, details = check_duplicates(records, id_field="Quote_ID__c")
    report.add_check("duplicates", passed, details)
    if not passed:
        report.errors.append(f"Found {details['duplicate_count']} duplicate records")

    # Check 4: Valid product types (warning only)
    passed, details = check_enum_values(
        records, "Product_Type__c", LIFE_INSURANCE_CONFIG["product_types"]
    )
    report.add_check("valid_product_types", passed, details, critical=False)
    if not passed:
        report.warnings.append(
            f"Found {details['invalid_count']} records with invalid product types"
        )

    # Check 5: Coverage amount range (warning only)
    passed, details = check_numeric_range(
        records,
        "Coverage_Amount__c",
        LIFE_INSURANCE_CONFIG["coverage_ranges"]["min"],
        LIFE_INSURANCE_CONFIG["coverage_ranges"]["max"],
    )
    report.add_check("coverage_range", passed, details, critical=False)
    if not passed:
        report.warnings.append(
            f"Found {details['out_of_range_count']} records with out-of-range coverage"
        )

    # Check 6: Date sequence (Created < Expiry) (warning only)
    passed, details = check_date_sequence(
        records, ["Created_Date__c", "Expiry_Date__c"]
    )
    report.add_check("date_sequence", passed, details, critical=False)
    if not passed:
        report.warnings.append(
            f"Found {details['invalid_count']} records with invalid date sequences"
        )

    return report


def validate_applications(
    data: dict, quotes_data: dict = None
) -> QualityReport:
    """Run quality checks on application data"""
    report = QualityReport(dataset_name="applications")
    records = data.get("data", [])
    report.record_count = len(records)

    required_fields = QA_THRESHOLDS["required_fields"]["applications"]

    # Check 1: Required fields
    passed, details = check_required_fields(records, required_fields)
    report.add_check("required_fields", passed, details)
    if not passed:
        report.errors.append(
            f"Missing required fields in {details['records_with_missing']} records"
        )

    # Check 2: Duplicates
    passed, details = check_duplicates(records, id_field="Application_ID__c")
    report.add_check("duplicates", passed, details)
    if not passed:
        report.errors.append(f"Found {details['duplicate_count']} duplicate records")

    # Check 3: Valid underwriting statuses (warning only)
    passed, details = check_enum_values(
        records,
        "Underwriting_Status__c",
        LIFE_INSURANCE_CONFIG["underwriting_statuses"],
    )
    report.add_check("valid_underwriting_status", passed, details, critical=False)
    if not passed:
        report.warnings.append(
            f"Found {details['invalid_count']} records with invalid underwriting status"
        )

    # Check 4: Valid health classes (warning only)
    passed, details = check_enum_values(
        records, "Health_Class__c", LIFE_INSURANCE_CONFIG["health_classes"]
    )
    report.add_check("valid_health_class", passed, details, critical=False)
    if not passed:
        report.warnings.append(
            f"Found {details['invalid_count']} records with invalid health class"
        )

    # Check 5: Foreign key integrity (Quote_ID)
    if quotes_data:
        passed, details = check_foreign_key_integrity(
            records, "Quote_ID__c", quotes_data.get("data", []), "Quote_ID__c"
        )
        report.add_check("quote_fk_integrity", passed, details)
        if not passed:
            report.errors.append(
                f"Found {details['orphaned_count']} orphaned application records"
            )

    # Check 6: Risk score range (warning only)
    passed, details = check_numeric_range(records, "Risk_Score__c", 1, 100)
    report.add_check("risk_score_range", passed, details, critical=False)
    if not passed:
        report.warnings.append(
            f"Found {details['out_of_range_count']} records with invalid risk scores"
        )

    return report


def validate_policies(
    data: dict, applications_data: dict = None
) -> QualityReport:
    """Run quality checks on policy data"""
    report = QualityReport(dataset_name="policies")
    records = data.get("data", [])
    report.record_count = len(records)

    required_fields = QA_THRESHOLDS["required_fields"]["policies"]

    # Check 1: Required fields
    passed, details = check_required_fields(records, required_fields)
    report.add_check("required_fields", passed, details)
    if not passed:
        report.errors.append(
            f"Missing required fields in {details['records_with_missing']} records"
        )

    # Check 2: Duplicates on Policy_ID
    passed, details = check_duplicates(records, id_field="Policy_ID__c")
    report.add_check("duplicates_id", passed, details)
    if not passed:
        report.errors.append(f"Found {details['duplicate_count']} duplicate policy IDs")

    # Check 3: Duplicates on Policy_Number
    passed, details = check_duplicates(records, id_field="Policy_Number__c")
    report.add_check("duplicates_number", passed, details)
    if not passed:
        report.errors.append(
            f"Found {details['duplicate_count']} duplicate policy numbers"
        )

    # Check 4: Valid statuses (warning only)
    passed, details = check_enum_values(
        records, "Status__c", LIFE_INSURANCE_CONFIG["policy_statuses"]
    )
    report.add_check("valid_status", passed, details, critical=False)
    if not passed:
        report.warnings.append(
            f"Found {details['invalid_count']} records with invalid status"
        )

    # Check 5: Valid payment frequencies (warning only)
    passed, details = check_enum_values(
        records,
        "Payment_Frequency__c",
        LIFE_INSURANCE_CONFIG["payment_frequencies"],
    )
    report.add_check("valid_payment_frequency", passed, details, critical=False)
    if not passed:
        report.warnings.append(
            f"Found {details['invalid_count']} records with invalid payment frequency"
        )

    # Check 6: Foreign key integrity (Application_ID)
    if applications_data:
        passed, details = check_foreign_key_integrity(
            records,
            "Application_ID__c",
            applications_data.get("data", []),
            "Application_ID__c",
        )
        report.add_check("application_fk_integrity", passed, details)
        if not passed:
            report.errors.append(
                f"Found {details['orphaned_count']} orphaned policy records"
            )

    # Check 7: Date sequence (Effective < Expiry) (warning only)
    passed, details = check_date_sequence(
        records, ["Effective_Date__c", "Expiry_Date__c"]
    )
    report.add_check("date_sequence", passed, details, critical=False)
    if not passed:
        report.warnings.append(
            f"Found {details['invalid_count']} records with invalid date sequences"
        )

    return report


def validate_claims(data: dict, policies_data: dict = None) -> QualityReport:
    """Run quality checks on claims data"""
    report = QualityReport(dataset_name="claims")
    records = data.get("data", [])
    report.record_count = len(records)

    required_fields = QA_THRESHOLDS["required_fields"]["claims"]

    # Check 1: Required fields
    passed, details = check_required_fields(records, required_fields)
    report.add_check("required_fields", passed, details)
    if not passed:
        report.errors.append(
            f"Missing required fields in {details['records_with_missing']} records"
        )

    # Check 2: Duplicates
    passed, details = check_duplicates(records, id_field="Claim_ID__c")
    report.add_check("duplicates", passed, details)
    if not passed:
        report.errors.append(f"Found {details['duplicate_count']} duplicate claims")

    # Check 3: Valid claim types (warning only)
    passed, details = check_enum_values(
        records, "Claim_Type__c", LIFE_INSURANCE_CONFIG["claim_types"]
    )
    report.add_check("valid_claim_type", passed, details, critical=False)
    if not passed:
        report.warnings.append(
            f"Found {details['invalid_count']} records with invalid claim type"
        )

    # Check 4: Valid statuses (warning only)
    passed, details = check_enum_values(
        records, "Status__c", LIFE_INSURANCE_CONFIG["claim_statuses"]
    )
    report.add_check("valid_status", passed, details, critical=False)
    if not passed:
        report.warnings.append(
            f"Found {details['invalid_count']} records with invalid status"
        )

    # Check 5: Foreign key integrity (Policy_ID)
    if policies_data:
        passed, details = check_foreign_key_integrity(
            records, "Policy_ID__c", policies_data.get("data", []), "Policy_ID__c"
        )
        report.add_check("policy_fk_integrity", passed, details)
        if not passed:
            report.errors.append(
                f"Found {details['orphaned_count']} orphaned claim records"
            )

    # Check 6: Claim amount positive (warning only)
    passed, details = check_numeric_range(records, "Claim_Amount__c", 0, float("inf"))
    report.add_check("claim_amount_positive", passed, details, critical=False)
    if not passed:
        report.warnings.append(
            f"Found {details['out_of_range_count']} records with invalid claim amounts"
        )

    # Check 7: Date sequence (Filed < Processed) (warning only)
    passed, details = check_date_sequence(records, ["Filed_Date__c", "Processed_Date__c"])
    report.add_check("date_sequence", passed, details, critical=False)
    if not passed:
        report.warnings.append(
            f"Found {details['invalid_count']} records with invalid date sequences"
        )

    return report


def run_validation(data_file: Path, dataset_type: str) -> QualityReport:
    """Run validation on a data file"""
    with open(data_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    validators = {
        "customers": validate_customers,
        "agents": validate_agents,
        "quotes": validate_quotes,
        "applications": validate_applications,
        "policies": validate_policies,
        "claims": validate_claims,
    }

    validator = validators.get(dataset_type)
    if validator:
        return validator(data)
    else:
        raise ValueError(f"Unknown dataset type: {dataset_type}")

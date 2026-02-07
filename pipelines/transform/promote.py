"""
Data Promotion Pipeline for Life Insurance Data Lake
Promotes validated data from QA to PROD layer
"""
import json
from datetime import datetime, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import QA_DIR, PROD_DIR, LIFE_INSURANCE_ENTITIES
from pipelines.quality_checks.validators import (
    QualityReport,
    validate_customers,
    validate_agents,
    validate_quotes,
    validate_applications,
    validate_policies,
    validate_claims,
)


def get_latest_qa_file(dataset_type: str) -> Path | None:
    """Get the most recent file from QA layer"""
    qa_path = QA_DIR / dataset_type
    if not qa_path.exists():
        return None

    files = sorted(qa_path.glob("*.json"), key=lambda x: x.stat().st_mtime, reverse=True)
    return files[0] if files else None


def get_latest_prod_file(dataset_type: str) -> Path | None:
    """Get the latest PROD file for a dataset type"""
    prod_file = PROD_DIR / dataset_type / f"{dataset_type}_latest.json"
    return prod_file if prod_file.exists() else None


def load_prod_data(dataset_type: str) -> dict | None:
    """Load data from PROD layer for FK validation"""
    prod_file = get_latest_prod_file(dataset_type)
    if prod_file:
        with open(prod_file, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def clean_data(data: dict, dataset_type: str) -> dict:
    """Clean and transform data for PROD layer"""
    records = data.get("data", [])
    cleaned_records = []

    id_fields = {
        "customers": "Customer_ID__c",
        "agents": "Agent_ID__c",
        "quotes": "Quote_ID__c",
        "applications": "Application_ID__c",
        "policies": "Policy_ID__c",
        "claims": "Claim_ID__c",
    }

    for record in records:
        cleaned = {k: v for k, v in record.items() if v is not None and v != ""}

        # Standardize string fields
        for key, value in cleaned.items():
            if isinstance(value, str):
                cleaned[key] = value.strip()

        cleaned_records.append(cleaned)

    # Sort by ID
    id_field = id_fields.get(dataset_type, "id")
    cleaned_records.sort(key=lambda x: x.get(id_field, ""))

    return {
        "metadata": {
            "source": data["metadata"]["source"],
            "qa_extracted_at": data["metadata"]["extracted_at"],
            "promoted_at": datetime.now(timezone.utc).isoformat(),
            "record_count": len(cleaned_records),
            "layer": "PROD",
        },
        "data": cleaned_records,
    }


def promote_to_prod(dataset_type: str, force: bool = False) -> dict:
    """
    Promote data from QA to PROD layer with validation

    Args:
        dataset_type: Entity type to promote
        force: Skip validation if True

    Returns:
        Dictionary with promotion results
    """
    result = {
        "dataset_type": dataset_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "success": False,
        "qa_file": None,
        "prod_file": None,
        "validation_report": None,
    }

    # Get latest QA file
    qa_file = get_latest_qa_file(dataset_type)
    if not qa_file:
        result["error"] = f"No QA files found for {dataset_type}"
        return result

    result["qa_file"] = str(qa_file)
    print(f"Processing QA file: {qa_file}")

    # Load QA data
    with open(qa_file, "r", encoding="utf-8") as f:
        qa_data = json.load(f)

    # Run validation unless forced
    if not force:
        print(f"Running quality checks on {dataset_type}...")

        # Load parent data for FK validation
        parent_data = {}
        if dataset_type == "quotes":
            parent_data["customers"] = load_prod_data("customers")
        elif dataset_type == "applications":
            parent_data["quotes"] = load_prod_data("quotes")
        elif dataset_type == "policies":
            parent_data["applications"] = load_prod_data("applications")
        elif dataset_type == "claims":
            parent_data["policies"] = load_prod_data("policies")

        # Run appropriate validator
        validators = {
            "customers": lambda d: validate_customers(d),
            "agents": lambda d: validate_agents(d),
            "quotes": lambda d: validate_quotes(d),
            "applications": lambda d: validate_applications(d, parent_data.get("quotes")),
            "policies": lambda d: validate_policies(d, parent_data.get("applications")),
            "claims": lambda d: validate_claims(d, parent_data.get("policies")),
        }

        validator = validators.get(dataset_type)
        if validator:
            report = validator(qa_data)
            result["validation_report"] = report.to_dict()

            if not report.passed:
                result["error"] = "Validation failed"
                print(f"Validation FAILED: {report.errors}")
                return result

            print(f"Validation PASSED with {len(report.warnings)} warnings")
        else:
            print(f"No validator found for {dataset_type}, skipping validation")

    # Clean and promote
    prod_data = clean_data(qa_data, dataset_type)

    # Save to PROD
    prod_dir = PROD_DIR / dataset_type
    prod_dir.mkdir(parents=True, exist_ok=True)
    prod_file = prod_dir / f"{dataset_type}_latest.json"

    with open(prod_file, "w", encoding="utf-8") as f:
        json.dump(prod_data, f, indent=2)

    result["prod_file"] = str(prod_file)
    result["success"] = True
    result["record_count"] = prod_data["metadata"]["record_count"]

    print(f"Successfully promoted {result['record_count']} records to PROD: {prod_file}")
    return result


def promote_all(force: bool = False) -> list[dict]:
    """
    Promote all life insurance entities from QA to PROD
    Entities are promoted in dependency order
    """
    results = []

    for dataset_type in LIFE_INSURANCE_ENTITIES:
        print(f"\n{'='*50}")
        print(f"Promoting {dataset_type}...")
        print(f"{'='*50}")
        result = promote_to_prod(dataset_type, force=force)
        results.append(result)

        # Stop if a critical entity fails (maintains referential integrity)
        if not result["success"] and dataset_type in ["customers", "quotes"]:
            print(f"\nCritical entity {dataset_type} failed. Stopping promotion.")
            break

    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Promote data from QA to PROD")
    parser.add_argument("--force", action="store_true", help="Skip validation")
    parser.add_argument(
        "--type",
        choices=LIFE_INSURANCE_ENTITIES + ["all"],
        default="all",
        help="Entity type to promote",
    )
    args = parser.parse_args()

    if args.type == "all":
        results = promote_all(force=args.force)
    else:
        results = [promote_to_prod(args.type, force=args.force)]

    # Print summary
    print("\n" + "=" * 50)
    print("PROMOTION SUMMARY")
    print("=" * 50)
    for r in results:
        status = "SUCCESS" if r["success"] else "FAILED"
        print(f"{r['dataset_type']}: {status}")
        if r.get("error"):
            print(f"  Error: {r['error']}")
        if r.get("record_count"):
            print(f"  Records: {r['record_count']}")

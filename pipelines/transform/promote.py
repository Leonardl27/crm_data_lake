"""
Data Promotion Pipeline
Promotes validated data from QA to PROD layer
"""
import json
import shutil
from datetime import datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import QA_DIR, PROD_DIR
from pipelines.quality_checks.validators import run_validation, QualityReport


def get_latest_qa_file(dataset_type: str) -> Path | None:
    """Get the most recent file from QA layer"""
    qa_path = QA_DIR / dataset_type
    if not qa_path.exists():
        return None

    files = sorted(qa_path.glob("*.json"), key=lambda x: x.stat().st_mtime, reverse=True)
    return files[0] if files else None


def clean_data(data: dict, dataset_type: str) -> dict:
    """
    Clean and transform data for PROD layer

    Args:
        data: Raw data dictionary
        dataset_type: Type of dataset

    Returns:
        Cleaned data dictionary
    """
    records = data.get("data", [])
    cleaned_records = []

    for record in records:
        cleaned = {k: v for k, v in record.items() if v is not None and v != ""}

        # Remove extraction metadata from individual records
        cleaned.pop("extracted_at", None)

        # Standardize string fields
        for key, value in cleaned.items():
            if isinstance(value, str):
                cleaned[key] = value.strip()

        cleaned_records.append(cleaned)

    # Sort by ID
    cleaned_records.sort(key=lambda x: x.get("id", ""))

    return {
        "metadata": {
            "source": data["metadata"]["source"],
            "qa_extracted_at": data["metadata"]["extracted_at"],
            "promoted_at": datetime.utcnow().isoformat(),
            "record_count": len(cleaned_records),
            "layer": "PROD"
        },
        "data": cleaned_records
    }


def promote_to_prod(dataset_type: str, force: bool = False) -> dict:
    """
    Promote data from QA to PROD layer

    Args:
        dataset_type: 'customers' or 'interactions'
        force: Skip validation if True

    Returns:
        Dictionary with promotion results
    """
    result = {
        "dataset_type": dataset_type,
        "timestamp": datetime.utcnow().isoformat(),
        "success": False,
        "qa_file": None,
        "prod_file": None,
        "validation_report": None
    }

    # Get latest QA file
    qa_file = get_latest_qa_file(dataset_type)
    if not qa_file:
        result["error"] = f"No QA files found for {dataset_type}"
        return result

    result["qa_file"] = str(qa_file)
    print(f"Processing QA file: {qa_file}")

    # Run validation
    if not force:
        print(f"Running quality checks on {dataset_type}...")
        report = run_validation(qa_file, dataset_type)
        result["validation_report"] = report.to_dict()

        if not report.passed:
            result["error"] = "Validation failed"
            print(f"Validation FAILED: {report.errors}")
            return result

        print(f"Validation PASSED with {len(report.warnings)} warnings")

    # Load and clean data
    with open(qa_file, "r", encoding="utf-8") as f:
        qa_data = json.load(f)

    prod_data = clean_data(qa_data, dataset_type)

    # Save to PROD
    prod_dir = PROD_DIR / dataset_type
    prod_dir.mkdir(parents=True, exist_ok=True)

    # Use a consistent filename for latest data (for easy consumption by frontend)
    prod_file = prod_dir / f"{dataset_type}_latest.json"

    with open(prod_file, "w", encoding="utf-8") as f:
        json.dump(prod_data, f, indent=2)

    result["prod_file"] = str(prod_file)
    result["success"] = True
    result["record_count"] = prod_data["metadata"]["record_count"]

    print(f"Successfully promoted {result['record_count']} records to PROD: {prod_file}")

    return result


def promote_all(force: bool = False) -> list[dict]:
    """Promote all dataset types from QA to PROD"""
    results = []
    for dataset_type in ["customers", "interactions"]:
        print(f"\n{'='*50}")
        print(f"Promoting {dataset_type}...")
        print(f"{'='*50}")
        result = promote_to_prod(dataset_type, force=force)
        results.append(result)
    return results


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Promote data from QA to PROD")
    parser.add_argument("--force", action="store_true", help="Skip validation")
    parser.add_argument("--type", choices=["customers", "interactions", "all"], default="all")
    args = parser.parse_args()

    if args.type == "all":
        results = promote_all(force=args.force)
    else:
        results = [promote_to_prod(args.type, force=args.force)]

    # Print summary
    print("\n" + "="*50)
    print("PROMOTION SUMMARY")
    print("="*50)
    for r in results:
        status = "SUCCESS" if r["success"] else "FAILED"
        print(f"{r['dataset_type']}: {status}")
        if r.get("error"):
            print(f"  Error: {r['error']}")
        if r.get("record_count"):
            print(f"  Records: {r['record_count']}")

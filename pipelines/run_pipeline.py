"""
Main Pipeline Runner
Orchestrates the full ETL pipeline: Extract -> QA -> Validate -> PROD
"""
import json
from datetime import datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipelines.extract.customers import run_extraction as extract_customers
from pipelines.extract.interactions import run_extraction as extract_interactions
from pipelines.transform.promote import promote_all
from config.settings import BASE_DIR


def run_full_pipeline(num_customers: int = 50) -> dict:
    """
    Run the complete ETL pipeline

    Args:
        num_customers: Number of customers to generate

    Returns:
        Pipeline execution results
    """
    results = {
        "started_at": datetime.utcnow().isoformat(),
        "extraction": {},
        "promotion": [],
        "success": True
    }

    print("\n" + "="*60)
    print("CRM DATA LAKE - FULL PIPELINE EXECUTION")
    print("="*60)

    # Step 1: Extract customers
    print("\n[STEP 1/4] Extracting customer data...")
    try:
        customers_file = extract_customers(num_customers)
        results["extraction"]["customers"] = {
            "success": True,
            "file": str(customers_file)
        }
    except Exception as e:
        results["extraction"]["customers"] = {"success": False, "error": str(e)}
        results["success"] = False

    # Step 2: Extract interactions
    print("\n[STEP 2/4] Extracting interaction data...")
    try:
        interactions_file = extract_interactions()
        results["extraction"]["interactions"] = {
            "success": True,
            "file": str(interactions_file)
        }
    except Exception as e:
        results["extraction"]["interactions"] = {"success": False, "error": str(e)}
        results["success"] = False

    # Step 3 & 4: Validate and promote to PROD
    print("\n[STEP 3/4] Validating data quality...")
    print("[STEP 4/4] Promoting to PROD layer...")
    try:
        promotion_results = promote_all()
        results["promotion"] = promotion_results
        for pr in promotion_results:
            if not pr["success"]:
                results["success"] = False
    except Exception as e:
        results["promotion"] = [{"success": False, "error": str(e)}]
        results["success"] = False

    results["completed_at"] = datetime.utcnow().isoformat()

    # Generate dashboard data
    print("\n[BONUS] Generating dashboard data...")
    generate_dashboard_data()

    # Print final summary
    print("\n" + "="*60)
    print("PIPELINE EXECUTION COMPLETE")
    print("="*60)
    print(f"Status: {'SUCCESS' if results['success'] else 'FAILED'}")
    print(f"Started: {results['started_at']}")
    print(f"Completed: {results['completed_at']}")

    return results


def generate_dashboard_data():
    """Generate aggregated data for the dashboard"""
    from config.settings import PROD_DIR

    dashboard_data = {
        "generated_at": datetime.utcnow().isoformat(),
        "customers": None,
        "interactions": None
    }

    # Load customers data
    customers_file = PROD_DIR / "customers" / "customers_latest.json"
    if customers_file.exists():
        with open(customers_file, "r") as f:
            cust_data = json.load(f)

        customers = cust_data["data"]
        dashboard_data["customers"] = {
            "total_count": len(customers),
            "by_nationality": {},
            "by_gender": {},
            "age_distribution": {"18-25": 0, "26-35": 0, "36-45": 0, "46-55": 0, "56+": 0}
        }

        for c in customers:
            # By nationality
            nat = c.get("nationality", "Unknown")
            dashboard_data["customers"]["by_nationality"][nat] = \
                dashboard_data["customers"]["by_nationality"].get(nat, 0) + 1

            # By gender
            gender = c.get("gender", "Unknown")
            dashboard_data["customers"]["by_gender"][gender] = \
                dashboard_data["customers"]["by_gender"].get(gender, 0) + 1

            # Age distribution
            age = c.get("age", 0)
            if age <= 25:
                dashboard_data["customers"]["age_distribution"]["18-25"] += 1
            elif age <= 35:
                dashboard_data["customers"]["age_distribution"]["26-35"] += 1
            elif age <= 45:
                dashboard_data["customers"]["age_distribution"]["36-45"] += 1
            elif age <= 55:
                dashboard_data["customers"]["age_distribution"]["46-55"] += 1
            else:
                dashboard_data["customers"]["age_distribution"]["56+"] += 1

    # Load interactions data
    interactions_file = PROD_DIR / "interactions" / "interactions_latest.json"
    if interactions_file.exists():
        with open(interactions_file, "r") as f:
            int_data = json.load(f)

        interactions = int_data["data"]
        dashboard_data["interactions"] = {
            "total_count": len(interactions),
            "by_type": {},
            "by_sentiment": {},
            "by_channel": {}
        }

        for i in interactions:
            for key in ["type", "sentiment", "channel"]:
                value = i.get(key, "Unknown")
                bucket = f"by_{key}"
                dashboard_data["interactions"][bucket][value] = \
                    dashboard_data["interactions"][bucket].get(value, 0) + 1

    # Save dashboard data
    output_file = BASE_DIR / "docs" / "assets" / "data" / "dashboard_data.json"
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w") as f:
        json.dump(dashboard_data, f, indent=2)

    print(f"Dashboard data saved to: {output_file}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Run CRM Data Lake Pipeline")
    parser.add_argument("--customers", type=int, default=50, help="Number of customers to generate")
    args = parser.parse_args()

    run_full_pipeline(args.customers)

"""
Main Pipeline Runner for Life Insurance Data Lake
Orchestrates the full ETL pipeline: Generate -> QA -> Validate -> PROD -> Dashboard
"""
import json
from datetime import datetime, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from pipelines.extract.life_insurance_generator import run_extraction as extract_life_insurance
from pipelines.transform.promote import promote_all
from config.settings import BASE_DIR, PROD_DIR


def run_full_pipeline(num_customers: int = 100) -> dict:
    """
    Run the complete Life Insurance ETL pipeline

    Args:
        num_customers: Number of customers to generate

    Returns:
        Pipeline execution results
    """
    results = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "extraction": {},
        "promotion": [],
        "success": True,
    }

    print("\n" + "=" * 60)
    print("LIFE INSURANCE DATA LAKE - FULL PIPELINE EXECUTION")
    print("=" * 60)

    # Step 1: Generate life insurance data
    print("\n[STEP 1/3] Generating life insurance data...")
    try:
        extraction_files = extract_life_insurance(num_customers)
        results["extraction"] = {
            "success": True,
            "files": {k: str(v) for k, v in extraction_files.items()},
        }
    except Exception as e:
        results["extraction"] = {"success": False, "error": str(e)}
        results["success"] = False
        print(f"Extraction FAILED: {e}")
        return results

    # Step 2: Validate and promote to PROD
    print("\n[STEP 2/3] Validating and promoting to PROD layer...")
    try:
        promotion_results = promote_all()
        results["promotion"] = promotion_results
        for pr in promotion_results:
            if not pr["success"]:
                results["success"] = False
    except Exception as e:
        results["promotion"] = [{"success": False, "error": str(e)}]
        results["success"] = False
        print(f"Promotion FAILED: {e}")

    # Step 3: Generate dashboard data
    print("\n[STEP 3/3] Generating dashboard data...")
    try:
        generate_dashboard_data()
    except Exception as e:
        print(f"Dashboard data generation failed: {e}")
        results["dashboard_error"] = str(e)

    results["completed_at"] = datetime.now(timezone.utc).isoformat()

    # Print final summary
    print("\n" + "=" * 60)
    print("PIPELINE EXECUTION COMPLETE")
    print("=" * 60)
    print(f"Status: {'SUCCESS' if results['success'] else 'FAILED'}")
    print(f"Started: {results['started_at']}")
    print(f"Completed: {results['completed_at']}")

    return results


def generate_dashboard_data():
    """Generate aggregated data for the life insurance dashboard"""
    dashboard_data = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": {},
        "customers": None,
        "quotes": None,
        "applications": None,
        "policies": None,
        "claims": None,
        "funnel": None,
    }

    # Load all entities
    entities = {}
    entity_types = ["customers", "agents", "quotes", "applications", "policies", "claims"]

    for entity in entity_types:
        file_path = PROD_DIR / entity / f"{entity}_latest.json"
        if file_path.exists():
            with open(file_path, "r") as f:
                entities[entity] = json.load(f).get("data", [])

    # Summary counts
    dashboard_data["summary"] = {
        "total_customers": len(entities.get("customers", [])),
        "total_agents": len(entities.get("agents", [])),
        "total_quotes": len(entities.get("quotes", [])),
        "total_applications": len(entities.get("applications", [])),
        "total_policies": len(entities.get("policies", [])),
        "total_claims": len(entities.get("claims", [])),
    }

    # Customers analytics
    if "customers" in entities:
        customers = entities["customers"]
        dashboard_data["customers"] = {
            "total_count": len(customers),
            "by_gender": {},
            "by_employment": {},
            "smoker_count": 0,
            "age_distribution": {"18-30": 0, "31-40": 0, "41-50": 0, "51-60": 0, "61+": 0},
            "avg_income": 0,
        }

        total_income = 0
        for c in customers:
            gender = c.get("Gender__c", "Unknown")
            dashboard_data["customers"]["by_gender"][gender] = (
                dashboard_data["customers"]["by_gender"].get(gender, 0) + 1
            )

            emp = c.get("Employment_Status__c", "Unknown")
            dashboard_data["customers"]["by_employment"][emp] = (
                dashboard_data["customers"]["by_employment"].get(emp, 0) + 1
            )

            if c.get("Smoker__c"):
                dashboard_data["customers"]["smoker_count"] += 1

            age = c.get("Age__c", 0)
            if age <= 30:
                dashboard_data["customers"]["age_distribution"]["18-30"] += 1
            elif age <= 40:
                dashboard_data["customers"]["age_distribution"]["31-40"] += 1
            elif age <= 50:
                dashboard_data["customers"]["age_distribution"]["41-50"] += 1
            elif age <= 60:
                dashboard_data["customers"]["age_distribution"]["51-60"] += 1
            else:
                dashboard_data["customers"]["age_distribution"]["61+"] += 1

            total_income += c.get("Annual_Income__c", 0)

        if customers:
            dashboard_data["customers"]["avg_income"] = round(total_income / len(customers), 2)

    # Quotes analytics
    if "quotes" in entities:
        quotes = entities["quotes"]
        dashboard_data["quotes"] = {
            "total_count": len(quotes),
            "by_product_type": {},
            "by_status": {},
            "by_source": {},
            "avg_coverage": 0,
            "avg_premium": 0,
        }

        total_coverage = 0
        total_premium = 0

        for q in quotes:
            pt = q.get("Product_Type__c", "Unknown")
            dashboard_data["quotes"]["by_product_type"][pt] = (
                dashboard_data["quotes"]["by_product_type"].get(pt, 0) + 1
            )

            status = q.get("Status__c", "Unknown")
            dashboard_data["quotes"]["by_status"][status] = (
                dashboard_data["quotes"]["by_status"].get(status, 0) + 1
            )

            source = q.get("Source__c", "Unknown")
            dashboard_data["quotes"]["by_source"][source] = (
                dashboard_data["quotes"]["by_source"].get(source, 0) + 1
            )

            total_coverage += q.get("Coverage_Amount__c", 0)
            total_premium += q.get("Premium_Monthly__c", 0)

        if quotes:
            dashboard_data["quotes"]["avg_coverage"] = round(total_coverage / len(quotes), 2)
            dashboard_data["quotes"]["avg_premium"] = round(total_premium / len(quotes), 2)

    # Applications analytics
    if "applications" in entities:
        apps = entities["applications"]
        dashboard_data["applications"] = {
            "total_count": len(apps),
            "by_underwriting_status": {},
            "by_health_class": {},
            "approval_rate": 0,
            "avg_risk_score": 0,
            "medical_exam_required_pct": 0,
        }

        approved_count = 0
        total_risk = 0
        medical_required = 0

        for a in apps:
            status = a.get("Underwriting_Status__c", "Unknown")
            dashboard_data["applications"]["by_underwriting_status"][status] = (
                dashboard_data["applications"]["by_underwriting_status"].get(status, 0) + 1
            )

            if status == "Approved":
                approved_count += 1

            hc = a.get("Health_Class__c", "Unknown")
            dashboard_data["applications"]["by_health_class"][hc] = (
                dashboard_data["applications"]["by_health_class"].get(hc, 0) + 1
            )

            total_risk += a.get("Risk_Score__c", 0)

            if a.get("Medical_Exam_Required__c"):
                medical_required += 1

        if apps:
            dashboard_data["applications"]["approval_rate"] = round(
                approved_count / len(apps) * 100, 1
            )
            dashboard_data["applications"]["avg_risk_score"] = round(total_risk / len(apps), 1)
            dashboard_data["applications"]["medical_exam_required_pct"] = round(
                medical_required / len(apps) * 100, 1
            )

    # Policies analytics
    if "policies" in entities:
        policies = entities["policies"]
        dashboard_data["policies"] = {
            "total_count": len(policies),
            "by_status": {},
            "by_product_type": {},
            "by_payment_frequency": {},
            "total_coverage": 0,
            "total_premium_annual": 0,
            "premium_distribution": {
                "0-100": 0,
                "100-250": 0,
                "250-500": 0,
                "500-1000": 0,
                "1000+": 0,
            },
        }

        for p in policies:
            status = p.get("Status__c", "Unknown")
            dashboard_data["policies"]["by_status"][status] = (
                dashboard_data["policies"]["by_status"].get(status, 0) + 1
            )

            pt = p.get("Product_Type__c", "Unknown")
            dashboard_data["policies"]["by_product_type"][pt] = (
                dashboard_data["policies"]["by_product_type"].get(pt, 0) + 1
            )

            pf = p.get("Payment_Frequency__c", "Unknown")
            dashboard_data["policies"]["by_payment_frequency"][pf] = (
                dashboard_data["policies"]["by_payment_frequency"].get(pf, 0) + 1
            )

            dashboard_data["policies"]["total_coverage"] += p.get("Coverage_Amount__c", 0)

            premium = p.get("Premium_Amount__c", 0)
            freq = p.get("Payment_Frequency__c", "Monthly")
            divisors = {"Monthly": 1, "Quarterly": 3, "Semi-Annual": 6, "Annual": 12}
            monthly_premium = premium / divisors.get(freq, 1)

            if monthly_premium < 100:
                dashboard_data["policies"]["premium_distribution"]["0-100"] += 1
            elif monthly_premium < 250:
                dashboard_data["policies"]["premium_distribution"]["100-250"] += 1
            elif monthly_premium < 500:
                dashboard_data["policies"]["premium_distribution"]["250-500"] += 1
            elif monthly_premium < 1000:
                dashboard_data["policies"]["premium_distribution"]["500-1000"] += 1
            else:
                dashboard_data["policies"]["premium_distribution"]["1000+"] += 1

    # Claims analytics
    if "claims" in entities:
        claims = entities["claims"]
        dashboard_data["claims"] = {
            "total_count": len(claims),
            "by_type": {},
            "by_status": {},
            "total_claimed": 0,
            "total_paid": 0,
            "avg_processing_days": 0,
            "approval_rate": 0,
        }

        processing_days_total = 0
        claims_with_dates = 0
        approved_claims = 0

        for c in claims:
            ctype = c.get("Claim_Type__c", "Unknown")
            dashboard_data["claims"]["by_type"][ctype] = (
                dashboard_data["claims"]["by_type"].get(ctype, 0) + 1
            )

            status = c.get("Status__c", "Unknown")
            dashboard_data["claims"]["by_status"][status] = (
                dashboard_data["claims"]["by_status"].get(status, 0) + 1
            )

            if status in ["Approved", "Paid", "Closed"]:
                approved_claims += 1

            dashboard_data["claims"]["total_claimed"] += c.get("Claim_Amount__c", 0)
            dashboard_data["claims"]["total_paid"] += c.get("Payout_Amount__c", 0) or 0

            if c.get("Filed_Date__c") and c.get("Processed_Date__c"):
                filed = datetime.fromisoformat(c["Filed_Date__c"])
                processed = datetime.fromisoformat(c["Processed_Date__c"])
                processing_days_total += (processed - filed).days
                claims_with_dates += 1

        if claims_with_dates:
            dashboard_data["claims"]["avg_processing_days"] = round(
                processing_days_total / claims_with_dates, 1
            )

        if claims:
            dashboard_data["claims"]["approval_rate"] = round(
                approved_claims / len(claims) * 100, 1
            )

    # Conversion funnel
    dashboard_data["funnel"] = {
        "quotes": len(entities.get("quotes", [])),
        "applications": len(entities.get("applications", [])),
        "policies": len(entities.get("policies", [])),
        "claims": len(entities.get("claims", [])),
        "conversion_rates": {
            "quote_to_application": 0,
            "application_to_policy": 0,
            "policy_to_claim": 0,
        },
    }

    if dashboard_data["funnel"]["quotes"] > 0:
        dashboard_data["funnel"]["conversion_rates"]["quote_to_application"] = round(
            dashboard_data["funnel"]["applications"]
            / dashboard_data["funnel"]["quotes"]
            * 100,
            1,
        )

    if dashboard_data["funnel"]["applications"] > 0:
        dashboard_data["funnel"]["conversion_rates"]["application_to_policy"] = round(
            dashboard_data["funnel"]["policies"]
            / dashboard_data["funnel"]["applications"]
            * 100,
            1,
        )

    if dashboard_data["funnel"]["policies"] > 0:
        dashboard_data["funnel"]["conversion_rates"]["policy_to_claim"] = round(
            dashboard_data["funnel"]["claims"]
            / dashboard_data["funnel"]["policies"]
            * 100,
            1,
        )

    # Save dashboard data
    output_file = BASE_DIR / "docs" / "assets" / "data" / "dashboard_data.json"
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with open(output_file, "w") as f:
        json.dump(dashboard_data, f, indent=2)

    print(f"Dashboard data saved to: {output_file}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run Life Insurance Data Lake Pipeline")
    parser.add_argument(
        "--customers", type=int, default=100, help="Number of customers to generate"
    )
    args = parser.parse_args()

    run_full_pipeline(args.customers)
